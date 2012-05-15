import json

from zpax import tzmq

from paxos import multi, basic
from paxos.leaders import heartbeat

from twisted.internet import defer, task, reactor


class BasicHeartbeatProposer (heartbeat.Proposer):
    hb_period       = 0.5
    liveness_window = 1.5

    def __init__(self, basic_node, node_uid, quorum_size, leader_uid):
        self.node = basic_node

        super(BasicHeartbeatProposer, self).__init__(node_uid,
                                                     quorum_size,
                                                     leader_uid = leader_uid)

    def send_prepare(self, proposal_id):
        self.node._paxos_send_prepare(proposal_id)

    def send_accept(self, proposal_id, proposal_value):
        self.node._paxos_send_accept(proposal_id, proposal_value)

    def send_heartbeat(self, leader_proposal_id):
        self.node._paxos_send_heartbeat(leader_proposal_id)

    def schedule(self, msec_delay, func_obj):
        pass

    def on_leadership_acquired(self):
        self.node._paxos_on_leadership_acquired()

    def on_leadership_lost(self):
        self.node._paxos_on_leadership_lost()

    def on_leadership_change(self, prev_leader_uid, new_leader_uid):
        self.node._paxos_on_leadership_change(prev_leader_uid, new_leader_uid)
        


class BasicMultiPaxos(multi.MultiPaxos):

    def __init__(self, node_uid, quorum_size, sequence_number, node_factory,
                 on_resolution_callback):

        self.node_factory     = node_factory
        self.on_resolution_cb = on_resolution_callback
        
        super(BasicMultiPaxos, self).__init__( node_uid,
                                                quorum_size,
                                                sequence_number )
                
    def on_proposal_resolution(self, instance_num, value):
        self.on_resolution_cb(instance_num, value)
    



class BasicNode (object):

    def __init__(self, node_uid,
                 local_pub_sub_addr,
                 remote_pub_sub_addrs,
                 quorum_size,
                 sequence_number=0):

        self.node_uid         = node_uid
        self.local_ps_addr    = local_pub_sub_addr
        self.local_rtr_addr   = local_rtr_addr
        self.remote_ps_addrs  = remote_pub_sub_addrs
        self.quorum_size      = quorum_size
        self.sequence_number  = sequence_number
        self.accept_retry     = None

        self.mpax             = BasicMultiPaxos(node_uid,
                                                 quorum_size,
                                                 sequence_number,
                                                 self._node_factory,
                                                 self.on_proposal_resolution)

        self.heartbeat_poller = task.LoopingCall( self._poll_heartbeat         )
        self.heartbeat_pulser = task.LoopingCall( self._pulse_leader_heartbeat )
        
        self.pub              = tzmq.ZmqPubSocket()
        self.sub              = tzmq.ZmqSubSocket()

        self.sub.subscribe = 'zpax'
        
        self.sub.messageReceived    = self._on_sub_received
        self.router.messageReceived = self._on_router_received
        
        self.pub.bind(self.local_ps_addr)

        for x in remote_pub_sub_addrs:
            self.sub.connect(x)

        self.heartbeat_poller.start( BasicHeartbeatProposer.liveness_window )

    #
    # Subclass API
    #
    def onLeadershipAcquired(self):
        pass

    def onLeadershipLost(self):
        pass

    def onLeadershipChanged(self, prev_leader_uid, new_leader_uid):
        pass

    def onBehindInSequence(self):
        pass

    def onOtherNodeBehindInSequence(self, node_uid):
        pass
    #------------------------
    
    def _node_factory(self, node_uid, leader_uid, quorum_size, resolution_callback):
        return basic.Node( BasicHeartbeatProposer(self, node_uid, quorum_size, leader_uid),
                           basic.Acceptor(),
                           basic.Learner(quorum_size),
                           resolution_callback )
    #
    # --- Heartbeats ---
    #
    def _poll_heartbeat(self):
        self.mpax.node.proposer.poll_liveness()

        
    def _pulse_leader_heartbeat(self):
        self.mpax.node.proposer.pulse()

    #
    # --- Paxos Leadership Changes ---
    #
    def _paxos_on_leadership_acquired(self):
        print self.node_uid, 'I have the leader!', self.mpax.node.proposer.value
        self.heartbeat_pulser.start( BasicHeartbeatProposer.hb_period )
        self.onLeadershipAcquired()

        
    def _paxos_on_leadership_lost(self):
        print self.node_uid, 'I LOST the leader!'
        if self.accept_retry is not None:
            self.accept_retry.cancel()
            self.accept_retry = None
            
        if self.heartbeat_pulser.running:
            self.heartbeat_pulser.stop()

        self.onLeadershipLost()


    def _paxos_on_leadership_change(self, prev_leader_uid, new_leader_uid):
        print '*** Change of guard: ', prev_leader_uid, new_leader_uid
        self.onLeadershipChange(prev_leader_uid, new_leader_uid)

    #
    # --- Paxos Messaging ---
    #
    def _on_sub_received(self, msg_parts):
        '''
        msg_parts - [0] 'zpax'
                    [1] is BasicNode's JSON-encoded structure
                    [2] If present, it's a JSON-encoded Paxos message
        '''
        try:
            parts = [ json.loads(p) for p in msg_parts[1:] ]
        except ValueError:
            print 'Invalid JSON: ', msg_parts
            return

        if not 'type' in parts[0]:
            print 'Missing message type'
            return

        fobj = getattr(self, '_on_sub_' + parts[0]['type'], None)
        
        if fobj:
            fobj(*parts)


    def _check_sequence(self, header):
        if header['sequence_number'] > self.sequence_number:
            self.onBehindInSequence()
            
        if header['sequence_number'] < self.sequence_number:
            self.onOtherNodeBehindInSequence(header['node_uid'])


    def _on_sub_paxos_heartbeat(self, header, pax):
        self.mpax.node.proposer.recv_heartbeat( tuple(pax[0]) )

    
    def _on_sub_paxos_prepare(self, header, pax):
        #print self.node_uid, 'got prepare', header, pax
        self._check_sequence(header)
        r = self.mpax.recv_prepare(header['sequence_number'], tuple(pax[0]))
        if r:
            #print self.node_uid, 'sending promise'
            self.publish( dict( type='paxos_promise', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                          r )

            
    def _on_sub_paxos_promise(self, header, pax):
        #print self.node_uid, 'got promise', header, pax
        self._check_sequence(header)
        r = self.mpax.recv_promise(header['sequence_number'], header['node_uid'],
                                   tuple(pax[0]), tuple(pax[1]) if pax[1] else None, pax[2])
        if r and r[1] is not None:
            #print self.node_uid, 'sending accept', r
            self._paxos_send_accept( *r )
            

    def _on_sub_paxos_accept(self, header, pax):
        #print 'Got Accept!', pax
        self._check_sequence(header)
        r = self.mpax.recv_accept_request(header['sequence_number'], tuple(pax[0]), pax[1])
        if r:
            self.publish( dict( type='paxos_accepted', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                          r )


    def _on_sub_paxos_accepted(self, header, pax):
        #print 'Got accepted', header, pax
        self._check_sequence(header)
        self.mpax.recv_accepted(header['sequence_number'], header['node_uid'],
                                tuple(pax[0]), pax[1])
        

    def _paxos_send_prepare(self, proposal_id):
        #print self.node_uid, 'sending prepare: ', proposal_id
        self.publish( dict( type='paxos_prepare', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                      [proposal_id,] )

        
    def _paxos_send_accept(self, proposal_id, proposal_value):
        if self.mpax.have_leadership() and (self.accept_retry is None or not self.accept_retry.active()):
            #print 'Sending accept'
            self.publish( dict( type='paxos_accept', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                          [proposal_id, proposal_value] )

            self.accept_retry = reactor.callLater(self.mpax.node.proposer.hb_period, self._paxos_send_accept, proposal_id, proposal_value)
            

    def _paxos_send_heartbeat(self, leader_proposal_id):
        self.publish( dict( type='paxos_heartbeat', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                      [leader_proposal_id,] )


    def publish(self, *parts):
        jparts = [ json.dumps(p) for p in parts ]
        self.pub.send( 'zpax', *jparts )
        self._on_sub_received(['zpax'] + jparts)

    #
    # --- BasicNode's Pub-Sub Messaging ---
    #
    def publish_value(self):
        self.publish( dict(type='value', sequence_number=self.sequence_number, value=self.value) )

        
    def _on_sub_get_value(self, header):
        self.publish_value()
        

    def _on_sub_value(self, header):
        if header['sequence_number'] > self.sequence_number:
            self.value           = header['value']
            self.sequence_number = header['sequence_number']
            if self.mpax.node.proposer.leader:
                self._paxos_on_leadership_lost()
            self.mpax.set_instance_number(self.sequence_number)

            
    def _on_sub_value_proposal(self, header):
        #print 'Proposal made. Seq = ', self.sequence_number, 'Req: ', header
        if header['sequence_number'] == self.sequence_number:
            #print 'Setting proposal'
            self.mpax.set_proposal(self.sequence_number, header['value'])

    #
    # --- Paxos Proposal Resolution ---
    #
    def on_proposal_resolution(self, instance_num, value):
        print '*** Resolution! ', instance_num, repr(value)
        if self.accept_retry is not None:
            self.accept_retry.cancel()
            self.accept_retry = None
            
        self.value            = value
        self.sequence_number  = instance_num + 1

        for addr in self.waiting_clients:
            self.reply_value(addr)

        self.waiting_clients.clear()

    
    #
    # --- Router Socket Handling ---
    #
    def _on_router_received(self, msg_parts):
        #print 'Router Rec: ', msg_parts
        try:
            addr  = msg_parts[0]
            parts = [ json.loads(p) for p in msg_parts[2:] ]
        except ValueError:
            print 'Invalid JSON: ', msg_parts
            return

        if not parts or not 'type' in parts[0]:
            print 'Missing message type', parts
            return

        fobj = getattr(self, '_on_router_' + parts[0]['type'], None)
        
        if fobj:
            fobj(addr, *parts)

            
    def reply(self, addr, *parts):
        jparts = [ json.dumps(p) for p in parts ]
        self.router.send( addr, '', *jparts )

        
    def reply_value(self, addr):
        self.reply(addr, dict(sequence_number=self.sequence_number-1, value=self.value) )

        
    def _on_router_propose_value(self, addr, header):
        if header['sequence_number'] == self.sequence_number:
            print 'Proposing...', header['value']
            self.publish( dict(type='value_proposal', sequence_number=self.sequence_number, value=header['value']) )
            self.mpax.set_proposal(self.sequence_number, header['value'])
            self.reply(addr, dict(proposed=True))
        else:
            print 'Proposal rejected. Seq not match! Seq is', self.sequence_number
            self.reply(addr, dict(proposed=False, message='Invalid sequence number'))

            
    def _on_router_query_value(self, addr, header):
        self.reply_value(addr)

        
    def _on_router_get_next_value(self, addr, header):
        if header['sequence_number'] < self.sequence_number:
            self.reply_value(addr)
        else:
            self.waiting_clients.add( addr )
        

        
        



        
        
