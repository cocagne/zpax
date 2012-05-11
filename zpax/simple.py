import json

from zpax import tzmq
from paxos import multi
from paxos.leaders import heartbeat

from twisted.internet import defer, task, reactor


class SimpleHeartbeatProposer (heartbeat.Proposer):
    hb_period       = 500
    liveness_window = 1500

    def __init__(self, simple_node):
        self.node = simple_node

        super(SimpleHeartbeatProposer, self).__init__(self.node.local_ps_addr,
                                                      self.node.paxos_threshold,
                                                      leader_uid = self.node.current_leader)

    def send_prepare(self, proposal_id):
        self.node.paxos_send_prepare(proposal_id)

    def send_accept(self, proposal_id, proposal_value):
        self.node.paxos_send_accept(proposal_id, proposal_value)

    def send_heartbeat(self, leader_proposal_id):
        self.node.paxos_send_heartbeat(leader_proposal_id)

    def schedule(self, msec_delay, func_obj):
        pass

    def on_leadership_acquired(self):
        self.node.paxos_on_leadership_acquired()

    def on_leadership_lost(self):
        self.node.paxos_on_leadership_lost()

    def on_leadership_change(self, prev_leader_uid, new_leader_uid):
        self.node.paxos_on_leadership_change(prev_leader_uid, new_leader_uid)
        


class SimpleMultiPaxos(multi.MultiPaxos):

    def __init__(self, simple_node):
        super(SimpleMultiPaxos, self).__init__( simple_node.node_uid,
                                                simple_node.paxos_threshold,
                                                simple_node.sequence_number )
        self.simple_node  = simple_node
        self.node_factory = simple_node._node_factory

        
    def on_proposal_resolution(self, instance_num, value):
        self.simple_node.onProposalResolution(instance_num, value)
    



class SimpleNode (object):

    def __init__(self, node_uid,
                 local_pub_sub_addr,   local_rep_addr,
                 remote_pub_sub_addrs,
                 paxos_threshold,
                 initial_value='', sequence_number=0):

        self.node_uid         = node_uid
        self.local_ps_addr    = local_pub_sub_addr
        self.local_rep_addr   = local_rep_addr
        self.remote_ps_addrs  = remote_pub_sub_addrs
        self.paxos_threshold  = paxos_threshold
        self.value            = initial_value
        self.sequence_number  = sequence_number
        self.current_leader   = None

        self.mpax             = SimpleMultiPaxos(self)

        self.heartbeat_poller = task.LoopingCall( self._poll_heartbeat         )
        self.heartbeat_pulser = task.LoopingCall( self._pulse_leader_heartbeat )
        
        self.pubsub           = tzmq.ZmqPubSocket()
        self.rep              = tzmq.ZmqRepSocket()
        
        self.pubsub.messageReceived = self.onPubSubReceived
        self.rep.messageReceived    = self.onRepReceived
        
        self.pubsub.bind(self.local_ps_addr)
        self.rep.bind(self.local_rep_addr)

        for x in remote_pub_sub_addrs:
            self.pubsub.connect(x)

        self.heartbeat_poller.start( SimpleHeartbeatProposer.liveness_window / 1000.0 )


    def _node_factory(self,  quorum_size, resolution_callback):
        return basic.Node( SimpleHeartbeatProposer(self),
                           basic.Acceptor(),
                           basic.Learner(quorum_size),
                           resolution_callback )


    def _poll_heartbeat(self):
        self.mpax.node.proposer.poll_liveness()

        
    def _pulse_leader_heartbeat(self):
        self.mpax.node.proposer.pulse()

        
    def paxos_on_leadership_acquired(self):
        self.heartbeat_pulser.start( SimpleHeartbeatProposer.hb_period / 1000.0 )

        
    def paxos_on_leadership_lost(self):
        if self.heartbeat_pulser.running:
            self.heartbeat_pulser.stop()


    def paxos_send_prepare(self, proposal_id):
        self.publish( dict( type='paxos_prepare', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                      [proposal_id,] )

        
    def paxos_send_accept(self, proposal_id, proposal_value):
        self.publish( dict( type='paxos_accept', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                      [proposal_id, proposal_value] )

        
    def paxos_send_heartbeat(self, leader_proposal_id):
        self.publish( dict( type='paxos_heartbeat', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                      [leader_proposal_id,] )


    def publish(self, *parts):
        jparts = [ json.dumps(p) for p in parts ]
        self.pubsub.send( jparts )


    def onPubSubReceived(self, msg_parts):
        '''
        msg_parts - [0] is SimpleNode's JSON-encoded structure
                    [1] If present, it's a JSON-encoded Paxos message
        '''
        try:
            parts = [ json.loads(p) for p in msg_parts ]
        except ValueError:
            print 'Invalid JSON: ', msg_parts

        if not 'type' in parts[0]:
            print 'Missing message type'
            return

        fobj = getattr(self, '_on_pub_' + parts[0]['type'], None)
        
        if fobj:
            fobj(*parts)



    def _check_sequence(self, header):
        if header['sequence_number'] > self.sequence_number:
            self.publish( dict( type='get_value' ) )

        if send_nack and header['sequence_number'] < self.sequence_number:
            self.publish_value()

    
    def _on_pub_paxos_prepare(self, header, pax):
        self._check_sequence(header):
        r = self.mpax.recv_prepare(self.sequence_number, *pax)
        if r:
            self.publish( dict( type='paxos_promise', sequence_number=self.sequence_number, node_uid=self.node_uid ),
                          r )

            
    def _on_pub_paxos_promise(self, header, pax):
        self._check_sequence(header):
        r = self.mpax.recv_promise(self.sequence_number, header['node_uid'], *pax)
        if r:
            self.paxos_send_accept( *r )
            

    def _on_pub_paxos_accept(self, header, pax):
        self._check_sequence(header):
        self.mpax.recv_accept_request(self.sequence_number, *pax)

        
    def _on_pub_get_value(self, header):
        self.publish_value()
        

    def _on_pub_value(self, header):
        if header['sequence_number'] > self.sequence_number:
            self.value           = header['value']
            self.sequence_number = header['sequence_number']
            if self.mpax.node.proposer.leader:
                self.paxos_on_leadership_lost()
            self.mpax.set_instance_number(self.sequence_number)

            
    def publish_value(self):
        self.publish( dict(type='value', sequence_number=self.sequence_number, value=self.value) )


    def onProposalResolution(self, instance_num, value):
        pass


    def onRepReceived(self, msg_parts):
        pass



        
        
