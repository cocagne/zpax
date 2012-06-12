import json
import random

from zpax import tzmq

from paxos import multi, basic
from paxos.proposers import heartbeat

from twisted.internet import defer, task, reactor


class ProposalFailed(Exception):
    pass


class SequenceMismatch(ProposalFailed):

    def __init__(self, current):
        super(SequenceMismatch,self).__init__('Sequence Number Mismatch')
        self.current_seq_num = current

        
class ValueAlreadyProposed(ProposalFailed):
    
    def __init__(self):
        super(ValueAlreadyProposed,self).__init__('Value Already Proposed')


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

    node_factory     = None
    on_resolution_cb = None

    def __getstate__(self):
        d = dict( self.__dict__ )
        del d['node_factory']
        del d['on_resolution_cb']
        return d
                
    def on_proposal_resolution(self, instance_num, value):
        self.on_resolution_cb(instance_num, value)
    


class JSONResponder (object):
    '''
    Mixin class providing a simple mechanism for dispatching message handling
    functions. 
    '''
    def _generateResponder(self, prefix, parts_transform = lambda x : x):
        '''
        Returns a message dispatching function suitable for assignment to a
        ZmqSocket instance. The message parts must be in JSON format and the
        first message part must include a "type" field that names the message
        type (as a string). The returned function will call a member function
        of the same name with the supplied prefix argument.
        '''
        def on_rcv(msg_parts):
            msg_parts = parts_transform(msg_parts)
            try:
                parts = [ json.loads(p) for p in msg_parts ]
            except ValueError:
                print 'Invalid JSON: ', msg_parts
                return

            if not parts or not 'type' in parts[0]:
                print 'Missing message type', parts
                return

            fobj = getattr(self, prefix + parts[0]['type'], None)
        
            if fobj:
                fobj(*parts)
        return on_rcv



class BasicNode (JSONResponder):
    '''
    This class provides the basic functionality required for Multi-Paxos
    over ZeroMQ Publish/Subscribe sockets. This class follows the GoF95
    template design pattern and delegates all application level logic to
    a subclass.

    This class uses two paris of sockets. One is the Publish/Subscribe pair
    for sending Paxos protocol messages. The second is a Req/Rep pair used
    for communication with the current leader. As the leader is the one that
    must propose the value, the proposeValue() method uses the Req socket to
    forward the request to the current leader's Rep socket. This is infinitely
    retried until an acknoledgement is received from the leader. When leadership
    changes and the proposed value is still outstanding, the proposal is sent
    to the new leader and infinitely retried until an acknowledgement is
    received.
    '''

    hb_proposer_klass = BasicHeartbeatProposer

    def __init__(self,
                 local_rep_addr,
                 local_pub_sub_addr,
                 remote_pub_sub_addrs,
                 durable_dir=None,
                 object_id=None):

        self.node_uid         = local_rep_addr
        self.local_rep_addr   = local_rep_addr
        self.local_ps_addr    = local_pub_sub_addr
        self.remote_ps_addrs  = remote_pub_sub_addrs
        self.quorum_size      = None
        self.sequence_number  = None
        self.accept_retry     = None
        self.delayed_prepare  = None

        self.current_proposal = None
        self.proposal_retry   = None

        self.pax_rep          = tzmq.ZmqRepSocket()
        self.pax_req          = None                # Assigned on leadership change
        self.pax_pub          = tzmq.ZmqPubSocket()
        self.pax_sub          = tzmq.ZmqSubSocket()

        self.pax_rep.bind(self.local_rep_addr)
        self.pax_pub.bind(self.local_ps_addr)
        
        self.pax_sub.subscribe       = 'zpax'        
        self.pax_sub.messageReceived = self._generateResponder('_SUB_', lambda x : x[1:])

        self.pax_rep.messageReceived = self._generateResponder('_REP_')

        for x in remote_pub_sub_addrs:
            self.pax_sub.connect(x)

        self.mpax                  = BasicMultiPaxos(durable_dir, object_id)
        self.mpax.node_factory     = self._node_factory
        self.mpax.on_resolution_cb = self._on_proposal_resolution

        self.quorum_size      = self.mpax.quorum_size
        self.sequence_number  = self.mpax.instance_num
        
        self.heartbeat_poller = task.LoopingCall( self._poll_heartbeat         )
        self.heartbeat_pulser = task.LoopingCall( self._pulse_leader_heartbeat )

        if self.mpax.quorum_size is not None:
            self.heartbeat_poller.start( self.hb_proposer_klass.liveness_window )


    def initialize(self, quorum_size):
        assert self.mpax.quorum_size is None, 'MultiPaxos instance already initialized'
        
        self.mpax.initialize( self.node_uid, quorum_size )

        self.quorum_size     = self.mpax.quorum_size
        self.sequence_number = self.mpax.instance_num
        
        self.heartbeat_poller.start( self.hb_proposer_klass.liveness_window )


    
    #--------------------------------------------------------------------------
    # Subclass API
    #
    def onLeadershipAcquired(self):
        '''
        Called when this node acquires Paxos leadership.

        Note, this is not reliable as a unique distiction between
        nodes. Multiple nodes may simultaneously believe themselves to be the
        leader. Also, it is possible for another leader to have been elected
        and several proposals resolved before this method is even called. Be
        wary of using this method for anything beyond starting a timer for
        sending heartbeats.
        '''

    def onLeadershipLost(self):
        '''
        Called when this node looses Paxos leadership.

        See note on onLeadershipAcquired() about the reliability of this method.
        '''

    def onLeadershipChanged(self, prev_leader_uid, new_leader_uid):
        '''
        Called whenver Paxos leadership changes.

        See note on onLeadershipAcquired() about the reliability of this method.
        '''

    def onBehindInSequence(self):
        '''
        Called when this node's sequence number is behind the most recently observed
        value.
        '''

    def onProposalResolution(self, instance_num, value):
        '''
        Called when an instance of the Paxos algorithm agrees on a value.
        '''

    def onHeartbeat(self, data):
        '''
        data - Dictionary of key=value paris in the heartbeat message
        '''

    def onShutdown(self):
        '''
        Called immediately before shutting down
        '''

    def getHeartbeatData(self):
        '''
        Returns a dictionary of key=value parameters to be included
        in the heartbeat message
        '''
        return {}

    
    def currentInstanceNum(self):
        return self.mpax.instance_num

    
    def slewSequenceNumber(self, new_sequence_number):
        assert new_sequence_number > self.sequence_number

        self._cancel_proposal()
        
        self.sequence_number = new_sequence_number
        
        if self.mpax.node.proposer.leader:
            self.paxos_on_leadership_lost()
            
        self.mpax.set_instance_number(self.sequence_number)

        
    def proposeValue(self, value, sequence_number=None):
        if sequence_number is not None and not sequence_number == self.sequence_number:
            raise SequenceMismatch( self.sequence_number )

        if self.mpax.node.proposer.value is not None:
            raise ValueAlreadyProposed()

        if self.mpax.node.acceptor.accepted_value is not None:
            raise ValueAlreadyProposed()

        self._set_proposal(value)


    def publish(self, message_type, *parts):
        if not parts:
            parts = [{}]
            
        parts[0]['type'    ] = message_type
        parts[0]['node_uid'] = self.node_uid
        parts[0]['seq_num' ] = self.sequence_number
        
        msg_stack = [ 'zpax' ]

        msg_stack.extend( json.dumps(p) for p in parts )
        
        self.pax_pub.send( msg_stack )
        self.pax_sub.messageReceived( msg_stack )


    def shutdown(self):
        self.onShutdown()
        self._cancel_proposal()
        if self.accept_retry is not None and self.accept_retry.active():
            self.accept_retry.cancel()
        if self.delayed_prepare is not None and self.delayed_prepare.active():
            self.delayed_prepare.cancel()
        self.pax_rep.close()
        self.pax_rep = None
        if self.pax_req is not None:
            self.pax_req.close()
        self.pax_pub.close()
        self.pax_sub.close()
        if self.heartbeat_poller.running:
            self.heartbeat_poller.stop()
        if self.heartbeat_pulser.running:
            self.heartbeat_pulser.stop()


    def checkSequence(self, header):
        '''
        Checks the sequence number of the header. If our sequence number is behind,
        it calls onBehindInSequence().

        The return value is True if the header's squence number matches our own and
        False otherwise. If False is returned, the message will be considered invalid
        and will not be processed. Overloading this method and artificially returning
        False may be used by subclasses to temporarily disable participation in
        Paxos messaging.
        '''
        seq = header['seq_num']
        
        if seq > self.sequence_number:
            self.onBehindInSequence()
            
        return seq == self.sequence_number
            
    #--------------------------------------------------------------------------
    # Helper Methods
    #
    def _node_factory(self, node_uid, leader_uid, quorum_size, resolution_callback):
        return basic.Node( self.hb_proposer_klass(self, node_uid, quorum_size, leader_uid),
                           basic.Acceptor(),
                           basic.Learner(quorum_size),
                           resolution_callback )


    def _set_proposal(self, value):
        if self.current_proposal is None:
            d = dict( type     = 'propose_value',
                      node_uid = self.node_uid,
                      seq_num  = self.sequence_number,
                      value    = value)
            
            self.current_proposal = json.dumps(d)

            self._try_propose()

            
    def _try_propose(self):
        if self.current_proposal:
            if self.proposal_retry and self.proposal_retry.active():
                self.proposal_retry.cancel()

            retry_delay = self.mpax.node.proposer.hb_period
            
            self.proposal_retry = reactor.callLater(retry_delay, self._try_propose)

            if self.pax_req:
                self.pax_req.send( self.current_proposal )
        else:
            self.proposal_retry = None

            
    def _cancel_proposal(self):
        if self.proposal_retry and self.proposal_retry.active():
            self.proposal_retry.cancel()
        self.current_proposal = None
        self.proposal_retry   = None
        
        
    #--------------------------------------------------------------------------
    # Heartbeats 
    #
    def _poll_heartbeat(self):
        self.mpax.node.proposer.poll_liveness()

        
    def _pulse_leader_heartbeat(self):
        self.mpax.node.proposer.pulse()

        
    #--------------------------------------------------------------------------
    # Paxos Leadership Changes 
    #
    def _paxos_on_leadership_acquired(self):
        self.heartbeat_pulser.start( self.hb_proposer_klass.hb_period )
        self.onLeadershipAcquired()

        
    def _paxos_on_leadership_lost(self):
        if self.accept_retry and self.accept_retry.active():
            self.accept_retry.cancel()
            self.accept_retry = None
            
        if self.heartbeat_pulser.running:
            self.heartbeat_pulser.stop()

        self.onLeadershipLost()


    def _paxos_on_leadership_change(self, prev_leader_uid, new_leader_uid):
        if self.pax_rep is None:
            return # Ignore this if shutdown() has been called
        
        if self.pax_req is not None:
            self.pax_req.close()

        if new_leader_uid is not None:
            self.pax_req = tzmq.ZmqReqSocket()

            self.pax_req.messageReceived = self._generateResponder('_REQ_')
        
            self.pax_req.connect( new_leader_uid )

            self._try_propose()
        
        self.onLeadershipChanged(prev_leader_uid, new_leader_uid)

        
    #--------------------------------------------------------------------------
    # Paxos Messaging 
    #
    def _SUB_paxos_heartbeat(self, header, pax):
        self.mpax.node.proposer.recv_heartbeat( tuple(pax[0]) )
        self.onHeartbeat( header )

    
    def _SUB_paxos_prepare(self, header, pax):
        #print self.node_uid, 'got prepare', header, pax
        if self.checkSequence(header):
            r = self.mpax.recv_prepare(header['seq_num'], tuple(pax[0]))
            if r:
                #print 'SND %s:  ' % self.node_uid[-5], (r[0][0], str(r[0][1][-5]))
                self.publish( 'paxos_promise', {}, r )

            
    def _SUB_paxos_promise(self, header, pax):
        #print 'RCV %s:' % self.node_uid[-5], header['node_uid'][-5], (pax[0][0], str(pax[0][1][-5]))
        if self.checkSequence(header):
            r = self.mpax.recv_promise(header['seq_num'],
                                       header['node_uid'],
                                       tuple(pax[0]),
                                       tuple(pax[1]) if pax[1] else None, pax[2])
            if r and r[1] is not None:
                #print self.node_uid, 'sending accept', r
                self._paxos_send_accept( *r )
            

    def _SUB_paxos_accept(self, header, pax):
        #print 'Got Accept(%s)!' % self.node_uid[-5], (self.sequence_number, header['seq_num']), header['node_uid'][-5], pax[0],
        if self.checkSequence(header):
            r = self.mpax.recv_accept_request(header['seq_num'],
                                              tuple(pax[0]),
                                              pax[1])
            if r:
                self.publish( 'paxos_accepted', {}, r )
            else:
                self.publish( 'paxos_accepted_nack', dict( proposal_id = tuple(pax[0]),
                                                           new_proposal_id = self.mpax.node.acceptor.promised_id) )


    def _SUB_paxos_accepted(self, header, pax):
        #print 'Got accepted', header, pax
        if self.checkSequence(header):
            self.mpax.recv_accepted(header['seq_num'], header['node_uid'],
                                    tuple(pax[0]), pax[1])


    def _SUB_paxos_accepted_nack(self, header):
        if self.checkSequence(header):
            self.mpax.node.proposer.recv_accept_nack( header['node_uid'],
                                                      tuple(header['proposal_id']),
                                                      tuple(header['new_proposal_id']) )
        

    def _paxos_send_prepare(self, proposal_id):
        #print self.node_uid, 'sending prepare: ', proposal_id
        
        # Add a random delay before sending this message and recheck to see if a
        # leader is active. This should reduce battles for supremacy.
        def recheck_send():
            if not self.mpax.node.proposer.leader_is_alive():
                #print '### THERE CAN BE ONLY ONE! ###'
                self.publish( 'paxos_prepare', {}, [proposal_id,] )

        w = self.mpax.node.proposer.liveness_window
        w = w - (w * 0.1)
        self.delayed_prepare = reactor.callLater( random.random() * w,
                                                  recheck_send )

        
    def _paxos_send_accept(self, proposal_id, proposal_value):
        if self.mpax.have_leadership() and (
            self.accept_retry is None or not self.accept_retry.active()
            ):
            #print 'Sending accept', self.node_uid, self.mpax.have_leadership()
            self.publish( 'paxos_accept', {}, [proposal_id, proposal_value] )

            retry_delay = self.mpax.node.proposer.hb_period
            
            self.accept_retry = reactor.callLater(retry_delay,
                                                  self._paxos_send_accept,
                                                  proposal_id,
                                                  proposal_value)
            

    def _paxos_send_heartbeat(self, leader_proposal_id):
        self.publish( 'paxos_heartbeat', self.getHeartbeatData(), [leader_proposal_id,] )


    #--------------------------------------------------------------------------
    # Request Messaging 
    #   
    def _REQ_value_proposed(self, header):
        if self.proposal_retry and self.proposal_retry.active():
            self.proposal_retry.cancel()
            self.proposal_retry = None

            
    #--------------------------------------------------------------------------
    # Reply Messaging 
    #   
    def _REP_propose_value(self, header):
        #print 'Proposal made. Seq = ', self.sequence_number, 'Req: ', header
        if header['seq_num'] == self.sequence_number:
            if self.mpax.node.acceptor.accepted_value is None:
                #print 'Setting proposal'
                self.mpax.set_proposal(self.sequence_number, header['value'])
                
        self.pax_rep.send( json.dumps(dict(type='value_proposed')) )

                
    #--------------------------------------------------------------------------
    # Paxos Proposal Resolution 
    #
    def _on_proposal_resolution(self, instance_num, value):
        self._cancel_proposal()
        
        if self.accept_retry is not None:
            self.accept_retry.cancel()
            self.accept_retry = None
            
        self.value            = value
        self.sequence_number  = instance_num + 1

        self.onProposalResolution(instance_num, value)
    
