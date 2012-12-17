import os

from twisted.internet import defer, task, reactor

import paxos.heartbeat

from zpax import tzmq


class ProposalFailed(Exception):
    pass


class InstanceMismatch(ProposalFailed):

    def __init__(self, current):
        super(InstanceMismatch,self).__init__('Instance Number Mismatch')
        self.current_instance = current


class ValueAlreadyProposed(ProposalFailed):
    
    def __init__(self):
        super(ValueAlreadyProposed,self).__init__('Value Already Proposed')


class ProposalAdvocate (object):
    '''
    Instances of this class ensure that the leader receives the
    proposed value.
    '''
    callLater   = reactor.callLater
    retry_delay = 10 # seconds

    def __init__(self, mnode):
        '''
        retry_delay - Floating point delay in seconds between retry attempts
        '''
        self.mnode      = mnode
        self.instance   = None
        self.proposal   = None
        self.request_id = None
        self.retry_cb   = None

        
    def cancel(self):
        '''
        Called at:
           * Application shutdown
           * On instance resolution & prior to switching to the next instance
        '''
        if self.retry_cb and self.retry_cb.active():
            self.retry_cb.cancel()
            
        self.retry_cb   = None
        self.instance   = None
        self.proposal   = None
        self.request_id = None

        
    def proposal_acknowledged(self, request_id):
        '''
        Once the current leader has been informed, we can suspend retransmits.
        Transmitting will resume if the current leader changes.
        '''
        if self.request_id == request_id and self.retry_cb.active():
            self.retry_cb.cancel()

        
    def leadership_changed(self):
        '''
        The proposal must be re-sent on every leadership acquisition to
        ensure that the current leader is aware that a proposal is awaiting
        resolution.
        '''
        self._send_proposal()
    

    def set_proposal(self, instance, request_id, proposed_value):
        if self.request_id is None:
            self.instance   = instance
            self.proposal   = proposed_value
            self.request_id = request_id

            self._send_proposal()
            
        elif request_id != self.request_id:
            raise ValueAlreadyProposed()

            
    def _send_proposal(self):
        if self.proposal is None:
            return
        
        if self.retry_cb and self.retry_cb.active():
            self.retry_cb.cancel()

        if self.instance == self.mnode.instance:
            self.retry_cb = self.callLater(self.retry_delay,
                                           self._send_proposal)

            self.mnode.send_proposal_to_leader(self.instance, self.request_id, self.proposal)
        else:
            self.cancel()



        
class MultiPaxosNode(object):

    def __init__(self, net_node, quorum_size):
        self.net         = net_node
        self.quorum_size = quorum_size
        self.instance    = 0
        self.leader_uid  = None
        self.pax         = None
        self.advocate    = ProposalAdvocate(self)

        self.instance_exceptions  = set() # message types that should be processed
                                          # even if the instance number is not current
                                          
        self.net.dispatch_message = self.dispatch_message

        self.next_instance()
        
        
    def shutdown(self):
        self.advocate.cancel()
        self.net.shutdown()


    def _new_paxos_node(self):
        '''
        Abstract function that returns a new paxos.node.Node instance
        '''


    @property
    def node_uid(self):
        return self.net.node_uid

    
    def next_instance(self, set_instance_to=None):
        self.advocate.cancel()
        
        if set_instance_to is None:
            self.instance += 1
        else:
            self.instance = set_instance_to
            
        self.pax = self._new_paxos_node()


    def broadcast(self, msg_type, **kwargs):
        kwargs.update( dict(instance=self.instance) )
        self.net.broadcast_message(msg_type, kwargs)
        self.dispatch_message(self.node_uid, msg_type, [kwargs,])

        
    def unicast(self, dest_uid, msg_type, **kwargs):
        kwargs.update( dict(instance=self.instance) )
        if dest_uid == self.node_uid:
            self.dispatch_message(self.node_uid, msg_type, [kwargs,])
        else:
            self.net.unicast_message(dest_uid, msg_type, kwargs)


    def dispatch_message(self, from_uid, msg_type, parts):
        if len(parts) != 1:
            print 'Invalid message: parts length'
            return
        
        kwargs = parts[0]

        if kwargs['instance'] != self.instance and not msg_type in self.instance_exceptions:
            return # Old message

        f = getattr(self, 'receive_' + msg_type, None)

        if f:
            f(from_uid, kwargs)


    def behind_in_sequence(self, current_instance):
        pass
        
            
    #------------------------------------------------------------------
    #
    # Proposal Management
    #
    #------------------------------------------------------------------

    def set_proposal(self, request_id, proposal_value, instance=None):
        if instance is None:
            instance = self.instance
            
        if instance == self.instance:
            self.advocate.set_proposal( instance, request_id, proposal_value )
        else:
            raise InstanceMismatch(self.instance)
        

    def send_proposal_to_leader(self, instance, request_id, proposal_value):
        if instance == self.instance and self.leader_uid is not None:
            self.unicast( self.leader_uid, 'set_proposal',
                          request_id     = request_id,
                          proposal_value = proposal_value )

            
    def receive_set_proposal(self, from_uid, kw):
        self.pax.set_proposal( kw['proposal_value'] )
        self.advocate.set_proposal( kw['instance'], kw['request_id'], kw['proposal_value'] )
        self.unicast( from_uid, 'set_proposal_ack', request_id = kw['request_id'] )
        

    def receive_set_proposal_ack(self, from_uid, kw):
        if from_uid == self.leader_uid:
            self.advocate.proposal_acknowledged( kw['request_id'] )

    #------------------------------------------------------------------
    #
    # Messenger interface required by paxos.node.Node
    #
    #------------------------------------------------------------------

    def send_prepare(self, proposer_obj, proposal_id):
        self.broadcast( 'prepare', proposal_id = proposal_id )

        
    def receive_prepare(self, from_uid, kw):
        self.pax.recv_prepare( kw['proposal_id'] )
        

    def send_promise(self, proposer_obj, proposal_id, previous_id, accepted_value):
        self.broadcast( 'promise', proposal_id    = proposal_id,
                                   previous_id    = previous_id,
                                   accepted_value = accepted_value )

        
    def receive_promise(self, from_uid, kw):
        self.pax.recv_promise( from_uid, kw['proposal_id'], kw['previous_id'],
                               kw['accepted_value'] )

        
    def send_prepare_nack(self, propser_obj, proposal_id):
        self.broadcast( 'prepare_nack', proposal_id = proposal_id )

        
    def receive_prepare_nack(self, from_uid, kw):
        self.pax.recv_prepare_nack( kw['proposal_id'] )

        
    def send_accept(self, proposer_obj, proposal_id, proposal_value):
        self.broadcast( 'accept', proposal_id    = proposal_id,
                                  proposal_value = proposal_value )

        
    def receive_accept(self, from_uid, kw):
        self.pax.recv_accept_request( kw['proposal_id'], kw['proposal_value'] )

        
    def send_accept_nack(self, proposer_obj, proposal_id, promised_id):
        self.broadcast( 'accept_nack', proposal_id = proposal_id,
                                       promised_id = promised_id )


    def receive_accept_nack(self, from_uid, kw):
        self.pax.recv_accept_nack( from_uid, kw['proposal_id'], kw['promised_id'] )
        

    def send_accepted(self, proposer_obj, proposal_id, accepted_value):
        self.broadcast( 'accepted', proposal_id    = proposal_id,
                                    accepted_value = accepted_value )

        
    def receive_accepted(self, from_uid, kw):
        self.pax.recv_accepted( from_uid, kw['proposal_id'], kw['accepted_value'] )

        
    def on_leadership_acquired(self, proposer_obj):
        pass

    
    def on_resolution(self, proposer_obj, proposal_id, value):
        self.next_instance()



        
class MultiPaxosHeartbeatNode(MultiPaxosNode):

    hb_period       = 60
    liveness_window = 180

    def __init__(self, *args, **kwargs):

        self.hb_period       = kwargs.pop('hb_period',       self.hb_period)
        self.liveness_window = kwargs.pop('liveness_window', self.liveness_window)
            
        super(MultiPaxosHeartbeatNode, self).__init__(*args, **kwargs)

        self.hb_poll_task      = task.LoopingCall( lambda : self.pax.poll_liveness()  )
        self.leader_pulse_task = task.LoopingCall( lambda : self.pax.pulse()          )

        self.hb_poll_task.start( self.liveness_window )

        self.instance_exceptions.add('heartbeat')


    def shutdown(self):
        if self.hb_poll_task.running:
            self.hb_poll_task.stop()

        if self.leader_pulse_task.running:
            self.leader_pulse_task.stop()

        super(MultiPaxosHeartbeatNode, self).shutdown()

        
    def _new_paxos_node(self):
        return paxos.heartbeat.HeartbeatNode(self, self.node_uid, self.quorum_size,
                                             leader_uid      = self.leader_uid,
                                             hb_period       = self.hb_period,
                                             liveness_window = self.liveness_window)

    
    def on_leadership_acquired(self, pax_node_obj):
        self.leader_pulse_task.start( self.hb_period )
        super(MultiPaxosHeartbeatNode, self).on_leadership_acquired(pax_node_obj)


    #------------------------------------------------------------
    #
    # Messenger methods required by paxos.heartbeat.HeartbeatNode
    #
    #------------------------------------------------------------

    
    def send_heartbeat(self, node_obj, leader_proposal_id):
        '''
        Sends a heartbeat message to all nodes
        '''
        self.broadcast( 'heartbeat', leader_proposal_id = leader_proposal_id )


    def receive_heartbeat(self, from_uid, kw):
        if kw['instance'] > self.instance:
            self.next_instance( set_instance_to = kw['instance'] )
            self.pax.recv_heartbeat( kw['leader_proposal_id'] )
            self.behind_in_sequence( kw['instance'] )
        elif kw['instance'] == self.instance:
            self.pax.recv_heartbeat( kw['leader_proposal_id'] )
        

    def schedule(self, node_obj,  msec_delay, func_obj):
        pass # we use Twisted's task.LoopingCall mechanism instead

        
    def on_leadership_lost(self, node_obj):
        '''
        Called when loss of leadership is detected
        '''
        if self.leader_pulse_task.running:
            self.leader_pulse_task.stop()

            
    def on_leadership_change(self, node_obj, prev_leader_uid, new_leader_uid):
        '''
        Called when a change in leadership is detected
        '''
        self.leader_uid = new_leader_uid
        self.advocate.leadership_changed()
        
