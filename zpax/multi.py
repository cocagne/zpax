import os
import random

from twisted.internet import defer, task, reactor

import paxos.functional
from paxos.functional import ProposalID

from zpax import tzmq


class ProposalFailed(Exception):
    pass


class InstanceMismatch(ProposalFailed):

    def __init__(self, current):
        super(InstanceMismatch,self).__init__('Instance Number Mismatch')
        self.current_instance = current


class ProposalAdvocate (object):
    '''
    Instances of this class ensure that the leader receives the
    proposed value.
    '''
    callLater   = reactor.callLater
    retry_delay = 10 # seconds

    instance   = None
    proposal   = None
    request_id = None
    retry_cb   = None

    
    def __init__(self, mnode):
        '''
        retry_delay - Floating point delay in seconds between retry attempts
        '''
        self.mnode = mnode
        
        
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

        
    def __getstate__(self):
        d = dict(self.__dict__)
        d.pop('retry_cb', None)
        d.pop('mnode', None)
        return d


    def recover(self, mnode):
        '''
        Called when recovering from durable state
        '''
        self.mnode = mnode
        if self.request_id is not None:
            self._send_proposal()
            
        
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
            
            
    def _send_proposal(self):
        if self.proposal is None:
            return
        
        if self.retry_cb and self.retry_cb.active():
            self.retry_cb.cancel()

        if self.instance == self.mnode.instance:
            self.retry_cb = self.callLater(self.retry_delay,
                                           self._send_proposal)

            if self.mnode.pax.leader:
                self.mnode.pax.resend_accept()
            else:
                self.mnode.send_proposal_to_leader(self.instance, self.request_id, self.proposal)
        else:
            self.cancel()


        
class MultiPaxosNode(object):

    def __init__(self, net_channel, quorum_size):
        self.net         = net_channel
        self.node_uid    = net_channel.node_uid
        self.quorum_size = quorum_size
        self.instance    = 0
        self.leader_uid  = None
        self.pax         = None
        self.pax_ignore  = False # True if all Paxos messages should be ignored
        self.advocate    = ProposalAdvocate(self)

        self.instance_exceptions  = set() # message types that should be processed
                                          # even if the instance number is not current

        self.net.add_message_handler(self)

        self.next_instance()
        
        
    def shutdown(self):
        self.advocate.cancel()
        self.net.shutdown()


    def __getstate__(self):
        d = dict(self.__dict__)
        d.pop('net', None)
        return d

    
    def change_quorum_size(self, quorum_size):
        self.quorum_size = quorum_size
        self.pax.change_quorum_size( quorum_size )


    def recover(self, net_channel):
        '''
        Called when recovering from durable state. Recovery of the paxos node instance
        is left to the subclass.
        '''
        assert self.node_uid == net_node.node_uid, "Node UID mismatch"
        self.net = net_channel
        self.net.add_message_handler(self)
        self.advocate.recover(self)


    def _new_paxos_node(self):
        '''
        Abstract function that returns a new paxos.node.Node instance
        '''


    def next_instance(self, set_instance_to=None):
        self.advocate.cancel()
        
        if set_instance_to is None:
            self.instance += 1
        else:
            self.instance = set_instance_to
            
        self.pax = self._new_paxos_node()


    def broadcast(self, msg_type, **kwargs):
        kwargs.update( dict(instance=self.instance) )
        self.net.broadcast(msg_type, kwargs)

        
    def unicast(self, dest_uid, msg_type, **kwargs):
        kwargs.update( dict(instance=self.instance) )
        self.net.unicast(dest_uid, msg_type, kwargs)
        
    
    def behind_in_sequence(self, current_instance):
        pass
        

    def _mpax_filter(func):
        def wrapper(self, from_uid, msg):
            if msg['instance'] == self.instance and not self.pax_ignore:
                func(self, from_uid, msg)
        return wrapper

    #------------------------------------------------------------------
    #
    # Proposal Management
    #
    #------------------------------------------------------------------

    def set_proposal(self, request_id, proposal_value, instance=None):
        if instance is None:
            instance = self.instance
            
        if instance == self.instance:
            self.pax.set_proposal( (request_id, proposal_value) )
            self.advocate.set_proposal( instance, request_id, proposal_value )
        else:
            raise InstanceMismatch(self.instance)
        

    def send_proposal_to_leader(self, instance, request_id, proposal_value):
        if instance == self.instance and self.leader_uid is not None:
            self.unicast( self.leader_uid, 'set_proposal',
                          request_id     = request_id,
                          proposal_value = proposal_value )


    @_mpax_filter
    def receive_set_proposal(self, from_uid, msg):
        self.set_proposal(msg['request_id'], msg['proposal_value'])
        self.unicast( from_uid, 'set_proposal_ack', request_id = msg['request_id'] )

        
    @_mpax_filter
    def receive_set_proposal_ack(self, from_uid, msg):
        if from_uid == self.leader_uid:
            self.advocate.proposal_acknowledged( msg['request_id'] )

    #------------------------------------------------------------------
    #
    # Messenger interface required by paxos.node.Node
    #
    #------------------------------------------------------------------

    def send_prepare(self, proposal_id):
        self.broadcast( 'prepare', proposal_id = proposal_id )

        
    @_mpax_filter
    def receive_prepare(self, from_uid, msg):
        self.pax.recv_prepare( from_uid, ProposalID(*msg['proposal_id']) )

        # TODO: Actual durability implementation
        if self.pax.persistance_required:
            self.pax.persisted()
        

    def send_promise(self, to_uid, proposal_id, previous_id, accepted_value):
        self.unicast( to_uid, 'promise', proposal_id    = proposal_id,
                                         previous_id    = previous_id,
                                         accepted_value = accepted_value )

        
    @_mpax_filter
    def receive_promise(self, from_uid, msg):
        self.pax.recv_promise( from_uid, ProposalID(*msg['proposal_id']),
                               ProposalID(*msg['previous_id']) if msg['previous_id'] else None,
                               msg['accepted_value'] )

        
    def send_prepare_nack(self, propser_obj, to_uid, proposal_id):
        self.unicast( to_uid, 'prepare_nack', proposal_id = proposal_id )

        
    @_mpax_filter
    def receive_prepare_nack(self, from_uid, msg):
        self.pax.recv_prepare_nack( from_uid, ProposalID(*msg['proposal_id']) )

        
    def send_accept(self, proposal_id, proposal_value):
        self.broadcast( 'accept', proposal_id    = proposal_id,
                                  proposal_value = proposal_value )


    @_mpax_filter
    def receive_accept(self, from_uid, msg):
        self.pax.recv_accept_request( from_uid, ProposalID(*msg['proposal_id']), msg['proposal_value'] )

        # TODO: Actual durability implementation
        if self.pax.persistance_required:
            self.pax.persisted()

        
    def send_accept_nack(self, to_uid, proposal_id, promised_id):
        self.unicast( to_uid, 'accept_nack', proposal_id = proposal_id,
                                             promised_id = promised_id )


    @_mpax_filter
    def receive_accept_nack(self, from_uid, msg):
        self.pax.recv_accept_nack( from_uid, ProposalID(*msg['proposal_id']), ProposalID(*msg['promised_id']) )
        

    def send_accepted(self, proposal_id, accepted_value):
        self.broadcast( 'accepted', proposal_id    = proposal_id,
                                    accepted_value = accepted_value )


    @_mpax_filter
    def receive_accepted(self, from_uid, msg):
        self.pax.recv_accepted( from_uid, ProposalID(*msg['proposal_id']), msg['accepted_value'] )

        
    def on_leadership_acquired(self):
        #print 'LEADERSHIP ACQ: ', self.node_uid
        pass

    
    def on_resolution(self, proposal_id, value):
        self.next_instance()



        
class MultiPaxosHeartbeatNode(MultiPaxosNode):

    hb_period         = 60  # Seconds
    liveness_window   = 180 # Seconds

    hb_poll_task      = None
    leader_pulse_task = None
    is_leader         = False

    def __init__(self, *args, **kwargs):

        self.hb_period       = kwargs.pop('hb_period',       self.hb_period)
        self.liveness_window = kwargs.pop('liveness_window', self.liveness_window)
            
        super(MultiPaxosHeartbeatNode, self).__init__(*args, **kwargs)

        self._start_tasks()

        self.instance_exceptions.add('heartbeat')


    def shutdown(self):
        if self.hb_poll_task.running:
            self.hb_poll_task.stop()

        if self.leader_pulse_task.running:
            self.leader_pulse_task.stop()

        super(MultiPaxosHeartbeatNode, self).shutdown()


    def __getstate__(self):
        d = super(MultiPaxosHeartbeatNode, self).__getstate__()
        d.pop('hb_poll_task',      None)
        d.pop('leader_pulse_task', None)
        return d


    def recover(self, *args):
        self.pax.recover(self)
        super(MultiPaxosHeartbeatNode, self).recover(*args)
        self._start_tasks()
        


    def _start_tasks(self):
        self.hb_poll_task      = task.LoopingCall( lambda : self.pax.poll_liveness()  )
        self.leader_pulse_task = task.LoopingCall( lambda : self.pax.pulse()          )

        self.hb_poll_task.start( self.liveness_window )

        
    def _new_paxos_node(self):
        return paxos.functional.HeartbeatNode(self, self.node_uid, self.quorum_size,
                                              leader_uid      = self.leader_uid,
                                              hb_period       = self.hb_period,
                                              liveness_window = self.liveness_window)

    
    def on_leadership_acquired(self):
        self.is_leader = True
        self.leader_pulse_task.start( self.hb_period )
        super(MultiPaxosHeartbeatNode, self).on_leadership_acquired()


    #------------------------------------------------------------
    #
    # Messenger methods required by paxos.functional.HeartbeatNode
    #
    #------------------------------------------------------------

    
    def send_heartbeat(self, leader_proposal_id):
        '''
        Sends a heartbeat message to all nodes
        '''
        self.broadcast( 'heartbeat', leader_proposal_id = leader_proposal_id )


    def receive_heartbeat(self, from_uid, msg):
        if msg['instance'] > self.instance:
            self.next_instance( set_instance_to = msg['instance'] )
            self.pax.recv_heartbeat( from_uid, ProposalID(*msg['leader_proposal_id']) )
            self.behind_in_sequence( msg['instance'] )
        elif msg['instance'] == self.instance:
            self.pax.recv_heartbeat( from_uid, ProposalID(*msg['leader_proposal_id']) )
        

    def schedule(self, msec_delay, func_obj):
        pass # we use Twisted's task.LoopingCall mechanism instead

        
    def on_leadership_lost(self):
        '''
        Called when loss of leadership is detected
        '''
        self.is_leader = False
        if self.leader_pulse_task.running:
            self.leader_pulse_task.stop()

            
    def on_leadership_change(self, prev_leader_uid, new_leader_uid):
        '''
        Called when a change in leadership is detected
        '''
        self.leader_uid = new_leader_uid
        self.advocate.leadership_changed()
        
