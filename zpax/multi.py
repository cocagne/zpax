'''
This module provides a Multi-Paxos node implementation. The implementation is
broken into two separate components. MultiPaxosNode implements the basic logic
required for Multi-Paxos operation. MultiPaxosHeartbeatNode enhances the basic
implementation with a heartbeating mechanism used to detect and recover from
failed leaders. The heartbeating mechanism is only one of potentially many
mechanisms that could be used to serve this purpose so it is isolated from
the rest of the Multi-Paxoxs logic to facilitate reuse of the generic component.
'''

import os
import random

from   twisted.internet import defer, task, reactor

import paxos.functional
from   paxos.functional import ProposalID


class ProposalFailed(Exception):
    pass


class InstanceMismatch(ProposalFailed):

    def __init__(self, current):
        super(InstanceMismatch,self).__init__('Instance Number Mismatch')
        self.current_instance = current


class ProposalAdvocate (object):
    '''
    Instances of this class ensure that the leader receives the proposed value.
    '''
    callLater   = reactor.callLater
    retry_delay = 10 # seconds

    instance    = None
    proposal    = None
    request_id  = None
    retry_cb    = None

    
    def __init__(self, mnode, retry_delay = None):
        '''
        mnode       - MultiPaxosNode
        retry_delay - Floating point delay in seconds between retry attempts. Defaults
                      to 10
        '''
        self.mnode = mnode
        if retry_delay:
            self.retry_delay = None
        
        
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
        '''
        Sets the proposal value if one hasn't already been set and begins 
        attempting to notify the current leader of the proposed value.
        '''
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
    '''
    This is an abstract class that provides an implementation for the majority
    of the required Multi-Paxos components. The only omitted component is the
    Detection of and recovery from leadership failures. There are multiple
    strategies for handling this so it is left for subclasses to define.
    '''

    def __init__(self, net_channel, quorum_size, durable_data_id, durable_data_store):
        '''
        net_channel        - net.Channel instance
        quorum_size        - Paxos quorum size
        durable_data_id    - Unique ID for use with the durable_data_store
        durable_data_store - Implementer of durable.IDurableDataStore
        '''
        self.net         = net_channel
        self.node_uid    = net_channel.node_uid
        self.quorum_size = quorum_size
        self.dd_store    = durable_data_store
        self.dd_id       = durable_data_id
        self.instance    = 0
        self.leader_uid  = None
        self.pax         = None
        self.pax_ignore  = False # True if all Paxos messages should be ignored
        self.persisting  = False
        self.advocate    = ProposalAdvocate(self)

        self.instance_exceptions  = set() # message types that should be processed
                                          # even if the instance number is not current

        self.net.add_message_handler(self)
        

    @defer.inlineCallbacks
    def initialize(self):
        '''
        Initilizes the node from the durable state saved for a pre-existing,
        multi-paxos session or creates a new session if no durable state is found.
        '''
        state = yield self.dd_store.get_state(self.dd_id)

        if state is None:
            self.next_instance()
        else:
            self.instance = state['instance']
            
            yield self._recover_from_persistent_state( state )
            
            self.advocate.set_proposal( self.instance, state['request_id'],
                                        state['proposal'] )


    def _recover_from_persistent_state(self, state):
        '''
        This method may be used by subclasses to recover the multi-paxos
        session from durable state. The 'state' argument is a dictionary
        containing the Acceptor's promised_id, accepted_id, and accepted_value
        attributes. The subclass may add additional state to this dictionary by
        overriding the _get_additional_persistent_state method.
        '''
    

    def _get_additional_persistent_state(self):
        '''
        Called when multi-paxos state is being persisted to disk prior to sending
        a promise/accepted message. The return value is a dictionary of key-value
        pairs to persist
        '''
        return {}
        
        
    def shutdown(self):
        '''
        Shuts down the node.
        '''
        self.advocate.cancel()
        self.net.shutdown()


    def change_quorum_size(self, quorum_size):
        '''
        If nodes are added/removed from the Paxos group, this method may be used
        to update the quorum size. Note: negotiation of the new Paxos membership
        and resulting quorum size should occur within the context of the
        previous multi-paxos instance. Doing so outside the multi-paxos chain of
        resolutions will almost certainly result in an inconsistent view of the
        membership list which, in turn, breaks the safety guarantees provided
        by Paxos.
        '''
        self.quorum_size = quorum_size
        self.pax.change_quorum_size( quorum_size )


    def _new_paxos_node(self):
        '''
        Abstract function that returns a new paxos.node.Node instance
        '''

        
    @defer.inlineCallbacks
    def persist(self):
        '''
        Uses the durable state store to flush recovery state to disk.
        Subclasses may add to the flushed state by overriding the
        _get_additional_persistent_state() method.
        '''
        if not self.persisting:
            self.persisting = True

            state = dict( instance       = self.instance,
                          proposal       = self.advocate.proposal,
                          request_id     = self.advocate.request_id,
                          promised_id    = self.pax.promised_id,
                          accepted_id    = self.pax.accepted_id,
                          accepted_value = self.pax.accepted_value )

            state.update( self._get_additional_persistent_state() )
            
            yield self.dd_store.set_state( self.dd_id, state )
            
            self.persisting = False
            
            self.pax.persisted()
            



    def next_instance(self, set_instance_to=None):
        '''
        Advances to the next multi-paxos instance or to the specified
        instance number if one is provided. 
        '''
        self.advocate.cancel()
        
        if set_instance_to is None:
            self.instance += 1
        else:
            self.instance = set_instance_to
            
        self.pax = self._new_paxos_node()


    def broadcast(self, msg_type, **kwargs):
        '''
        Broadcasts a message to all peers
        '''
        kwargs.update( dict(instance=self.instance) )
        self.net.broadcast(msg_type, kwargs)

        
    def unicast(self, dest_uid, msg_type, **kwargs):
        '''
        Sends a message to the specified peer only
        '''
        kwargs.update( dict(instance=self.instance) )
        self.net.unicast(dest_uid, msg_type, kwargs)
        
    
    def behind_in_sequence(self, current_instance):
        '''
        Nodes can only send messages for an instance once the previous instance
        has been completed. Consequently, observance of a "future" message
        indicates that this node has fallen behind it's peers. This method is
        called when that case is detected. Application-specific logic must then
        preform some form of catchup mechanism to bring the node back in sync
        with it's peers.
        '''
        

    def _mpax_filter(func):
        '''
        Function decorator that filters out messages that do not match the
        current instance number
        '''
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
        '''
        Proposes a value for the current instance.

        request_id - arbitrary string that may be used to match the resolved
                     value to the originating request

        proposal_value - Value proposed for resolution

        instance - Optional instance number for which the proposal is valid.
                   InstanceMismatch is thrown If the current proposal number
                   does not match this parameter.
        '''
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

        
    def receive_prepare(self, from_uid, msg):
        '''
        Prepare messages must be inspected for "future" instance numbers and
        the catchup mechanism must be invoked if they are seen. This protects
        against the following case:
        
           * A threshold number of nodes resolve on a proposal
           * One of the nodes crashes before learning of the resolution
           * The rest of the nodes crash
           * All nodes recover
           * At this point, leadership cannot be obtained since the first node that
             crashed believes it is still working with paxos instance N whereas the
             other nodes believe they are working on paxos instance N+1. No quorum
             can be achieved.
        '''
        if msg['instance'] > self.instance:
            self.behind_in_sequence( msg['instance'] )

        elif msg['instance'] == self.instance:
            self.pax.recv_prepare( from_uid, ProposalID(*msg['proposal_id']) )

            if self.pax.persistance_required:
                self.persist()
        

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
            self.persist()

        
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
        pass

    
    def on_resolution(self, proposal_id, value):
        self.next_instance()



        
class MultiPaxosHeartbeatNode(MultiPaxosNode):
    '''
    This class provides a concrete implementation of MultiPaxosNode that uses
    the heartbeating mechanism defined in paxos.functional.HeartbeatNode to
    detect and recover from leadership failures.

    Instance Variables:
    
    hb_period       - Floating-point time in seconds between heartbeats
    liveness_window - Window of time within which at least one heartbeat message
                      must be seen for the the leader to be considered alive.
    '''

    hb_period         = 60  # Seconds
    liveness_window   = 180 # Seconds

    hb_poll_task      = None
    leader_pulse_task = None
    is_leader         = False

    def __init__(self, *args, **kwargs):

        self.hb_period       = kwargs.pop('hb_period',       self.hb_period)
        self.liveness_window = kwargs.pop('liveness_window', self.liveness_window)
            
        super(MultiPaxosHeartbeatNode, self).__init__(*args, **kwargs)

        self.instance_exceptions.add('heartbeat')


    def shutdown(self):
        if self.hb_poll_task.running:
            self.hb_poll_task.stop()

        if self.leader_pulse_task.running:
            self.leader_pulse_task.stop()

        super(MultiPaxosHeartbeatNode, self).shutdown()


    @defer.inlineCallbacks
    def initialize(self):
        yield super(MultiPaxosHeartbeatNode,self).initialize()
        self._start_tasks()


    def _recover_from_persistent_state(self, state):
        self.pax = self._new_paxos_node()
        
        self.pax.recover( state['promised_id'],
                          state['accepted_id'],
                          state['accepted_value'] )
        
        self._start_tasks()
        
        return defer.succeed(None)
        

    def _start_tasks(self):
        if self.hb_poll_task is None:
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
        
