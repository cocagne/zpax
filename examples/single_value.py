import json

from zpax import multi

from twisted.internet import defer, task, reactor


class SingleValueNode (multi.MultiPaxosHeartbeatNode):
    '''
    This class implements a network node that uses Paxos to coordinate changes
    to a single, shared value.

    Clients propose changes, learn the current value, and wait for new values
    to be chosen via a Router socket. The Pub/Sub Paxos messaging is handled by
    node.BasicNode.
    '''
    catchup_delay = 10
    
    def __init__(self, paxos_net_channel, quorum_size, durable_data_id, durable_data_store):
        '''
        paxos_net_channel  - net.Channel instance
        quorum_size        - Paxos quorum size
        durable_data_id    - Unique ID for use with the durable_data_store
        durable_data_store - Implementer of durable.IDurableDataStore
        '''

        super(SingleValueNode,self).__init__(paxos_net_channel.create_subchannel('paxos'),
                                             quorum_size, durable_data_id, durable_data_store)

        self.catchup_channel  = paxos_net_channel.create_subchannel('catchup')
        self.client_channel   = paxos_net_channel.create_subchannel('clients')
        self.current_value    = None
        self.behind           = False
        self.catchup_cb       = None

        self.waiting_clients  = set() # Contains router addresses of clients waiting for updates

        self.catchup_channel.add_message_handler(self)
        self.client_channel.add_message_handler(self)
        
        
        
    def shutdown(self):
        if self.catchup_cb and self.catchup_cb.active():
            self.catchup_cb.cancel()
        super(SingleValueNode,self).shutdown()


    def _get_additional_persistent_state(self):
        x = dict( value = self.current_value )
        x.update( super(SingleValueNode,self)._get_additional_persistent_state() )
        return x

    
    def _recover_from_persistent_state(self, state):
        super(SingleValueNode,self)._recover_from_persistent_state(state)
        self.current_value = state.get('value', None)
        

    def behind_in_sequence(self, current_instance):
        super(SingleValueNode,self).behind_in_sequence(current_instance)
        self.behind = True
        self.catchup()


    def catchup(self):
        if self.behind:
            if self.leader_uid is not None:
                self.catchup_channel.unicast( self.leader_uid, 'get_value', dict() )
            else:
                self.catchup_channel.broadcast( 'get_value', dict() )
            self.catchup_cb = reactor.callLater(self.catchup_delay, self.catchup)

            
    def caught_up(self):
        self.behind = False
        
        if self.catchup_cb and self.catchup_cb.active():
            self.catchup_cb.cancel()
            self.catchup_cb = None

        #self.catchup_channel.broadcast( 'current_value', dict(instance=self.cur_instance,
        #                                                  proposal_id=self.cur_proposal,
        #                                                  value=self.current_value) )
                                                          


    def on_resolution(self, proposal_id, value):
        self.current_value = value
        super(SingleValueNode,self).on_resolution(proposal_id, value)
        self.caught_up()
        

    #--------------------------------------------------------------------------
    # Messaging
    #
    def receive_get_value(self, from_uid, msg):
        try:
            if not self.behind:
                self.catchup_channel.unicast( from_uid, 'current_value',
                                              dict(value=self.current_value,
                                                   current_instance=self.instance) )
        except:
            import traceback
            traceback.print_exc()
        

    def receive_current_value(self, from_uid, msg):
        self.current_value = msg['value']
        self.next_instance(msg['current_instance'])
        self.caught_up()

            
        
    def receive_propose_value(self, from_uid, msg):
        try:
            self.set_proposal(msg['request_id'],
                              msg['proposed_value'],
                              msg['instance'])
            self.client_channel.unicast( from_uid, 'proposal_result', dict() )
        except multi.ProposalFailed, e:
            self.client_channel.unicast( from_uid, 'proposal_result',
                                         dict(error_message=str(e), current_instance=e.current_instance))

            
    def receive_query_value(self, from_uid, msg):
        self.client_channel.unicast( from_uid, 'query_result',
                                    dict( current_instance=self.instance,
                                          value=self.current_value ))

        
