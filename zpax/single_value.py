import json

from zpax import multi, tzmq

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
    
    def __init__(self, net_node, quorum_size, rep_addr, pub_addr):

        super(SingleValueNode,self).__init__(net_node, quorum_size)

        self.rep_addr         = rep_addr
        self.pub_addr         = pub_addr
        self.cur_instance     = None
        self.cur_proposal     = None
        self.cur_value        = None
        self.behind           = False
        self.catchup_cb       = None

        self.waiting_clients  = set() # Contains router addresses of clients waiting for updates

        self.rep              = tzmq.ZmqRepSocket()
        self.pub              = tzmq.ZmqPubSocket()

        self.rep.linger = 0
        self.rep.messageReceived = self._on_rep_received

        self.pub.linger = 0
        
        self.rep.bind(self.rep_addr)
        self.pub.bind(self.pub_addr)

        
    def shutdown(self):
        self.rep.close()
        self.pub.close()
        self.caught_up()
        super(SingleValueNode,self).shutdown()


    def behind_in_sequence(self, current_instance):
        super(SingleValueNode,self).behind_in_sequence(current_instance)
        self.behind = True
        self.catchup()


    def catchup(self):
        if self.behind:
            if self.leader_uid is not None:
                self.unicast( self.leader_uid, 'get_value' )
            self.catchup_cb = reactor.callLater(self.catchup_delay, self.catchup)

            
    def caught_up(self):
        self.behind = False
        
        if self.catchup_cb and self.catchup_cb.active():
            self.catchup_cb.cancel()
            self.catchup_cb = None

        self.pub.send( ['current_value', json.dumps(dict( instance=self.cur_instance,
                                                          proposal_id=self.cur_proposal,
                                                          value=self.cur_value))] )
                                                          


    def on_resolution(self, proposer_obj, proposal_id, value):
        super(SingleValueNode,self).on_resolution(proposer_obj, proposal_id, value)
        self.cur_instance = self.instance - 1
        self.cur_proposal = proposal_id
        self.cur_value    = value
        self.caught_up()
        

    def receive_get_value(self, from_uid, kw):
        if not self.behind:
            self.unicast( from_uid, 'current_value', value=self.cur_value, proposal=self.cur_proposal )
        

    def receive_current_value(self, from_uid, kw):
        self.cur_instance = self.instance - 1
        self.cur_proposal = kw['proposal']
        self.cur_value    = kw['value']
        self.caught_up()

            
    #--------------------------------------------------------------------------
    # Rep Socket Messaging
    #
    def reply(self, **kw):
        self.rep.send( json.dumps(kw) )

        
    def _on_rep_received(self, msg_parts):
        if not len(msg_parts) == 1:
            return # bad message
        
        try:
            msg = json.loads(msg_parts[0])

            fobj = getattr(self, '_REP_' + msg['type'])
            
        except ValueError:
            print 'Invalid JSON: ', msg_parts[0]
            return

        except KeyError:
            print 'Invalid Message Type', msg_parts[0]
            return

        fobj(msg)
            
        
    def _REP_propose_value(self, msg):
        try:
            self.set_proposal(msg['request_id'],
                              msg['proposed_value'],
                              msg['instance'],)
            self.reply(proposed=True)
        except multi.ProposalFailed, e:
            self.reply(proposed=False, error_message=str(e), current_instance=e.current_instance)

            
    def _REP_query_value(self, msg):
        self.reply(instance=self.cur_instance,
                   request_id=self.cur_proposal,
                   value=self.cur_value)

        
