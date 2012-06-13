import json

from zpax import node, tzmq

from twisted.internet import defer, task, reactor


class SimpleNode (node.BasicNode):
    '''
    This class implements a network node that uses Paxos to coordinate
    changes to a single, shared value.

    Clients propose changes, learn the current value, and wait for new
    values to be chosen via a Router socket. The Pub/Sub Paxos
    messaging is handled by node.BasicNode.
    '''

    chatty = False
    
    def __init__(self, node_uid,
                 local_pub_sub_addr,   local_rtr_addr,
                 initial_value='',
                 durable_dir=None,
                 object_id=None):

        super(SimpleNode,self).__init__( node_uid,
                                         local_pub_sub_addr,
                                         durable_dir,
                                         object_id )

        self.local_rtr_addr   = local_rtr_addr
        self.value            = initial_value

        self.waiting_clients  = set() # Contains router addresses of clients waiting for updates

        self.router           = tzmq.ZmqRouterSocket()

        self.router.messageReceived = self._on_router_received
        
        self.router.bind(self.local_rtr_addr)


        
    def onShutdown(self):
        self.router.close()

        
    def onLeadershipAcquired(self):
        if self.chatty:
            print self.node_uid, 'I have the leader!', self.mpax.node.proposer.value


    def onLeadershipLost(self):
        if self.chatty:
            print self.node_uid, 'I LOST the leader!'


    def onLeadershipChanged(self, prev_leader_uid, new_leader_uid):
        if self.chatty:
            print '*** Change of guard: ', prev_leader_uid, new_leader_uid


    def onBehindInSequence(self):
        self.publish( 'get_value' )

        
    def onProposalResolution(self, instance_num, value):
        if self.chatty:
            print '*** Resolution! ', instance_num, repr(value)
        
        for addr in self.waiting_clients:
            self.reply_value(addr)

        self.waiting_clients.clear()
        

    #----------------------------
    # Additional PubSub Messaging
    #

    def publish_value(self):
        self.publish( 'value', dict(value=self.value) )

        
    def _SUB_get_value(self, header):
        self.publish_value()
        

    def _SUB_value(self, header):
        if header['seq_num'] > self.sequence_number:
            self.value = header['value']
            self.slewSequenceNumber(header['seq_num'])

            
    #-------------------------------
    # Router Socket Messaging
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
        try:
            if self.chatty:
                print "Proposing value: ", self.sequence_number, header['value']
            self.proposeValue(header['value'], header['sequence_number'])
            self.reply(addr, dict(proposed=True))
        except node.ProposalFailed, e:
            if self.chatty:
                print 'Proposal FAILED: ', str(e)
            self.reply(addr, dict(proposed=False, message=str(e)))

            
    def _on_router_query_value(self, addr, header):
        self.reply_value(addr)

        
    def _on_router_get_next_value(self, addr, header):
        if header['sequence_number'] < self.sequence_number:
            self.reply_value(addr)
        else:
            self.waiting_clients.add( addr )
        
