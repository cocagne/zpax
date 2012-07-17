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
    
    def __init__(self, node_uid,
                 local_rtr_addr,
                 initial_value='',
                 durable_dir=None,
                 object_id=None):

        super(SimpleNode,self).__init__(node_uid, durable_dir, object_id)

        self.local_rtr_addr   = local_rtr_addr
        self.value            = initial_value

        self.waiting_clients  = set() # Contains router addresses of clients waiting for updates

        self.router           = tzmq.ZmqRouterSocket()

        self.router.messageReceived = self._on_router_received
        
        self.router.bind(self.local_rtr_addr)

        
    def onShutdown(self):
        self.router.close()

        
    def onBehindInSequence(self, old_sequence_number, new_sequence_number):
        self.publish( 'get_value' )

        
    def onProposalResolution(self, instance_num, value):
        for addr in self.waiting_clients:
            self.reply_value(addr)

        self.waiting_clients.clear()
        

    #--------------------------------------------------------------------------
    # Additional PubSub Messaging
    #
    # For catching up with the current value, we'll take the simple and
    # inefficient approach of using the node.BasicNode's PUB/SUB sockets.
    #
    def publish_value(self):
        self.publish( 'value', dict(value=self.value) )

        
    def _SUB_get_value(self, header):
        self.publish_value()
        

    def _SUB_value(self, header):
        if header['seq_num'] > self.sequence_number:
            self.value = header['value']
            self.slewSequenceNumber(header['seq_num'])

            
    #--------------------------------------------------------------------------
    # Router Socket Messaging
    #
    # ZeroMQ Router sockets prepend the message with the reply address and an
    # empty message part. The _on_router_received method extracts the address
    # and uses the same message dispatching logic as JSONResponder.
    #
    def reply(self, addr, *parts):
        jparts = [ json.dumps(p) for p in parts ]
        self.router.send( addr, '', *jparts )

        
    def reply_value(self, addr):
        self.reply(addr, dict(sequence_number=self.sequence_number-1, value=self.value) )

        
    def _on_router_received(self, msg_parts):
        try:
            addr  = msg_parts[0]
            parts = [ json.loads(p) for p in msg_parts[2:] ]
        except ValueError:
            print 'Invalid JSON: ', msg_parts
            return

        if not parts or not 'type' in parts[0]:
            print 'Missing message type', parts
            return

        fobj = getattr(self, '_ROUTER_' + parts[0]['type'], None)
        
        if fobj:
            fobj(addr, *parts)

        
    def _ROUTER_propose_value(self, addr, header):
        try:
            self.proposeValue(header['value'], header['sequence_number'])
            self.reply(addr, dict(proposed=True))
        except node.ProposalFailed, e:
            self.reply(addr, dict(proposed=False, message=str(e)))

            
    def _ROUTER_query_value(self, addr, header):
        self.reply_value(addr)

        
    def _ROUTER_get_next_value(self, addr, header):
        if header['sequence_number'] < self.sequence_number:
            self.reply_value(addr)
        else:
            self.waiting_clients.add( addr )
        
