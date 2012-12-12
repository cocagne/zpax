from twisted.internet import defer, task, reactor

from zpax import tzmq


class SimpleEncoder(object):
    def encode(self, node_uid, message_type, parts):
        return ['{0}\0{1}'.format(node_uid, message_type)] + list(parts)

    def decode(self, parts):
        from_uid, message_type = parts[0].split('\0')
        return from_uid, message_type, parts[1:]
    

class NetworkNode (object):


    def __init__(self, node_uid):

        self.node_uid         = node_uid

        self.zpax_nodes       = None # Dictionary of node_uid -> (rtr_addr, pub_addr)

        self.pax_rtr          = None
        self.pax_pub          = None
        self.pax_sub          = None
        self.encoder          = SimpleEncoder()
        self.dispatch_message = lambda x, y: None
        

    def connect(self, zpax_nodes, recv_self_broadcast=True):
        '''
        zpax_nodes - Dictionary of node_uid => (zmq_rtr_addr, zmq_pub_addr)
        '''
        if not self.node_uid in zpax_nodes:
            raise Exception('Missing local node configuration')

        self.zpax_nodes = zpax_nodes
        
        if self.pax_rtr:
            self.pax_rtr.close()
            self.pax_pub.close()
            self.pax_sub.close()

        self.pax_rtr = tzmq.ZmqRouterSocket()
        self.pax_pub = tzmq.ZmqPubSocket()
        self.pax_sub = tzmq.ZmqSubSocket()

        self.pax_rtr.identity = self.node_uid
                    
        self.pax_rtr.linger = 0
        self.pax_pub.linger = 0
        self.pax_sub.linger = 0
        
        self.pax_rtr.bind(zpax_nodes[self.node_uid][0])
        self.pax_pub.bind(zpax_nodes[self.node_uid][1])

        self.pax_rtr.messageReceived = self._on_rtr_received
        self.pax_sub.messageReceived = self._on_sub_received

        self.pax_sub.subscribe = 'zpax'
        
        for node_uid, tpl in zpax_nodes.iteritems():
            if node_uid != self.node_uid or recv_self_broadcast:
                self.pax_sub.connect(tpl[1])
                
            if self.node_uid < node_uid:
                # We only need 1 connection between any two router nodes so
                # we'll make it the responsibility of the lower UID node to
                # initiate the connection
                self.pax_rtr.connect(tpl[0])


    def shutdown(self):
        self.pax_rtr.close()
        self.pax_pub.close()
        self.pax_sub.close()
        self.pax_rtr = None
        self.pax_pub = None
        self.pax_sub = None


    def broadcast_message(self, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = ['zpax']
        l.extend( self.encoder.encode(self.node_uid, message_type, parts) )
        self.pax_pub.send( l )


    def unicast_message(self, node_uid, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = [node_uid]
        l.extend( self.encoder.encode(self.node_uid, message_type, parts) )
        self.pax_rtr.send( l )


    def _on_rtr_received(self, raw_parts):
        # discard source address. We'll use the one embedded in the message
        # for consistency
        from_uid, message_type, parts = self.encoder.decode( raw_parts[1:] )
        self.dispatch_message( from_uid, message_type, parts )


    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        from_uid, message_type, parts = self.encoder.decode( raw_parts[1:] )
        self.dispatch_message( from_uid, message_type, parts )
