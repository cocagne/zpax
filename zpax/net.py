from twisted.internet import defer, task, reactor

from zpax import tzmq


class SimpleEncoder(object):
    def encode(self, node_uid, message_type, parts):
        return ['{0}\0{1}'.format(node_uid, message_type)] + list(parts)

    def decode(self, parts):
        from_uid, message_type = parts[0].split('\0')
        return from_uid, message_type, parts[1:]
    

class Channel (object):
    '''
    Wraps a NetworkNode object with an interface that sends and receives over
    a specific channel
    '''

    def __init__(self, channel_name, net_node):
        self.channel_name = channel_name
        self.net_node     = net_node

        
    def add_message_handler(self, handler):
        self.net_node.add_message_handler(self.channel_name, handler)
        

    def connect(self, *args, **kwargs):
        self.net_node.connect(*args, **kwargs)


    def shutdown(self):
        self.net_node.shutdown()


    def broadcast(self, message_type, *parts):
        self.net_node.broadcast_message(self.channel_name, message_type, *parts)


    def unicast(self, to_uid, message_type, *parts):
        self.net_node.unicast_message(to_uid, self.channel_name, message_type, *parts)

        
class NetworkNode (object):
    '''
    Messages are handled by adding instances to the message_handlers list. The
    first instance that contains a method named 'nn_receive_<message_type>'
    will have that message called. The first argument is always the message
    sender's node_uid. The remaining positional arguments are filled with the
    parts of the ZeroMQ message.
    '''

    def __init__(self, node_uid, encoder=SimpleEncoder()):

        self.node_uid         = node_uid

        self.zpax_nodes       = None # Dictionary of node_uid -> (rtr_addr, pub_addr)

        self.pax_rtr          = None
        self.pax_pub          = None
        self.pax_sub          = None
        self.encoder          = encoder
        self.message_handlers = dict() # Dictionary of channel_name => list( message_handlers )

        
    def add_message_handler(self, channel_name, handler):
        if not channel_name in self.message_handlers:
            self.message_handlers[ channel_name ] = list()
        self.message_handlers[channel_name].append( handler )
        

    def connect(self, zpax_nodes):
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


    def broadcast_message(self, channel_name, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = ['zpax', channel_name]
        l.extend( self.encoder.encode(self.node_uid, message_type, parts) )
        self.pax_pub.send( l )


    def unicast_message(self, to_uid, channel_name, message_type, *parts):
        if to_uid == self.node_uid:
            self.dispatch_message( self.node_uid, channel_name, message_type, parts )
            return
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = [str(to_uid), channel_name]
        l.extend( self.encoder.encode(self.node_uid, message_type, parts) )
        self.pax_rtr.send( l )


    def _dispatch_message(self, from_uid, channel_name, message_type, parts):
        handlers = self.message_handlers.get(channel_name, None)
        if handlers:
            for h in handlers:
                f = getattr(h, 'receive_' + message_type, None)
                if f:
                    f(from_uid, *parts)
                    break
            
    def _on_rtr_received(self, raw_parts):
        # discard source address. We'll use the one embedded in the message
        # for consistency
        channel_name = raw_parts[1]
        from_uid, message_type, parts = self.encoder.decode( raw_parts[2:] )
        self._dispatch_message( from_uid, channel_name, message_type, parts )

        
    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        channel_name = raw_parts[1]
        from_uid, message_type, parts = self.encoder.decode( raw_parts[2:] )
        self._dispatch_message( from_uid, channel_name, message_type, parts )
