from twisted.internet import defer, task, reactor

TRACE = False

nodes = dict() # uid => NetworkNode object


def setup():
    nodes.clear()


try:
    defer.gatherResults( [defer.succeed(None),], consumeErrors=True )
    use_consume = True
except TypeError:
    use_consume = False

def gatherResults( l ):
    if use_consume:
        return defer.gatherResults(l, consumeErrors=True)
    else:
        return defer.gatherResults(l)


def trace_messages( fn ):
    @defer.inlineCallbacks
    def wrapit(self, *args, **kwargs):
        global TRACE
        TRACE = True
        print ''
        print 'Trace:'
        yield fn(self, *args, **kwargs)
        TRACE = False
        print ''
    return wrapit



def broadcast_message( src_uid, channel_name, message_type, *parts ):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = parts[0]
    for n in nodes.values():
        if n.link_up:
            n.recv_message( src_uid, channel_name, message_type, parts )
            

def unicast_message( src_uid, dst_uid, channel_name, message_type, *parts ):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = parts[0]
    if dst_uid in nodes:
        nodes[dst_uid].recv_message( src_uid, channel_name, message_type, parts )



class NetworkNode (object):


    def __init__(self, node_uid):

        self.node_uid         = node_uid
        self.zpax_nodes       = None # Dictionary of node_uid -> (rtr_addr, pub_addr)
        self.message_handlers = dict()
        self.link_up          = False


    def add_message_handler(self, channel_name, handler):
        if not channel_name in self.message_handlers:
            self.message_handlers[ channel_name ] = list()
        self.message_handlers[channel_name].append( handler )
        

    def connect(self, zpax_nodes):
        self.zpax_nodes        = zpax_nodes
        self.link_up           = True
        nodes[ self.node_uid ] = self
        

    def shutdown(self):
        self.link_up = False
        if self.node_uid in nodes:
            del nodes[ self.node_uid ]


    def _dispatch_message(self, from_uid, channel_name, message_type, parts):
        handlers = self.message_handlers.get(channel_name, None)
        if handlers:
            for h in handlers:
                f = getattr(h, 'receive_' + message_type, None)
                if f:
                    f(from_uid, *parts)
                    break


    def recv_message(self, src_uid, channel_name, message_type, parts):
        if self.link_up:
            if TRACE:
                print src_uid, '=>', self.node_uid, '[rcv]', '[{0}]'.format(channel_name), message_type.ljust(15), parts
            self._dispatch_message( src_uid, channel_name, message_type, parts )
        else:
            if TRACE:
                print src_uid, '=>', self.node_uid, '[drp]', '[{0}]'.format(channel_name), message_type.ljust(15), parts


    def broadcast_message(self, channel_name, message_type, *parts):
        if not self.link_up:
            return
        
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        if isinstance(parts, tuple):
            parts = list(parts)
        broadcast_message(self.node_uid, channel_name, message_type, parts)


    def unicast_message(self, to_uid, channel_name, message_type, *parts):
        if not self.link_up:
            return
        
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        if isinstance(parts, tuple):
            parts = list(parts)
        unicast_message(self.node_uid, to_uid, channel_name, message_type, parts)

