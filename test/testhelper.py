from twisted.internet import defer, task, reactor

TRACE = False

nodes = dict() # uid => NetworkNode object


def setup():
    nodes.clear()


def broadcast_message( src_uid, message_type, *parts ):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = parts[0]
    for n in nodes.values():
        if n.link_up:
            n.recv_message( src_uid, message_type, parts )
            

def unicast_message( src_uid, dst_uid, message_type, *parts ):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = parts[0]
    if dst_uid in nodes:
        nodes[dst_uid].recv_message( src_uid, message_type, parts )



class NetworkNode (object):


    def __init__(self, node_uid):

        self.node_uid         = node_uid
        self.zpax_nodes       = None # Dictionary of node_uid -> (rtr_addr, pub_addr)
        self.message_handlers = list()
        self.link_up          = False
        

    def connect(self, zpax_nodes):
        self.zpax_nodes        = zpax_nodes
        self.link_up           = True
        nodes[ self.node_uid ] = self
        

    def shutdown(self):
        self.link_up = False
        if self.node_uid in nodes:
            del nodes[ self.node_uid ]


    def _dispatch_message(self, from_uid, message_type, parts):
        for h in self.message_handlers:
            f = getattr(h, 'receive_' + message_type, None)
            if f:
                f(from_uid, *parts)
                break


    def recv_message(self, src_uid, message_type, parts):
        if self.link_up:
            if TRACE:
                print src_uid, '=>', self.node_uid, '[rcv]', message_type.ljust(15), parts
            self._dispatch_message( src_uid, message_type, parts )
        else:
            if TRACE:
                print src_uid, '=>', self.node_uid, '[drp]', message_type.ljust(15), parts


    def broadcast_message(self, message_type, *parts):
        if not self.link_up:
            return
        
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        if isinstance(parts, tuple):
            parts = list(parts)
        broadcast_message(self.node_uid, message_type, parts)


    def unicast_message(self, to_uid, message_type, *parts):
        if not self.link_up:
            return
        
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        if isinstance(parts, tuple):
            parts = list(parts)
        unicast_message(self.node_uid, to_uid, message_type, parts)

