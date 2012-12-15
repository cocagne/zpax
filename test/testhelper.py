from twisted.internet import defer, task, reactor


nodes = dict() # uid => NetworkNode object


def setup():
    nodes.clear()


def broadcast_message( src_uid, message_type, *parts ):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = parts[0]
    for n in nodes.values():
        if n.link_up and (src_uid != n.node_uid or n.recv_self):
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
        self.dispatch_message = lambda x, y: None
        self.recv_self        = None
        self.link_up          = False
        

    def connect(self, zpax_nodes, recv_self_broadcast=True):
        self.zpax_nodes        = zpax_nodes
        self.recv_self         = recv_self_broadcast
        self.link_up           = True
        nodes[ self.node_uid ] = self
        

    def shutdown(self):
        self.link_up = False
        if self.node_uid in nodes:
            del nodes[ self.node_uid ]


    def recv_message(self, src_uid, message_type, parts):
        if self.link_up:
            self.dispatch_message( src_uid, message_type, parts )


    def broadcast_message(self, message_type, *parts):
        if not self.link_up:
            return
        
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        if isinstance(parts, tuple):
            parts = list(parts)
        broadcast_message(self.node_uid, message_type, parts)


    def unicast_message(self, node_uid, message_type, *parts):
        if not self.link_up:
            return
        
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        if isinstance(parts, tuple):
            parts = list(parts)
        unicast_message(self.node_uid, node_uid, message_type, parts)

