from twisted.internet import defer, task, reactor


nodes = dict() # uid => NetworkNode object


def setup():
    nodes.clear()


def broadcast_message( src_uid, message_type, *parts ):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = parts[0]
    for n in nodes.itervalues():
        if src_uid != n.node_uid or n.recv_self:
            n.dispatch_message( src_uid, message_type, parts )
            

def unicast_message( src_uid, dst_uid, message_type, *parts ):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = parts[0]
    if dst_uid in nodes:
        nodes[dst_uid].dispatch_message( src_uid, message_type, parts )



class NetworkNode (object):


    def __init__(self, node_uid):

        self.node_uid         = node_uid
        self.zpax_nodes       = None # Dictionary of node_uid -> (rtr_addr, pub_addr)
        self.dispatch_message = lambda x, y: None
        self.recv_self        = None
        

    def connect(self, zpax_nodes, recv_self_broadcast=True):
        self.zpax_nodes        = zpax_nodes
        self.recv_self         = recv_self_broadcast
        nodes[ self.node_uid ] = self
        

    def shutdown(self):
        if self.node_uid in nodes:
            del nodes[ self.node_uid ]


    def broadcast_message(self, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        if isinstance(parts, tuple):
            parts = list(parts)
        broadcast_message(self.node_uid, message_type, parts)


    def unicast_message(self, node_uid, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        if isinstance(parts, tuple):
            parts = list(parts)
        unicast_message(self.node_uid, node_uid, message_type, parts)

