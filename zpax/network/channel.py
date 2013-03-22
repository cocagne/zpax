
class Channel (object):
    '''
    Wraps a NetworkNode object with an interface that sends and receives over
    a specific channel
    '''

    def __init__(self, channel_name, net_node):
        self.channel_name = channel_name
        self.net_node     = net_node

        
    @property
    def node_uid(self):
        return self.net_node.node_uid

    
    def create_subchannel(self, sub_channel_name):
        return Channel( self.channel_name + '.' + sub_channel_name, self.net_node )

        
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
        
