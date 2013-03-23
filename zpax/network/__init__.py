'''
The networking package is implemented in terms of abstract NetworkNodes
and MessageEncoders. These allow the communication backends and protocols
to change independent of higher-level application code.

The NetworkNodes use channels to segregate related content. The 'channel'
module provides a convenience wrapper for NetworkNode instances that
sends and receives all messages over a specific channel.
'''

class IMessageEncoder (object):

    def encode(self, node_uid, message_type, parts):
        '''
        Encodes a message of the given type.

        Return value: list of encoded message segments
        '''


    def decode(self, encoded_parts):
        '''
        Decodes an encoded message.

        Return values: (from_uid, message_type, [message_parts...])
        '''
        

class INetworkNode (object):


    def add_message_handler(self, channel_name, handler):
        '''
        Adds a message handler for the specified channel. Multiple handlers may
        be specified for a channel but message processing stops with the first
        handler that implements a method named "receive_<message_type>". The
        arguments passed to the handler method are the node_uid of the message
        sender and the remaining message parts expanded into positional
        arguments.
        '''
        

    def connect(self, zpax_nodes):
        '''
        zpax_nodes - Dictionary of node_uid => (zmq_rtr_addr, zmq_pub_addr)
        '''


    def shutdown(self):
        '''
        Disconnects the node from the network.
        '''


    def broadcast_message(self, channel_name, message_type, *parts):
        '''
        Broadcasts a message to all peers on a channel
        '''


    def unicast_message(self, to_uid, channel_name, message_type, *parts):
        '''
        Sends a message to a single peer on a channel
        '''
