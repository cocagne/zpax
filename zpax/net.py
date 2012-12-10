import os
import json
import random
import hashlib
import hmac
import base64

from zpax import tzmq

from twisted.internet import defer, task, reactor



class NetworkNode (object):


    def __init__(self, node_uid):

        self.node_uid         = node_uid

        self.zpax_nodes       = None # Dictionary of node_uid -> (rep_addr, pub_addr)

        self.pax_rep          = None
        self.pax_pub          = None
        self.pax_sub          = None
        self.dispatch_message = lambda x: None
        

    def connect(self, zpax_nodes):
        '''
        zpax_nodes - Dictionary of node_uid => (zmq_rep_addr, zmq_pub_addr)
        '''
        if not self.node_uid in zpax_nodes:
            raise Exception('Missing local node configuration')

        self.zpax_nodes = zpax_nodes
        
        if self.pax_rep:
            self.pax_rep.close()
            self.pax_pub.close()
            self.pax_sub.close()

        self.pax_rep = tzmq.ZmqRepSocket()
        self.pax_pub = tzmq.ZmqPubSocket()
        self.pax_sub = tzmq.ZmqSubSocket()        
                    
        self.pax_rep.linger = 0
        self.pax_pub.linger = 0
        self.pax_sub.linger = 0
        
        self.pax_rep.bind(zpax_nodes[self.node_uid][0])
        self.pax_pub.bind(zpax_nodes[self.node_uid][1])

        self.pax_rep.messageReceived = self._on_rep_received
        self.pax_sub.messageReceived = self._on_sub_received

        self.pax_sub.subscribe = 'zpax'
        
        for x in cur_remote:
            self.pax_sub.connect(x)


    def shutdown(self):
        self.pax_rep.close()
        self.pax_pub.close()
        self.pax_sub.close()
        self.pax_rep = None
        self.pax_pub = None
        self.pax_sub = None


    def broadcast_message(self, *parts):
        l = ['zpax']
        l.extend(parts)
        self.pax_pub.send( l )


    def unicast_message(self, node_uid, *parts):
        # TODO


    def _on_rep_received(self, parts):
        self.dispatch_message( parts )


    def _on_sub_received(self, parts):
        self.dispatch_message( parts )
