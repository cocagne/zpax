import os
import os.path
import sys

pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )


from zpax import net

from twisted.internet import reactor, defer
from twisted.trial import unittest


def delay(t):
    d = defer.Deferred()
    reactor.callLater(t, lambda : d.callback(None) )
    return d



    
all_nodes = 'A B C'.split()


class NetworkNodeTester(unittest.TestCase):

    def setUp(self):
        self.nodes = dict()
        
        for node_uid in all_nodes:
            self.nodes[ node_uid ] = net.NetworkNode( node_uid )

    
    def tearDown(self):
        for n in self.nodes.itervalues():
            n.shutdown()
            
        # In ZeroMQ 2.1.11 there is a race condition for socket deletion
        # and recreation that can render sockets unusable. We insert
        # a short delay here to prevent the condition from occuring.
        return delay(0.05)

        
    def connect(self, recv_self=True):
        zpax_nodes = dict()
        
        for node_uid in all_nodes:
            zpax_nodes[ node_uid ] = ('ipc:///tmp/ts_{}_rtr'.format(node_uid),
                                      'ipc:///tmp/ts_{}_pub'.format(node_uid))
            
        for n in self.nodes.itervalues():
            n.connect( zpax_nodes, recv_self )

            
    @defer.inlineCallbacks
    def test_unicast_connections(self):
        self.connect()
        
        yield delay(1) # wait for connections to establish

        msgs = dict()
        
        def gend( nid ):
            def md( from_uid, parts ):
                msgs[ nid ].append( (from_uid, parts) )
            return md

        for n in all_nodes:
            msgs[ n ] = list()
            self.nodes[n].dispatch_message = gend(n)
            
        def s(src, dst, msg):
            self.nodes[src].unicast_message(dst, msg)

        s('A', 'B', 'AB')
        s('A', 'C', 'AC')
        s('B', 'A', 'BA')
        s('B', 'C', 'BC')
        s('C', 'A', 'AC')
        s('C', 'B', 'CB')

        yield delay(0.05) # process messages

        for l in msgs.itervalues():
            l.sort()

        expected = {'A': [('B', ['BA']), ('C', ['AC'])],
                    'B': [('A', ['AB']), ('C', ['CB'])],
                    'C': [('A', ['AC']), ('B', ['BC'])]}

        self.assertEquals( msgs, expected )

        
    @defer.inlineCallbacks
    def test_broadcast_connections_no_recv_self(self):
        self.connect(False)
        
        yield delay(1) # wait for connections to establish

        msgs = dict()
        
        def gend( nid ):
            def md( from_uid, parts ):
                msgs[ nid ].append( (from_uid, parts) )
            return md

        for n in all_nodes:
            msgs[ n ] = list()
            self.nodes[n].dispatch_message = gend(n)
            
        def s(src, msg):
            self.nodes[src].broadcast_message(msg)

        s('A', 'msgA')
        s('B', 'msgB')
        s('C', 'msgC')

        yield delay(0.05) # process messages

        for l in msgs.itervalues():
            l.sort()

        expected = {'A': [(None, ['msgB']), (None, ['msgC'])],
                    'B': [(None, ['msgA']), (None, ['msgC'])],
                    'C': [(None, ['msgA']), (None, ['msgB'])]}

        self.assertEquals( msgs, expected )

        
    @defer.inlineCallbacks
    def test_broadcast_connections_recv_self(self):
        self.connect()
        
        yield delay(1) # wait for connections to establish

        msgs = dict()
        
        def gend( nid ):
            def md( from_uid, parts ):
                msgs[ nid ].append( (from_uid, parts) )
            return md

        for n in all_nodes:
            msgs[ n ] = list()
            self.nodes[n].dispatch_message = gend(n)
            
        def s(src, msg):
            self.nodes[src].broadcast_message(msg)

        s('A', 'msgA')
        s('B', 'msgB')
        s('C', 'msgC')

        yield delay(0.05) # process messages

        for l in msgs.itervalues():
            l.sort()

        expected = {'A': [(None, ['msgA']), (None, ['msgB']), (None, ['msgC'])],
                    'B': [(None, ['msgA']), (None, ['msgB']), (None, ['msgC'])],
                    'C': [(None, ['msgA']), (None, ['msgB']), (None, ['msgC'])]}

        self.assertEquals( msgs, expected )

    
