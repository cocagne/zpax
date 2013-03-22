import os
import os.path
import sys

from twisted.internet import reactor, defer
from twisted.trial import unittest


pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )


from zpax.network import zmq_node, test_node


def delay(t):
    d = defer.Deferred()
    reactor.callLater(t, lambda : d.callback(None) )
    return d

    
all_nodes = 'A B C'.split()


class NetworkNodeTesterBase(object):

    NodeKlass  = None
    need_delay = False

    def setUp(self):
        self._pre_setup()
        
        self.nodes = dict()
        
        for node_uid in all_nodes:
            self.nodes[ node_uid ] = self.NodeKlass( node_uid )

        return self._setup()

    
    def tearDown(self):
        for n in self.nodes.itervalues():
            n.shutdown()

        return self._teardown()


    def _pre_setup(self):
        pass
    
    def _setup(self):
        pass

    def _teardown(self):
        pass


    def delay(self, t):
        if self.need_delay:
            return delay(t)
        else:
            return defer.succeed(None)

        
    def connect(self, recv_self=True):
        zpax_nodes = dict()
        
        for node_uid in all_nodes:
            zpax_nodes[ node_uid ] = ('ipc:///tmp/ts_{}_rtr'.format(node_uid),
                                      'ipc:///tmp/ts_{}_pub'.format(node_uid))
            
        for n in self.nodes.itervalues():
            n.connect( zpax_nodes )

            
    @defer.inlineCallbacks
    def test_unicast_connections(self):
        self.connect()
        
        yield self.delay(1) # wait for connections to establish

        msgs = dict()

        class H(object):
            def __init__(self, nid):
                self.nid = nid

            def receive_foomsg(self, from_uid, arg):
                msgs[ self.nid ].append( (from_uid, arg) )

        for n in all_nodes:
            msgs[ n ] = list()
            self.nodes[n].add_message_handler('test_channel', H(n))
            
        def s(src, dst, msg):
            self.nodes[src].unicast_message(dst, 'test_channel', 'foomsg', msg)

        s('A', 'B', 'AB')
        s('A', 'C', 'AC')
        s('B', 'A', 'BA')
        s('B', 'C', 'BC')
        s('C', 'A', 'AC')
        s('C', 'B', 'CB')

        yield self.delay(0.05) # process messages

        for l in msgs.itervalues():
            l.sort()

        expected = {'A': [('B', 'BA'), ('C', 'AC')],
                    'B': [('A', 'AB'), ('C', 'CB')],
                    'C': [('A', 'AC'), ('B', 'BC')]}

        self.assertEquals( msgs, expected )


    @defer.inlineCallbacks
    def test_multiple_arguments(self):
        self.connect()
        
        yield self.delay(1) # wait for connections to establish

        msgs = dict()

        class H(object):
            def __init__(self, nid):
                self.nid = nid

            def receive_foomsg(self, from_uid, arg0, arg1, arg2):
                msgs[ self.nid ].append( (from_uid, arg0, arg1, arg2) )

        for n in all_nodes:
            msgs[ n ] = list()
            self.nodes[n].add_message_handler('test_channel', H(n))
            
        def s(src, dst, msg0, msg1, msg2):
            self.nodes[src].unicast_message(dst, 'test_channel', 'foomsg', msg0, msg1, msg2)

        s('A', 'B', 'AB', 'foo', 'bar')

        yield self.delay(0.05) # process messages

        for l in msgs.itervalues():
            l.sort()

        expected = {'A': [],
                    'B': [('A', 'AB', 'foo', 'bar')],
                    'C': [] }

        self.assertEquals( msgs, expected )

        

        
    @defer.inlineCallbacks
    def test_broadcast_connections_recv(self):
        self.connect()
        
        yield self.delay(1) # wait for connections to establish

        msgs = dict()

        class H(object):
            def __init__(self, nid):
                self.nid = nid

            def receive_foomsg(self, from_uid, arg):
                msgs[ self.nid ].append( (from_uid, arg) )

        for n in all_nodes:
            msgs[ n ] = list()
            self.nodes[n].add_message_handler('test_channel', H(n))
            
        def s(src, msg):
            self.nodes[src].broadcast_message('test_channel', 'foomsg', msg)

        s('A', 'msgA')
        s('B', 'msgB')
        s('C', 'msgC')

        yield self.delay(0.05) # process messages

        for l in msgs.itervalues():
            l.sort()

        expected = {'A': [('A', 'msgA'), ('B', 'msgB'), ('C', 'msgC')],
                    'B': [('A', 'msgA'), ('B', 'msgB'), ('C', 'msgC')],
                    'C': [('A', 'msgA'), ('B', 'msgB'), ('C', 'msgC')]}


        self.assertEquals( msgs, expected )


    @defer.inlineCallbacks
    def test_multiple_handlers(self):
        self.connect()
        
        yield self.delay(1) # wait for connections to establish

        msgs = dict()

        class H(object):
            def __init__(self, nid):
                self.nid = nid

            def receive_foomsg(self, from_uid, arg):
                msgs[ self.nid ].append( (from_uid, arg) )

        for n in all_nodes:
            msgs[ n ] = list()
            self.nodes[n].add_message_handler('test_channel', H(n))


        class H2(object):
            def receive_special(self, from_uid, arg):
                msgs['C'].append( (from_uid, 'special', arg) )

        self.nodes['C'].add_message_handler( 'test_channel', H2() )
            
        def s(src, msg):
            self.nodes[src].broadcast_message('test_channel', 'foomsg', msg)

        s('A', 'msgA')
        s('B', 'msgB')
        s('C', 'msgC')

        self.nodes['A'].broadcast_message('test_channel', 'special', 'blah')

        yield self.delay(0.05) # process messages

        for l in msgs.itervalues():
            l.sort()

        expected = {'A': [('A', 'msgA'), ('B', 'msgB'), ('C', 'msgC')],
                    'B': [('A', 'msgA'), ('B', 'msgB'), ('C', 'msgC')],
                    'C': [('A', 'msgA'), ('A', 'special', 'blah'), ('B', 'msgB'), ('C', 'msgC')]}


        self.assertEquals( msgs, expected )



class ZeroMQNodeTester(NetworkNodeTesterBase, unittest.TestCase):

    NodeKlass  = zmq_node.NetworkNode
    need_delay = True

    def _teardown(self):
        # In ZeroMQ 2.1.11 there is a race condition for socket deletion
        # and recreation that can render sockets unusable. We insert
        # a short delay here to prevent the condition from occuring.
        return delay(0.05)
    
    

class TestNodeTester(NetworkNodeTesterBase, unittest.TestCase):
    
    NodeKlass      = test_node.NetworkNode

    def _pre_setup(self):
        test_node.setup()
