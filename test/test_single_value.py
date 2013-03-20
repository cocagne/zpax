import os
import os.path
import sys
import random
import json
import tempfile
import shutil

from twisted.internet import reactor, defer
from twisted.trial import unittest

pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )
sys.path.append( os.path.join(pd(this_dir), 'examples') )
sys.path.append( os.path.join(pd(pd(this_dir)), 'paxos') )

from zpax import multi, tzmq, testhelper, durable

from zpax.testhelper import trace_messages, show_stacktrace

import single_value


def delay(t):
    d = defer.Deferred()
    reactor.callLater(t, lambda : d.callback(None) )
    return d



class TestReq (object):
    d = None
    last_val = None

    def __init__(self, channel='test_channel.clients', node_id='testcli'):
        self.net = testhelper.NetworkNode(node_id)
        self.channel = channel
        self.net.add_message_handler(channel, self)
        self.net.connect([])

    def close(self):
        if self.d is not None and not self.d.called:
            self.d.cancel()
    
    def propose(self, to_id, instance, value, req_id='req_id'):
        self.d = defer.Deferred()
        self.net.unicast_message(to_id, self.channel, 'propose_value', dict(instance=instance,
                                                                            proposed_value=value,
                                                                            request_id=req_id))
        return self.d

    def query(self, to_id):
        self.d = defer.Deferred()
        self.net.unicast_message(to_id, self.channel, 'query_value', dict())
        return self.d

    def receive_proposal_result(self, from_uid, msg):
        #print 'Propose Reply Received:', msg
        self.d.callback(msg)

    def receive_query_result(self, from_uid, msg):
        #print 'Query Result Received:', msg
        self.d.callback(msg)
        




class SingleValueTester(unittest.TestCase):

    durable_key = 'durable_id_{0}'
    
    def setUp(self):
        self.nodes       = dict()
        self.leader      = None
        self.dleader     = defer.Deferred()
        self.dlost       = None
        self.clients     = list()
        self.all_nodes   = 'a b c'.split()

        self.dd_store = durable.MemoryOnlyStateStore()

        testhelper.setup()

        
    def tearDown(self):
        for c in self.clients:
            c.close()
            
        for n in self.all_nodes:
            self.stop(n)
        
        # In ZeroMQ 2.1.11 there is a race condition for socket deletion
        # and recreation that can render sockets unusable. We insert
        # a short delay here to prevent the condition from occuring.
        #return delay(0.05)


    def new_client(self):
        zreq = TestReq()
        self.clients.append(zreq)
        return zreq
            

    def start(self,  node_names):

        def gen_cb(x, func):
            def cb():
                func(x)
            return cb

        zpax_nodes = dict()
        
        for node_name in node_names.split():
            if not node_name in self.all_nodes or node_name in self.nodes:
                continue

            n = single_value.SingleValueNode(testhelper.Channel('test_channel', testhelper.NetworkNode(node_name)),
                                             2,
                                             self.durable_key.format(node_name),
                                             self.dd_store)

            n.on_leadership_acquired = gen_cb(node_name, self._on_leader_acq)
            n.on_leadership_lost     = gen_cb(node_name, self._on_leader_lost)

            n.hb_period       = 0.05
            n.liveness_window = 0.15

            n.name = node_name

            self.nodes[node_name] = n

            n.net.connect([])
            n.initialize()


        
    def stop(self, node_names):
        for node_name in node_names.split():
            if node_name in self.nodes:
                self.nodes[node_name].shutdown()
                del self.nodes[node_name]


    def _on_leader_acq(self, node_id):
        prev        = self.leader
        self.leader = node_id
        if self.dleader:
            d, self.dleader = self.dleader, None
            reactor.callLater(0.01, lambda : d.callback( (prev, self.leader) ))

            
    def _on_leader_lost(self, node_id):
        if self.dlost:
            d, self.dlost = self.dlost, None
            d.callback(node_id)


    @defer.inlineCallbacks
    def wait_for_value_equals(self, client, to_id, value):
        qval = None
        while qval != value:
            yield delay(0.05)
            r = yield client.query(to_id)
            if r['value'] is not None:
                qval = r['value'][1]

            
    @defer.inlineCallbacks
    def set_value(self, client, to_id, instance, value):
        yield client.propose(to_id, instance, value)
        yield self.wait_for_value_equals( client, to_id, value )
    

    #@trace_messages
    @defer.inlineCallbacks
    def test_initial_leader(self):
        self.start('a b')
        yield self.dleader


    #trace_messages
    @defer.inlineCallbacks
    def test_set_initial_value(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client()

        yield self.dleader

        msg = yield c.query('a')

        self.assertEquals(msg, dict(current_instance=1, value=None))
        
        yield c.propose('a', 1, 'foo')

        yield self.wait_for_value_equals( c, 'a', 'foo' )
        

    @defer.inlineCallbacks
    def test_set_multiple_values(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client()

        yield self.dleader

        msg = yield c.query('a')

        self.assertEquals(msg, dict(current_instance=1, value=None))
        
        yield c.propose('a', 1, 'foo')
        yield self.wait_for_value_equals( c, 'a', 'foo' )

        yield c.propose('a', 2, 'bar')
        yield self.wait_for_value_equals( c, 'a', 'bar' )

        yield c.propose('a', 3, 'baz')
        yield self.wait_for_value_equals( c, 'a', 'baz' )





    @show_stacktrace
    #@trace_messages
    @defer.inlineCallbacks
    def test_shutdown_and_restart(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client()

        yield self.dleader

        yield c.propose('a', 1, 'foo')
        yield self.wait_for_value_equals( c, 'a', 'foo' )

        self.stop('a b')

        yield delay(0.05)

        self.dleader = defer.Deferred()

        self.start('a b')

        yield self.dleader

        msg = yield c.query('a')

        self.assertEquals(msg['value'], ('req_id', 'foo'))



        
    #trace_messages
    @defer.inlineCallbacks
    def test_shutdown_and_restart_with_outstanding_proposal(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client()

        yield self.dleader

        yield c.propose('a', 1, 'foo')
        yield self.wait_for_value_equals( c, 'a', 'foo' )

        self.stop('b')

        yield c.propose('a', 2, 'bar')

        self.assertTrue( self.nodes['a'].pax.proposed_value is not None )

        self.stop('a')

        yield delay(0.05)

        self.dleader = defer.Deferred()
        
        self.start('a b')

        yield self.dleader
        
        yield self.wait_for_value_equals( c, 'a', 'bar' )
        

            

    @defer.inlineCallbacks
    def test_node_recovery(self):
        self.start('a b c')

        d = defer.Deferred()
        c = self.new_client()

        yield self.dleader

        yield self.set_value(c, 'a', 1, 'foo')

        yield self.wait_for_value_equals( c, 'c', 'foo' )

        self.stop('c')

        yield self.set_value(c, 'a', 2, 'bar')
        yield self.set_value(c, 'a', 3, 'baz')
    
        self.start('c')

        yield self.wait_for_value_equals( c, 'c', 'baz' )
