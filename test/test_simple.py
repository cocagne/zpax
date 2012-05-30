import os
import os.path
import sys
import json

import random

pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )
sys.path.append( os.path.join(pd(pd(this_dir)), 'paxos') )


from zpax import simple, node, tzmq

from twisted.internet import reactor, defer
from twisted.trial import unittest


class TestHBP (node.BasicHeartbeatProposer):
    hb_period       = 0.1
    liveness_window = 0.3

    
class TestKV (simple.SimpleNode):
    CATCHUP_RETRY_DELAY  = 0.2
    CATCHUP_NUM_ITEMS    = 2

    hb_proposer_klass = TestHBP

    #chatty=True



class TestReq(tzmq.ZmqReqSocket):

    def jsend(self, **kwargs):
        self.send( json.dumps(kwargs) )

    def messageReceived(self, parts):
        self.jrecv( [ json.loads(p) for p in parts ] )

    def jrecv(self, parts):
        pass

    
all_nodes = 'a b c'.split()


class SimpleTest(unittest.TestCase):

    def setUp(self):
        self.nodes       = dict()
        self.quorum_size = 2
        self.leader      = None
        self.dleader     = None
        self.dlost       = None
        self.clients     = list()
        

        
    def tearDown(self):
        for c in self.clients:
            c.close()
            
        for n in all_nodes:
            self.stop(n)
            

    def start(self,  node_names):

        def gen_cb(x):
            def cb():
                self._on_leader_acq(x)
            return cb
        
        for node_name in node_names.split():
            if not node_name in all_nodes:
                continue
            n = TestKV( 'ipc:///tmp/ts_{}_rep'.format(node_name),
                        'ipc:///tmp/ts_{}_pub'.format(node_name),
                        'ipc:///tmp/ts_{}_rtr'.format(node_name),
                        ['ipc:///tmp/ts_{}_pub'.format(n) for n in all_nodes],
                        self.quorum_size )

            
            
            n.onLeadershipAcquired = gen_cb(node_name)
            n.onLeadershipLost     = gen_cb(node_name)
        
            self.nodes[node_name] = n

        
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
            d.callback( (prev, self.leader) )

            
    def _on_leader_lost(self, node_id):
        if self.dlost:
            d, self.dlost = self.dlost, None
            d.callback(node_id)


    def new_client(self, node_name=None):
        zreq = TestReq()
        self.clients.append(zreq)
        if node_name:
            zreq.connect('ipc:///tmp/ts_{}_rtr'.format(node_name))
        return zreq


    def chatty(self, nodes=None):
        if nodes is None:
            nodes = all_nodes
        else:
            nodes = nodes.split()

        for x in nodes:
            if x in self.nodes:
                self.nodes[x].chatty = True
        

    #def get_addr(self):
    #    return 'ipc:///tmp/ts_{}_rtr'.format(node_name)

        
    def test_initial_leader(self):
        self.dleader = defer.Deferred()
        self.start('a b')
        self.dleader.addCallback( lambda _: self.assertTrue(True) )

        return self.dleader


    def test_leader_change_on_new_node(self):
        self.dleader = defer.Deferred()
        self.start('a b')

        d2 = defer.Deferred()
        d2.addCallback( lambda _: self.assertTrue(True) )
        
        def onleader1(tpl):
            self.stop(tpl[1])
            self.dleader = d2
            self.start('c')
        
        self.dleader.addCallback( onleader1 )
        return d2

    
    def test_leader_change_on_failed_node(self):
        self.dleader = defer.Deferred()
        self.start('a b c')

        d2 = defer.Deferred()
        d2.addCallback( lambda _: self.assertTrue(True) )
        
        def onleader1(tpl):
            self.stop(tpl[1])
            self.dleader = d2
        
        self.dleader.addCallback( onleader1 )
        return d2

    def test_leader_resolve(self):
        self.dleader = defer.Deferred()
        self.start('a b c')

        d = defer.Deferred()

        def on_resolve(instance_num, value):
            self.assertEquals( value, 'foo' )
            d.callback(None)
            
        
        def on_leader(tpl):
            self.nodes[tpl[1]].onProposalResolution = on_resolve
            c = self.new_client(tpl[1])
            c.send( json.dumps( dict(type='propose_value', sequence_number=0, value='foo') ) )
            
        
        self.dleader.addCallback( on_leader )
        return d


    def test_non_leader_resolve(self):
        self.dleader = defer.Deferred()
        self.start('a b c')

        d = defer.Deferred()

        def on_resolve(instance_num, value):
            self.assertEquals( value, 'foo' )
            d.callback(None)
            
        
        def on_leader(tpl):
            s = set( self.nodes.keys() )
            s.remove(tpl[1])
            x = s.pop()
            self.nodes[x].onProposalResolution = on_resolve
            c = self.new_client(x)
            c.send( json.dumps( dict(type='propose_value', sequence_number=0, value='foo') ) )
            
        
        self.dleader.addCallback( on_leader )
        return d

        
