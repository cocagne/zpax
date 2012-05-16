import os
import os.path
import sys

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
    
all_nodes = 'a b c'.split()
    
class SimpleTest(unittest.TestCase):

    def setUp(self):
        self.nodes       = dict()
        self.quorum_size = 2
        self.leader      = None
        self.dleader     = None
        self.dlost       = None
        

        
    def tearDown(self):
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
            n = TestKV( 'ipc:///tmp/ts_{}_pub'.format(node_name),
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


    def test_00_initial_leader(self):
        self.dleader = defer.Deferred()
        self.start('a b')
        self.dleader.addCallback( lambda _: self.assertTrue(True) )

        return self.dleader


    def test_01_leader_change(self):
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

        
