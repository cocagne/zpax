import os
import os.path
import sys
import tempfile
import shutil

pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )
sys.path.append( os.path.join(pd(pd(this_dir)), 'zpax') )
sys.path.append( os.path.join(pd(pd(this_dir)), 'paxos') )

from twisted.internet import reactor, defer
from twisted.trial import unittest

from zpax import durable

from zpax.network import test_node
from zpax.network.test_node import gatherResults, trace_messages

from zpax.commit import TransactionManager


class TransactionTester (unittest.TestCase):

    def setUp(self):
        test_node.setup()

        self.all_nodes = set(['A', 'B', 'C'])
        self.tms       = dict()               # Node ID => TransactionManager

        self.zpax_nodes = dict() # dict of node_id => (zmq_rtr_addr, zmq_pub_addr)
                                 # we'll fake it for the test_node.NetworkNode
        
        for nid in self.all_nodes:
            self.zpax_nodes[nid] = ('foo','foo')
            
            tm = TransactionManager( test_node.Channel('test_channel', test_node.NetworkNode(nid)),    #network_channel
                                     2,                              #quorum_size,
                                     self.all_nodes,                 #all_node_ids,
                                     2,                              #threshold,
                                     durable.MemoryOnlyStateStore()) #durable

            tm.timeout_duration = 5
            tm.get_current_time = lambda : 0
            
            self.tms[ nid ] = tm

            setattr(self, nid, tm)

        for nid in self.all_nodes:
            self.tms[nid].net.connect( self.zpax_nodes )

        self.auto_flush(True)

            
    def auto_flush(self, value):
        for nid in self.all_nodes:
            getattr(self, nid).durable.auto_flush = value
            

    @defer.inlineCallbacks
    def test_simple_commit(self):
        da = self.A.propose_result('tx1', 'commit')
        db = self.B.propose_result('tx1', 'commit')

        r = yield gatherResults([da, db])

        self.assertEquals(r, [('tx1','committed'), ('tx1','committed')])


    #@trace_messages
    @defer.inlineCallbacks
    def test_simple_all_commit(self):
        da = self.A.propose_result('tx1', 'commit')
        db = self.B.propose_result('tx1', 'commit')
        dc = self.C.propose_result('tx1', 'commit')

        r = yield gatherResults([da, db, dc])

        self.assertEquals(r, [('tx1','committed'), ('tx1','committed'), ('tx1','committed')])

        
    #@trace_messages
    @defer.inlineCallbacks
    def test_simple_abort(self):
        self.A.net.link_up = False
        da = self.A.propose_result('tx1', 'commit')
        self.A.net.link_up = True
        
        self.A.heartbeat(6, True)
        
        db = self.B.propose_result('tx1', 'commit')

        dc = self.C.propose_result('tx1', 'commit')

        r = yield gatherResults([da, db, dc])

        self.assertEquals(r, [('tx1','aborted'), ('tx1','aborted'), ('tx1','aborted')])

        
    #@trace_messages
    @defer.inlineCallbacks
    def test_complex_abort(self):
        
        da = self.A.propose_result('tx1', 'commit')

        self.assertEquals( self.A.get_transaction('tx1').num_committed, 1 )
        
        self.B.net.link_up = False
        self.C.net.link_up = False
        
        db = self.B.propose_result('tx1', 'commit')
        dc = self.C.propose_result('tx1', 'commit')
        
        self.B.net.link_up = True
        self.C.net.link_up = True
        
        self.A.heartbeat(6, True)
        
        r = yield gatherResults([da, db, dc])

        self.assertEquals(r, [('tx1','aborted'), ('tx1','aborted'), ('tx1','aborted')])

        

        
        
    
