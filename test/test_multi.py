import os
import os.path
import sys
import pickle

from twisted.internet import reactor, defer
from twisted.trial import unittest


pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )
sys.path.append( os.path.join(pd(pd(this_dir)), 'paxos') )


from zpax import multi

import testhelper


def delay(t):
    d = defer.Deferred()
    reactor.callLater(t, lambda : d.callback(None) )
    return d

    
all_nodes = 'A B C'.split()

try:
    defer.gatherResults( [defer.succeed(None),], consumeErrors=True )
    use_consume = True
except TypeError:
    use_consume = False

def gatherResults( l ):
    if use_consume:
        return defer.gatherResults(l, consumeErrors=True)
    else:
        return defer.gatherResults(l)


def trace( fn ):
    @defer.inlineCallbacks
    def wrapit(self, *args, **kwargs):
        testhelper.TRACE = True
        print ''
        print 'Trace:'
        yield fn(self, *args, **kwargs)
        testhelper.TRACE = False
        print ''
    return wrapit
    

class HBTestNode(multi.MultiPaxosHeartbeatNode):

    def __init__(self, *args, **kwargs):
        super(HBTestNode,self).__init__(*args, **kwargs)

        self.dleader_acq = defer.Deferred()
        self.dresolution = defer.Deferred()

    def __getstate__(self):
        d = super(HBTestNode,self).__getstate__()
        d.pop('dleader_acq')
        d.pop('dresolution')
        return d

    def __setstate__(self, d):
        self.__dict__ = d
        self.dleader_acq = defer.Deferred()
        self.dresolution = defer.Deferred()


    def on_leadership_acquired(self, *args):
        if testhelper.TRACE:
            print self.node_uid, 'Leadership Acquired'
        super(HBTestNode,self).on_leadership_acquired(*args)

        self.dleader_acq.callback(None)


    def on_leadership_lost(self, *args):
        if testhelper.TRACE:
            print self.node_uid, 'Leadership Lost'
        self.dleader_acq = defer.Deferred()
        super(HBTestNode,self).on_leadership_lost(*args)

    def on_resolution(self, proposer_obj, proposal_id, value):
        if testhelper.TRACE:
            print self.node_uid, 'Resolution:', proposal_id, value
        #print 'RESOLUTION: ', proposal_id, value
        d = self.dresolution
        self.dresolution = defer.Deferred()
        super(HBTestNode,self).on_resolution(proposer_obj, proposal_id, value)
        d.callback((proposal_id, value))


class MultiTesterBase(object):

    @defer.inlineCallbacks
    def setUp(self):
        
        self.nodes = dict()
        
        yield self._setup()

        for name, mn in self.nodes.iteritems():
            setattr(self, name, mn)

    
    def tearDown(self):
        for n in self.nodes.itervalues():
            n.shutdown()

        return self._teardown()

    def _setup(self):
        pass

    def _teardown(self):
        pass


    #    @trace
    @defer.inlineCallbacks
    def test_initial_leadership_acquisition(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        #@trace
    @defer.inlineCallbacks
    def test_leadership_recovery_on_failure(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        d = defer.Deferred()

        self.B.dleader_acq.addCallback( d.callback )
        self.C.dleader_acq.addCallback( d.callback )
        
        self.A.net.link_up = False

        yield d

        self.assertTrue(self.A.pax.leader)
        
        self.A.net.link_up = True

        yield delay(0.06)

        self.assertTrue(not self.A.pax.leader)


    # Durability tests:
    #  use pickle directly to load/restore from strings
    #  test in various circumstances

        
    @defer.inlineCallbacks
    def test_leader_resolution(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )
        
        self.A.set_proposal( 'reqid', 'foobar' )

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar'))] )

    @defer.inlineCallbacks
    def test_accept_nack(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )

        self.A.net.link_up = False
        
        self.A.set_proposal( 'reqid', 'foobar' )

        self.assertEquals( self.A.pax.next_proposal_number, 2 )
        self.assertTrue( self.A.pax.leader )

        self.A.receive_accept_nack( 'B', dict(proposal_id=self.A.pax.proposal_id,
                                              promised_id=(2, 'B')))
        self.A.receive_accept_nack( 'C', dict(proposal_id=self.A.pax.proposal_id,
                                              promised_id=(2, 'B')))

        self.assertEquals( self.A.pax.next_proposal_number, 3 )
        self.assertTrue( not self.A.pax.leader )

        
    @defer.inlineCallbacks
    def test_non_leader_resolution(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )
        
        
        self.B.set_proposal( 'reqid', 'foobar' )

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar'))] )


        #@trace
    @defer.inlineCallbacks
    def test_proposal_advocate_retry(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )

        self.B.net.link_up = False
        
        self.B.advocate.retry_delay = 0.01
        
        self.B.set_proposal( 'reqid', 'foobar' )

        yield delay( 0.03 )

        self.assertTrue( not self.A.dresolution.called )

        self.B.net.link_up = True

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar'))] )


        #@trace
    @defer.inlineCallbacks
    def test_proposal_advocate_retry_with_crash_recovery(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        self.B.net.link_up = False
        
        self.B.advocate.retry_delay = 0.01
        
        self.B.set_proposal( 'reqid', 'foobar' )

        yield delay( 0.03 )
        
        s = self.save_node( 'B' )

        self.recover_node( s, False )

        self.assertTrue( not self.A.dresolution.called )

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )

        self.B.net.link_up = True

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar'))] )

        
    @defer.inlineCallbacks
    def test_resolution_with_leadership_failure_and_isolated_node(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        d = gatherResults( [self.B.dresolution,
                            self.C.dresolution] )
                           
        self.A.net.link_up = False
        self.B.net.link_up = False
        
        self.B.advocate.retry_delay = 0.01
        
        self.B.set_proposal( 'reqid', 'foobar' )

        yield delay( 0.05 )

        self.assertTrue( not self.A.dresolution.called )

        self.B.net.link_up = True

        r = yield d

        self.assertTrue(r in ( [((1, 'B'), ('reqid', 'foobar')),
                                ((1, 'B'), ('reqid', 'foobar'))],
                               [((1, 'C'), ('reqid', 'foobar')),
                                ((1, 'C'), ('reqid', 'foobar'))]) )


    @defer.inlineCallbacks
    def test_multiple_instances(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        self.assertEquals( self.A.instance, 1 )

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )
                            
        
        self.A.set_proposal( 'reqid', 'foobar' )

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar'))] )

        self.assertEquals( self.A.instance, 2 )

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )
                            
        
        self.A.set_proposal( 'reqid', 'baz' )

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'baz')),
                              ((1, 'A'), ('reqid', 'baz')),
                              ((1, 'A'), ('reqid', 'baz'))] )

        self.assertEquals( self.A.instance, 3 )


    @defer.inlineCallbacks
    def test_multiple_instances_with_crash_recovery(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        self.assertEquals( self.A.instance, 1 )

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )
                            
        
        self.A.set_proposal( 'reqid', 'foobar' )

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar'))] )


        self.recover_node( self.save_node('A'), False )
        self.recover_node( self.save_node('B'), False )
        self.recover_node( self.save_node('C'), False )

        self.A.net.link_up = True
        self.B.net.link_up = True
        self.C.net.link_up = True
        
        self.assertEquals( self.A.instance, 2 )

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )
                            
        
        self.A.set_proposal( 'reqid', 'baz' )

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'baz')),
                              ((1, 'A'), ('reqid', 'baz')),
                              ((1, 'A'), ('reqid', 'baz'))] )

        self.assertEquals( self.A.instance, 3 )


    @defer.inlineCallbacks
    def test_behind_in_sequence(self):
        self.A.pax.acquire_leadership()
        yield self.A.dleader_acq

        self.assertEquals( self.A.instance, 1 )

        self.B.net.link_up = False

        d = gatherResults( [self.A.dresolution,
                            self.C.dresolution] )
                            
        
        self.A.set_proposal( 'reqid', 'foobar' )

        r = yield d
        

        self.assertEquals(r, [((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar'))] )

        self.assertEquals( self.A.instance, 2 )
        self.assertEquals( self.B.instance, 1 )
        
        self.B.net.link_up = True

        yield delay(0.05)

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )
                            
        
        self.A.set_proposal( 'reqid', 'baz' )
        
        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'baz')),
                              ((1, 'A'), ('reqid', 'baz')),
                              ((1, 'A'), ('reqid', 'baz'))] )

        self.assertEquals( self.A.instance, 3 )
        self.assertEquals( self.B.instance, 3 )
        




class HeartbeatTester(MultiTesterBase, unittest.TestCase):

    def _setup(self):

        testhelper.setup()

        self.zpax_nodes = dict()
        
        for uid in all_nodes:

            self.nodes[uid] =  HBTestNode( testhelper.NetworkNode(uid),
                                           2,
                                           hb_period       = 0.01,
                                           liveness_window = 0.03 )
            self.zpax_nodes[uid] = ('foo','foo')

        for uid in all_nodes:
            self.nodes[uid].net.connect( self.zpax_nodes )
            
        self.nodes['A'].pax._tlast_hb   = 0
        self.nodes['A'].pax._tlast_prep = 0

        
    def save_node(self, node_uid):
        pkl = pickle.dumps(self.nodes[node_uid], pickle.HIGHEST_PROTOCOL)
        self.nodes[node_uid].shutdown()
        return pkl

    
    def recover_node(self, pkl_string, link_up = True):
        n  = pickle.loads(pkl_string)

        nn = testhelper.NetworkNode(n.node_uid)

        nn.connect( self.zpax_nodes )

        nn.link_up = link_up
        
        n.recover( nn )

        self.nodes[ n.node_uid ] = n
        setattr(self, n.node_uid, n)
