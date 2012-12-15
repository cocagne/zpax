import os
import os.path
import sys

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
    

class HBTestNode(multi.MultiPaxosHeartbeatNode):

    def __init__(self, *args, **kwargs):
        super(HBTestNode,self).__init__(*args, **kwargs)

        self.dleader_acq = defer.Deferred()
        self.dresolution = defer.Deferred()


    def on_leadership_acquired(self, *args):
        super(HBTestNode,self).on_leadership_acquired(*args)

        self.dleader_acq.callback(None)


    def on_leadership_lost(self, *args):
        self.dleader_acq = defer.Deferred()
        super(HBTestNode,self).on_leadership_lost(*args)

    def on_resolution(self, proposer_obj, proposal_id, value):
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


    @defer.inlineCallbacks
    def test_initial_leadership_acquisition(self):
        yield self.A.dleader_acq


    @defer.inlineCallbacks
    def test_leadership_recovery_on_failure(self):
        yield self.A.dleader_acq

        d = defer.Deferred()

        self.B.dleader_acq.addCallback( d.callback )
        self.C.dleader_acq.addCallback( d.callback )
        
        self.A.net.link_up = False

        yield d

        
    @defer.inlineCallbacks
    def test_leader_resolution(self):
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
    def test_non_leader_resolution(self):
        yield self.A.dleader_acq

        d = gatherResults( [self.A.dresolution,
                            self.B.dresolution,
                            self.C.dresolution] )
        
        
        self.B.set_proposal( 'reqid', 'foobar' )

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar')),
                              ((1, 'A'), ('reqid', 'foobar'))] )


    @defer.inlineCallbacks
    def test_proposal_advocate_retry(self):
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

        
    @defer.inlineCallbacks
    def test_resolution_with_leadership_failure_and_isolated_node(self):
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

        self.assertEquals(r, [((1, 'B'), ('reqid', 'foobar')),
                              ((1, 'B'), ('reqid', 'foobar'))] )


    @defer.inlineCallbacks
    def test_multiple_instances(self):
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
    def test_behind_in_sequence(self):
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

        d = gatherResults( [self.A.dresolution,
                            self.C.dresolution] )

        self.B.net.link_up = True


        yield delay(0.05)
                            
        
        self.A.set_proposal( 'reqid', 'baz' )

        r = yield d

        self.assertEquals(r, [((1, 'A'), ('reqid', 'baz')),
                              ((1, 'A'), ('reqid', 'baz'))] )

        self.assertEquals( self.A.instance, 3 )
        




class HeartbeatTester(MultiTesterBase, unittest.TestCase):

    def _setup(self):

        testhelper.setup()

        zpax_nodes = dict()
        
        for uid in all_nodes:

            self.nodes[uid] =  HBTestNode( testhelper.NetworkNode(uid),
                                           2,
                                           hb_period       = 0.01,
                                           liveness_window = 0.03 )
            zpax_nodes[uid] = ('foo','foo')

        for uid in all_nodes:
            self.nodes[uid].net.connect( zpax_nodes, False )
            
        self.nodes['A'].pax._tlast = 0
        self.nodes['A'].pax.acquire_leadership()
