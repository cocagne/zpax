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



class HBTestNode(multi.MultiPaxosHeartbeatNode):

    def __init__(self, *args, **kwargs):
        super(HBTestNode,self).__init__(*args, **kwargs)

        self.dleader_acq = defer.Deferred()


    def on_leadership_acquired(self, *args):
        super(HBTestNode,self).on_leadership_acquired(*args)

        self.dleader_acq.callback(None)


    def on_leadership_lost(self, *args):
        self.dleader_acq = defer.Deferred()
        super(HBTestNode,self).on_leadership_lost(*args)



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
        
        self.A.shutdown()

        yield d
        




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
