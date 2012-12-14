import os
import os.path
import sys

from twisted.internet import reactor, defer
from twisted.trial import unittest


pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )


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

        d = self.dleader_acq
        
        self.dleader_acq = defer.Deferred()
        
        d.callback(None)


class MultiTesterBase(object):

    def setUp(self):
        
        self.nodes = dict()
        
        return self._setup()

    
    def tearDown(self):
        for n in self.nodes.itervalues():
            n.shutdown()

        return self._teardown()


class HeartbeatTester(MultiTesterBase, unittest.TestCase):

    def _setup(self):
        for uid in all_nodes.split():

            self.nodes[uid] =  HBTestNode( testhelper.NetworkNode(uid),
                                           2,
                                           hb_period       = 0.01,
                                           liveness_window = 0.03 )
            
            self.nodes['A'].pax.acquire_leadership()
