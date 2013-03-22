import os
import os.path
import sys
import json

import random

pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )
sys.path.append( os.path.join(pd(pd(this_dir)), 'paxos') )


from zpax.network import zed

from twisted.internet import reactor, defer
from twisted.trial import unittest


def delay(tsec):
    d = defer.Deferred()
    reactor.callLater(tsec, lambda : d.callback(None))
    return d

        
class PS (object):

    def __init__(self, name, pub_addr, sub_addrs):
        self.name = name
        self.pub  = zed.ZmqPubSocket()
        self.sub  = zed.ZmqSubSocket()

        self.pub.bind( pub_addr )
        for a in sub_addrs:
            self.sub.connect(a)

        def on_recv( parts ):
            self.jrecv( [ json.loads(p) for p in parts[1:] ] )

        self.sub.subscribe       = 'test_zmq'
        self.sub.messageReceived = on_recv

    def close(self):
        self.pub.close()
        self.sub.close()

    def jsend(self, **kwargs):
        kwargs.update( dict(name=self.name) )
        self.pub.send( ['test_zmq', json.dumps(kwargs) ] )

    def jrecv(self, jparts):
        pass

    

    
class SimpleTest(unittest.TestCase):

    def setUp(self):
        self.sockets = dict()
        
    def tearDown(self):
        for s in self.sockets.itervalues():
            s.close()

        return delay(0.05)
            

    def start(self, snames):

        l    = list()
        pubs = [ 'ipc:///tmp/tzmq_{0}_pub'.format(n) for n in snames.split() ]
        
        for sname in snames.split():
            s = PS(sname, 'ipc:///tmp/tzmq_{0}_pub'.format(sname), pubs)
            self.sockets[sname] = s
            l.append(s)

        return l
    
    def stop(self, sock_name):
        self.socket[sock_name].shutdown()
        del self.nodes[node_name]

    def test_connectivity(self):
        a, b, c = self.start('a b c')

        all_set = set('a b c'.split())

        def setup(s):
            s.d = defer.Deferred()
            s.r = set()
            
            def jrecv(parts):
                #print 'Recv', s.name, parts[0]['name'], '    ', s.r, s.sub.fd
                s.r.add( parts[0]['name'] )
                if s.r == all_set:
                    #print 'DONE: ', s.name
                    if not s.d.called:
                        s.d.callback(None)

            s.jrecv = jrecv

            def send():
                #print 'Send', s.name
                s.jsend(foo='bar')
                s.t = reactor.callLater(0.5, send)

            send()

        for x in (a, b, c):
            setup(x)

            
        d = defer.DeferredList([a.d, b.d, c.d])

        def done(_):
            for s in self.sockets.itervalues():
                if s.t.active():
                    s.t.cancel()
                    
        d.addCallback(done)
        
        return d


