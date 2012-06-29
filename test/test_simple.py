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

def delay(t):
    d = defer.Deferred()
    reactor.callLater(t, lambda : d.callback(None) )
    return d


class TestHBP (node.BasicHeartbeatProposer):
    hb_period       = 0.1
    liveness_window = 0.3

    
class TestKV (simple.SimpleNode):

    hb_proposer_klass = TestHBP

    #chatty=True

    drop_packets = False

    name = None

    def checkSequence(self, header):
        if self.drop_packets:
            return False
        
        return super(TestKV,self).checkSequence(header)
        



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
        self.seq         = 0
        

        
    def tearDown(self):
        for c in self.clients:
            c.close()
            
        for n in all_nodes:
            self.stop(n)

        # In ZeroMQ 2.1.11 there is a race condition for socket deletion
        # and recreation that can render sockets unusable. We insert
        # a short delay here to prevent the condition from occuring.
        return delay(0.05)
            

    def start(self,  node_names, chatty=False, hmac_key=None, value_key=None):

        def gen_cb(x, func):
            def cb():
                func(x)
            return cb

        zpax_nodes = dict()

        for node_name in all_nodes:
            t = ('ipc:///tmp/ts_{}_rep'.format(node_name),
                 'ipc:///tmp/ts_{}_pub'.format(node_name))
            
            #zpax_nodes[ 'ipc:///tmp/ts_{}_rep'.format(node_name) ] = t
            zpax_nodes[ node_name ] = t

            
        for node_name in node_names.split():
            if not node_name in all_nodes:
                continue
            #n = TestKV('ipc:///tmp/ts_{}_rep'.format(node_name),
            #           'ipc:///tmp/ts_{}_rtr'.format(node_name))
            
            n = TestKV(node_name,'ipc:///tmp/ts_{}_rtr'.format(node_name))

            if hmac_key:
                n.hmac_key = hmac_key

            if value_key:
                n.value_key = value_key

            n.initialize(self.quorum_size)
            n.connect( zpax_nodes )
            
            n.onLeadershipAcquired = gen_cb(node_name, self._on_leader_acq)
            n.onLeadershipLost     = gen_cb(node_name, self._on_leader_lost)

            n.name = node_name
            if chatty:
                n.chatty = True
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


    def test_on_leader_change(self):
        self.dleader = defer.Deferred()
        self.start('a b')

        d = defer.Deferred()

        def cb(x,y):
            if not d.called:
                d.callback(None)

        for n in self.nodes.itervalues():
            n.onLeadershipChanged = cb
        
        def onleader1(tpl):
            self.stop(tpl[1])
            self.start('c')
        self.dleader.addCallback( onleader1 )

        d.addCallback( lambda _: self.assertTrue(True) )
        return d

    
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
            self.seq += 1
            
        def on_leader(tpl):
            self.nodes[tpl[1]].onProposalResolution = on_resolve
            #self.nodes[tpl[1]].chatty = True
            c = self.new_client(tpl[1])
            c.send( json.dumps( dict(type='propose_value', sequence_number=self.seq, value='foo') ) )
        
        self.dleader.addCallback( on_leader )
        return d


    def test_non_leader_resolve(self):
        self.dleader = defer.Deferred()
        self.start('a b c')

        d = defer.Deferred()

        def on_resolve(instance_num, value):
            self.assertEquals( value, 'foo' )
            d.callback(None)
            self.seq += 1
        
        def on_leader(tpl):
            s = set( self.nodes.keys() )
            s.remove(tpl[1])
            x = s.pop()
            self.nodes[x].onProposalResolution = on_resolve
            c = self.new_client(x)
            c.send( json.dumps( dict(type='propose_value', sequence_number=self.seq, value='foo') ) )
        
        self.dleader.addCallback( on_leader )
        return d

    def _start_distinct(self, chatty=False):
        self.dleader = defer.Deferred()
        
        self.start('a b c', chatty)

        def on_leader(tpl):
            leader = self.nodes[tpl[1]]
            s = set( self.nodes.keys() )
            s.remove(tpl[1])
            return leader, [ self.nodes[k] for k in s ]
        
        self.dleader.addCallback( on_leader )
        return self.dleader

    
    def _resolve(self, leader):
        #print 'RBEGIN ', self.seq
        d = defer.Deferred()

        def on_resolve(instance_num, value):
            #print 'RESOLVED ', self.seq
            self.seq += 1
            d.callback(None)

        leader.onProposalResolution = on_resolve
        x = self.new_client(leader.name)
        x.send( json.dumps( dict(type='propose_value',
                                 sequence_number=self.seq,
                                 value='foo') ) )
        return d
        

    @defer.inlineCallbacks
    def test_behind_in_sequence(self):
        d = defer.Deferred()
        
        leader, others = yield self._start_distinct(False)
        #print 'Leader ', leader.name
        #leader.chatty = True

        n = others[0]
        
        #print '*** Disabling:', self.seq, n.name
        n.drop_packets       = True
        n.onBehindInSequence = lambda : d.callback(None)

        def cb():
            #print '******** BEHIND IN SEQUENCE ***********'
            d.callback(None)

        n.onBehindInSequence = cb

        yield self._resolve(leader)

        # Flush pending messages
        ddelay = defer.Deferred()
        reactor.callLater(0.01, lambda : ddelay.callback(None))
        yield ddelay

        #print '***2***', self.seq, n.sequence_number
        n.drop_packets = False

        self._resolve(leader)
        
        yield d


    def test_hmac(self):
        self.dleader = defer.Deferred()
        
        self.start('a b c', hmac_key='foobar')

        d = defer.Deferred()

        def on_resolve(instance_num, value):
            self.assertEquals( value, 'foo' )
            d.callback(None)
            self.seq += 1
            
        def on_leader(tpl):
            self.nodes[tpl[1]].onProposalResolution = on_resolve
            #self.nodes[tpl[1]].chatty = True
            c = self.new_client(tpl[1])
            c.send( json.dumps( dict(type='propose_value', sequence_number=self.seq, value='foo') ) )
        
        self.dleader.addCallback( on_leader )
        return d

    
    def test_basic_encryption(self):
        key = '0123456789ABCDEF'
        val = '{foo=5}'
        cip = node.encrypt_value( key, val )
        dec = node.decrypt_value( key, cip )
        self.assertEquals(val, dec)


    def test_encrypted_value(self):
        self.dleader = defer.Deferred()
        
        self.start('a b c', value_key='0123456789ABCDEF')

        d = defer.Deferred()

        def on_resolve(instance_num, value):
            self.assertEquals( value, 'foo' )
            d.callback(None)
            self.seq += 1
            
        def on_leader(tpl):
            self.nodes[tpl[1]].onProposalResolution = on_resolve
            #self.nodes[tpl[1]].chatty = True
            c = self.new_client(tpl[1])
            c.send( json.dumps( dict(type='propose_value', sequence_number=self.seq, value='foo') ) )
        
        self.dleader.addCallback( on_leader )
        return d


    def test_hmac_and_encrypted_value(self):
        self.dleader = defer.Deferred()
        
        self.start('a b c', hmac_key='foobar', value_key='0123456789ABCDEF')

        d = defer.Deferred()

        def on_resolve(instance_num, value):
            self.assertEquals( value, 'foo' )
            d.callback(None)
            self.seq += 1
            
        def on_leader(tpl):
            self.nodes[tpl[1]].onProposalResolution = on_resolve
            #self.nodes[tpl[1]].chatty = True
            c = self.new_client(tpl[1])
            c.send( json.dumps( dict(type='propose_value', sequence_number=self.seq, value='foo') ) )
        
        self.dleader.addCallback( on_leader )
        return d
        
        
