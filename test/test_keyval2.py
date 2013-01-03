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
sys.path.append( os.path.join(pd(pd(this_dir)), 'paxos') )

from zpax import keyval2, multi, tzmq


def delay(t):
    d = defer.Deferred()
    reactor.callLater(t, lambda : d.callback(None) )
    return d


class TestHBP (node.BasicHeartbeatProposer):
    hb_period       = 0.05
    liveness_window = 0.15

    
class TestKVN (keyval.KeyValNode):
    hb_proposer_klass = TestHBP

    
class TestKV (keyval.KeyValueDB):
    _node_klass = TestKVN


class TestReq(tzmq.ZmqReqSocket):
    d = None
    last_val = None
    
    def propose(self, key, value):
        self.linger=0
        return self.jsend(type='propose_value', key=key, value=value)

    def query(self, key):
        return self.jsend(type='query_value', key=key)
        
    def jsend(self, **kwargs):
        assert self.d is None
        self.d = defer.Deferred()
        self.send( json.dumps(kwargs) )
        return self.d

    def messageReceived(self, parts):
        #print 'MESSAGE RECEIVED', parts
        
        d = self.d
        self.d = None

        if len(parts) != 1:
            d.errback(Exception('Did not receive 1 message part in reply'))
        else:
            d.callback( json.loads(parts[0]) )




class KeyValueDBTester(unittest.TestCase):

    def setUp(self):
        tmpfs_dir = '/dev/shm' if os.path.exists('/dev/shm') else None
        
        self.tdir        = tempfile.mkdtemp(dir=tmpfs_dir)
        self.nodes       = dict()
        self.leader      = None
        self.dleader     = defer.Deferred()
        self.dlost       = None
        self.clients     = list()
        self.all_nodes   = 'a b c'.split()

        
    @property
    def json_config(self):
        nodes = list()
        
        for uid in self.all_nodes:
            pax_rep = 'ipc:///tmp/ts_{}_pax_rep'.format(uid)
            pax_pub = 'ipc:///tmp/ts_{}_pax_pub'.format(uid)
            kv_rep  = 'ipc:///tmp/ts_{}_kv_rep'.format(uid)
            nodes.append( dict(uid          = uid,
                               pax_pub_addr = pax_pub,
                               pax_rep_addr = pax_rep,
                               kv_rep_addr  = kv_rep) )
            
        return json.dumps( dict( nodes = nodes ) )
            
        
    def tearDown(self):
        for c in self.clients:
            c.close()
            
        for n in self.all_nodes:
            self.stop(n)

        shutil.rmtree(self.tdir)
        
        # In ZeroMQ 2.1.11 there is a race condition for socket deletion
        # and recreation that can render sockets unusable. We insert
        # a short delay here to prevent the condition from occuring.
        return delay(0.05)


    def new_client(self, node_name=None):
        zreq = TestReq()
        self.clients.append(zreq)
        zreq.connect('ipc:///tmp/ts_{}_kv_rep'.format(node_name))
        return zreq
            

    def start(self,  node_names, chatty=False, hmac_key=None, value_key=None, caughtup=None):

        def gen_cb(x, func):
            def cb():
                func(x)
            return cb

        zpax_nodes = dict()
        
        for node_name in node_names.split():
            if not node_name in self.all_nodes or node_name in self.nodes:
                continue
                        
            n = TestKV(node_name, self.tdir, os.path.join(self.tdir, node_name + '.sqlite'))

            n.allow_config_proposals = True

            if hmac_key:
                n.kv_node.hmac_key = hmac_key

            if value_key:
                n.kv_node.value_key = value_key

            n.kv_node.onLeadershipAcquired = gen_cb(node_name, self._on_leader_acq)
            n.kv_node.onLeadershipLost     = gen_cb(node_name, self._on_leader_lost)

            n.name = node_name

            if caughtup:
                n.onCaughtUp = caughtup
            
            if chatty:
                n.kv_node.chatty = True
                
            self.nodes[node_name] = n

            if not n.isInitialized():
                n.initialize( self.json_config )


        
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
    def set_key(self, client, key, value):
        
        v = None
        while v != value:
            yield client.propose(key,value)
            yield delay(0.1)
            r = yield client.query(key)
            v = r['value']
            #print 'set_key', key, value, v, v != value



    def get_key(self, client, key):
        d = client.query(key)
        d.addCallback( lambda r : r['value'] )
        return d
    

    @defer.inlineCallbacks
    def test_initial_leader(self):
        self.start('a b')
        yield self.dleader


    @defer.inlineCallbacks
    def test_set_key_val_pair(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client('a')

        yield self.dleader
        
        yield c.propose('foo', 'bar')

        keyval = None
        while keyval != 'bar':
            yield delay(0.05)
            r = yield c.query('foo')
            keyval = r['value']


    @defer.inlineCallbacks
    def test_set_keys(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client('a')

        yield self.dleader

        yield self.set_key(c, 'foo0', 'bar')
        yield self.set_key(c, 'foo1', 'bar')
        yield self.set_key(c, 'foo2', 'bar')
        yield self.set_key(c, 'foo3', 'bar')
        yield self.set_key(c, 'foo4', 'bar')
        yield self.set_key(c, 'foo5', 'bar')
        yield self.set_key(c, 'foo6', 'bar')
        yield self.set_key(c, 'foo7', 'bar')
        yield self.set_key(c, 'foo8', 'bar')
        yield self.set_key(c, 'foo9', 'bar')

        yield self.set_key(c, 'foo0', 'baz')
        yield self.set_key(c, 'foo1', 'baz')
        yield self.set_key(c, 'foo2', 'baz')
        yield self.set_key(c, 'foo3', 'baz')
        yield self.set_key(c, 'foo4', 'baz')
        yield self.set_key(c, 'foo5', 'baz')
        yield self.set_key(c, 'foo6', 'baz')
        yield self.set_key(c, 'foo7', 'baz')
        yield self.set_key(c, 'foo8', 'baz')
        yield self.set_key(c, 'foo9', 'baz')

    @defer.inlineCallbacks
    def test_shutdown_and_restart(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client('a')

        yield self.dleader
        
        yield self.set_key(c, 'foo0', 'bar')
        yield self.set_key(c, 'foo1', 'bar')

        self.stop('a b')

        yield delay(0.05)

        self.dleader = defer.Deferred()

        self.start('a b')

        yield self.dleader

        v = yield self.get_key(c, 'foo0')

        self.assertEquals(v, 'bar')

        yield self.set_key(c, 'foo1', 'baz')


    @defer.inlineCallbacks
    def test_shutdown_and_restart_with_outstanding_proposal(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client('a')

        yield self.dleader
        
        yield self.set_key(c, 'foo0', 'bar')

        self.stop('b')

        yield c.propose('foo1', 'bar')

        self.assertTrue( self.nodes['a'].kv_node.mpax.node.proposer.value is not None )

        self.stop('a')

        yield delay(0.05)

        self.dleader = defer.Deferred()

        self.start('a b')

        yield self.dleader

        v = None
        while v != 'bar':
            v = yield self.get_key(c, 'foo1')
            yield delay(0.01)

        self.assertEquals(v, 'bar')

            

    @defer.inlineCallbacks
    def xtest_zmq_req_down_rep_node(self):
        #self.all_nodes.append('d')
        self.start('a  b')

        d = defer.Deferred()
        c = self.new_client('a')

        yield self.dleader

        yield self.set_key(c, 'foo', 'bar')
        
        # Add a node to config
        self.all_nodes.append('d')
        
        yield self.set_key(c, keyval._ZPAX_CONFIG_KEY, self.json_config)
        
        # Quorum is now 3. No changes can be made until 3 functioning nodes
        # are up. Start the newly added node to reach a total of three then
        # set a key
        dcaughtup = defer.Deferred()
        self.start('d', caughtup = lambda : dcaughtup.callback(None))
        self.nodes['d'].chatty = True

        yield dcaughtup

        print 'Trying to set key with quorum 3'
        yield self.set_key(c, 'test_key', 'foo')

        print 'Done!!!'

        
    @defer.inlineCallbacks
    def test_dynamic_add_node(self, chatty=False):

        self.start('a c', chatty=chatty)

        d = defer.Deferred()
        c = self.new_client('a')

        yield self.dleader

        yield self.set_key(c, 'foo', 'bar')
        
        # Add a node to config
        self.all_nodes.append('d')

        #print '*'*30
        
        yield self.set_key(c, keyval._ZPAX_CONFIG_KEY, self.json_config)

        # Quorum is now 3. No changes can be made until 3 functioning nodes
        # are up. Start the newly added node to reach a total of three then
        # set a key

        #print '*'*30
        
        dcaughtup = defer.Deferred()

        self.start('d', caughtup = lambda : dcaughtup.callback(None))

        yield dcaughtup

        #yield delay(1)

        #print '*'*30

        # Trying to set key with quorum 3
        yield self.set_key(c, 'test_key', 'foo')
        yield self.set_key(c, 'test_key2', 'foo')
        defer.returnValue(c)
        

    @defer.inlineCallbacks
    def test_dynamic_remove_node(self):
        c = yield self.test_dynamic_add_node()

        #yield self.set_key(c, 'test_key2', 'foo')
        #print 'Node added', self.json_config
        self.all_nodes.remove('c')
        #print '*********'
        #print self.json_config

        #print  'Setting removed config'

        yield self.set_key(c, 'test_key3', 'foo')
        
        yield self.set_key(c, keyval._ZPAX_CONFIG_KEY, self.json_config)

        self.stop('c')

        #print 'Trying with quorum 2'
        # Trying to set key with quorum 2
        yield self.set_key(c, 'test_remove', 'foo')

        #print 'REMOVE COMPLETE'
        

    @defer.inlineCallbacks
    def test_node_recovery(self):
        self.start('a b')

        d = defer.Deferred()
        c = self.new_client('a')

        yield self.dleader

        yield self.set_key(c, 'foo', 'bar')
        yield self.set_key(c, 'baz', 'bish')
        yield self.set_key(c, 'william', 'wallace')

        dcaughtup = defer.Deferred()
        
        self.start('c', caughtup = lambda : dcaughtup.callback(None))

        yield dcaughtup

        c2 = self.new_client('c')

        r = yield c2.query('william')
        self.assertEquals(r['value'], 'wallace')
            





class SqliteDBTest(unittest.TestCase):

    def setUp(self):
        self.db = keyval.SqliteDB(':memory:')

    def test_update_missing_value(self):
        self.assertTrue(self.db.get_value('foo') is None)
        self.db.update_key('foo', 'bar', 5)
        self.assertEquals(self.db.get_value('foo'), 'bar')

    def test_update_new_value(self):
        self.assertTrue(self.db.get_value('foo') is None)
        self.db.update_key('foo', 'bar', 5)
        self.assertEquals(self.db.get_value('foo'), 'bar')
        self.db.update_key('foo', 'bish', 6)
        self.assertEquals(self.db.get_value('foo'), 'bish')

    def test_update_ignore_previous_resolution(self):
        self.assertTrue(self.db.get_value('foo') is None)
        self.db.update_key('foo', 'bar', 5)
        self.assertEquals(self.db.get_value('foo'), 'bar')
        self.db.update_key('foo', 'baz', 4)
        self.assertEquals(self.db.get_value('foo'), 'bar')

    def test_iter_updates_empty(self):
        l = [ x for x in self.db.iter_updates(100,200) ]
        self.assertEquals(l, [])

    def test_iter_updates_middle(self):
        for x in range(0,10):
            self.db.update_key(str(x), str(x), x)
        l = [ x for x in self.db.iter_updates(1,5) ]
        self.assertEquals(l, [(str(x),str(x),x) for x in range(2,5)])

    def test_iter_updates_ends(self):
        for x in range(0,10):
            self.db.update_key(str(x), str(x), x)
        l = [ x for x in self.db.iter_updates(0,10) ]
        self.assertEquals(l, [(str(x),str(x),x) for x in range(1,10)])

    def test_iter_updates_random_shuffle(self):
        rng = range(0,100)
        random.shuffle(rng)
        for x in rng:
            self.db.update_key(str(x), str(x), x)
        l = [ x for x in self.db.iter_updates(0,100) ]
        self.assertEquals(l, [(str(x),str(x),x) for x in range(1,100)])
        
