'''
This module implements a simple, distributed Key-Value database that uses Paxos
for ensuring consistency between nodes. The goal of this module is to provide a
simple and correct implementation that is useful both as an example for using
zpax for distributed consensus as well as an actual, embedded database for real
applications. Due to the simplicity goal, the performance of this
implementation is not stellar but should be sufficient for lightweight usage.
'''

# Potential future extensions:
#
#   * Buffered key=value assignments. Writes to different keys guarantee not to
#     conflict.
#
#   * Multi-key commit: Check all key-sequence numbers in request match the current
#     db state. If so, pass though to paxos for decision. If not, send nack.


import os.path
import sqlite3
import json

from zpax import tzmq, multi

from twisted.internet import defer, task, reactor


_ZPAX_CONFIG_KEY = '__zpax_config__'

tables = dict()

# Deletions update the deltion key with the
# instance number
tables['kv'] = '''
key      text PRIMARY KEY,
value    text,
instance integer
'''


class SqliteDB (object):

    def __init__(self, fn):
        self._fn = fn
        create = not os.path.exists(fn)
        
        self._con = sqlite3.connect(fn)
        self._cur = self._con.cursor()

        if create:
            self.create_db()

            
    def create_db(self):
        cur = self._con.cursor()

        for k,v in tables.iteritems():
            cur.execute('create table {} ({})'.format(k,v))

        cur.execute('create index instance_index on kv (instance)')

        self._con.commit()
        cur.close()


    def get_value(self, key):
        r = self._cur.execute('SELECT value FROM kv WHERE key=?', (key,)).fetchone()
        if r:
            return r[0]

        
    def get_instance(self, key):
        r = self._cur.execute('SELECT instance FROM kv WHERE key=?', (key,)).fetchone()
        if r:
            return r[0]

        
    def update_key(self, key, value, instance_number):
        prevpn = self.get_instance(key)

        if prevpn is None:
            self._cur.execute('INSERT INTO kv VALUES (?, ?, ?)',
                              (key, value, instance_number))
            self._con.commit()
            
        elif instance_number > prevpn:
            self._cur.execute('UPDATE kv SET value=?, instance=? WHERE key=?',
                              (value, instance_number, key))
            self._con.commit()

            
    def get_last_instance(self):
        r = self._cur.execute('SELECT MAX(instance) FROM kv').fetchone()[0]

        return r if r is not None else -1

    
    def iter_updates(self, start_instance, end_instance=2**32):
        c = self._con.cursor()
        c.execute('SELECT key,value,instance FROM kv WHERE instance>? AND instance<?  ORDER BY instance',
                  (start_instance, end_instance))
        return c



class KeyValNode (multi.MultiPaxosHeartbeatNode):
    '''
    This class implements the Paxos logic for KeyValueDB. It extends the
    base class functionality in two primary ways. First, it monitors the
    heartbeat messages for currency and uses them as a trigger for
    initiating and completing the database synchronization
    process. Second, it disables participation in the Paxos algorithm while
    during the synchronization process. This prevents newly chosen values
    from entering the database in an out-of-order manner.
    '''

    def __init__(self, kvdb, net_node, channel_name, quorum_size):
        super(KeyValNode,self).__init__(net_node, quorum_size)
        self.kvdb = kvdb
        self.net  = net_node


    def receive_heartbeat(self, from_uid, kw):

        super(KeyValNode,self).receive_heartbeat(from_uid, kw)

        self.enabled = kw['instance'] == self.kvdb.last_instance + 1

        if not self.enabled:
            self.kvdb.catchup(kw['instance'])
        

    def on_resolution(self, proposer_obj, proposal_id, value):
        # This method is only called when our database is current
        assert self.instance == self.kvdb.last_instance + 1
        
        key, value = json.loads(value[1])

        self.kvdb.on_paxos_resolution( key, value, self.instance )

        super(KeyValNode,self).on_resolution(proposer_obj, proposal_id, value)
        



class KeyValueDB (object):
    '''
    This class implements a distributed key=value database that uses Paxos to
    coordinate database updates. Unlike the replacated state machine design
    typically discussed in Paxos literature, this implementation takes a
    simpler approach to ensure consistency. Each key/value update includes in
    the database the Multi-Paxos instance number used to set the value for that
    key.

    Nodes detect that their database is out of sync with their peers when it
    sees a heartbeat message for for a Multi-Paxos instance ahead of what it is
    expecting. When this occurs, the node suspends it's participation in the
    Paxos protocol and synchronizes it's database. This is accomplished by
    continually requesting key-value pairs with instance ids greater than what
    it has already received. These are requested in ascending order until the
    node recieves the key-value pair with an instance number that is 1 less
    than the current Multi-Paxos instance under negotiation. This indicates
    that the node has fully synchronized with it's peers and may rejoin the
    Paxos protocol.

    To support the addition and removal of nodes, the configuration for the
    paxos configuration is, itself, stored in the database. This
    automatically ensures that at least a quorum number of nodes always agree
    on what the current configuration is and, consequently, ensures that
    progress can always be made. Note, however, that if encryption is
    being used and the encryption key changes, some additional work will be
    required to enable the out-of-date nodes to catch up.

    With this implementation, queries to nodes may return data that is out of
    date. "Retrieve the most recent value" is not an operation that this
    implementation can reliably handle; it is imposible to reliably detect
    whether this node's data is consistent with it's peers at any given point
    in time. Successful handling of this operation requires either that the
    read operation flow through the Paxos algorighm itself (and in which case
    the value could be rendered out-of-date even before it is delivered to the
    client) or leadership-leases must be used. Leadership leases are relatively
    straight-forward to implement but are omitted here for the sake of
    simplicity.
    '''

    catchup_retry_delay = 2.0
    catchup_num_items   = 2
    
    def __init__(self, net_node, net_channel, quorum_size,
                 database_dir,
                 database_filename=None):

        if database_filename is None:
            database_filename = os.path.join(database_dir, 'db.sqlite')

        # By default, prevent arbitrary clients from proposing new
        # configuration values
        self.allow_config_proposals = False

        self.db            = SqliteDB( database_filename )
        self.last_instance = self.db.get_last_instance()
        self.catching_up   = False
        self.catchup_retry = None
        self.active_instance = None

        self.kv_node = KeyValNode(self, net_node, net_channel + '.paxos', quorum_size)
        self.net     = net_node

        self.net_channel = net_channel + '.kv'
        self.net.message_handler.append(self.net_channel, self)

        if self.initialized:
            self._load_configuration()


    def _load_configuration(self):

        self.last_instance = self.db.get_last_instance()

        cfg = json.loads( self.db.get_value(_ZPAX_CONFIG_KEY) )

        zpax_nodes = dict()
        all_nodes  = set()

        for n in cfg['nodes']:
            zpax_nodes[ n['uid'] ] = (n['pax_rtr_addr'], n['pax_pub_addr'])
            all_nodes.add( n['uid'] )
            
        quorum_size = len(cfg['nodes'])/2 + 1

        if self.kv_node.quorum_size != quorum_size:
            self.kv_node.change_quorum_size( quorum_size )

        if not self.net.node_uid in all_nodes:
            # We've been removed from the inner circle
            print 'This node has been removed from the Paxos group'
            self.shutdown()

        self.net_node.connect( zpax_nodes )


    @property
    def initialized(self):
        return self.db.get_value(_ZPAX_CONFIG_KEY) is not None

    
    def initialize(self, config_str):
        if self.initialized:
            raise Exception('Node already initialized')
        
        self.db.update_key(_ZPAX_CONFIG_KEY, config_str, -1)
        self._load_configuration()

                
    def shutdown(self):
        if self.catchup_retry and self.catchup_retry.active():
            self.catchup_retry.cancel()
        self.kv_node.shutdown()

            
    def on_paxos_resolution(self, key, value, instance_num):
        assert not self.catching_up
        
        self.db.update_key( key, value, instance_num )        
        self.last_instance = instance_num
        
        if key == _ZPAX_CONFIG_KEY:
            self._load_configuration()
        

    def catchup(self, active_instance):
        self.active_instance = active_instance
        
        if self.catching_up:
            return 

        self.catching_up = True

        self._catchup()
        

    def _catchup(self):
        if self.catching_up:
            self.catchup_retry = reactor.callLater(self.catchup_retry_delay,
                                                   self._catchup)

            if self.kv_node.leader_uid is not None:
                self.unicast( self.kv_node.leader_uid,
                              'catchup_request',
                              last_known_instance=self.last_instance )
        

    #--------------------------------------------------------------------------
    # Messaging
    #           
    def unicast(self, to_uid, message_type, **kwargs):
        self.net.unicast( to_uid, self.net_channel, message_type, kwargs )


    def receive_catchup_request(self, from_uid, msg):
        l = list()
        
        for tpl in self.db.iter_updates(msg['last_known_instance']):
            l.append( tpl )
            if len(l) == self.catchup_num_items:
                break

        self.unicast( from_uid, 'catchup_data',
                      from_instance = header['last_known_instance'],
                      key_val_instance_list = l )

    
    def receive_catchup_data(self, from_instance):
        if self.last_instance != from_instance:
            # This is a reply to an old request. Ignore it.
            return
            
        if self.catchup_retry and self.catchup_retry.active():
            self.catchup_retry.cancel()
            self.catchup_retry = None

        reload_config = False
        
        for key, val, instance_num in msg['key_val_instance_list']:
            self.db.update_key(key, val, instance_num)
            
            if key == _ZPAX_CONFIG_KEY:
                reload_config = True

        if reload_config:
            self._load_configuration()
                
        self.last_instance = self.db.get_last_instance()
        
        self.catching_up = self.active_instance != self.last_instance

        if self.catching_up:
            self._catchup()

    
    def receive_propose_value(self, from_uid, msg):
        try:
            if not self.allow_config_proposals and msg['key'] == _ZPAX_CONFIG_KEY:
                raise Exception('Access Denied')
            self.kv_node.proposeValue( msg['request_id'], [msg['key'], msg['value']] )
            self.unicast(from_uid, 'propose_reply', request_id=msg['request_id'])
        except node.ProposalFailed, e:
            self.unicast(from_uid, 'propose_reply', request_id=msg['request_id'], error=str(e))

            
    def receive_query_value(self, from_uid, msg):
        if not self.allow_config_proposals and msg['key'] == _ZPAX_CONFIG_KEY:
            self.unicast(from_uid, 'query_result', error='Access Denied')
        else:
            self.unicast(from_uid, 'query_result', value=self.db.get_value(msg['key']) )

