import os.path
import sqlite3
import json

from zpax import tzmq, node
from paxos import multi, basic

from twisted.internet import defer, task, reactor


_ZPAX_CONFIG_KEY = '__zpax_config__'


tables = dict()

tables['kv'] = '''
key      text PRIMARY KEY,
value    text,
resolution integer
'''

class MissingConfiguration (Exception):
    pass

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

        cur.execute('create index resolution_index on kv (resolution)')

        self._con.commit()
        cur.close()


    def get_value(self, key):
        r = self._cur.execute('SELECT value FROM kv WHERE key=?', (key,)).fetchone()
        if r:
            return r[0]

        
    def get_resolution(self, key):
        r = self._cur.execute('SELECT resolution FROM kv WHERE key=?', (key,)).fetchone()
        if r:
            return r[0]

        
    def update_key(self, key, value, resolution_number):
        prevpn = self.get_resolution(key)
        #print 'PREV REV', prevpn, 'NEW REV', resolution_number

        if prevpn is None:
            self._cur.execute('INSERT INTO kv VALUES (?, ?, ?)',
                              (key, value, resolution_number))
            self._con.commit()
            
        elif resolution_number > prevpn:
            #print 'UPDATING TO: ', value
            self._cur.execute('UPDATE kv SET value=?, resolution=? WHERE key=?',
                              (value, resolution_number, key))
            self._con.commit()

            
    def get_last_resolution(self):
        r = self._cur.execute('SELECT MAX(resolution) FROM kv').fetchone()[0]

        return r if r is not None else -1

    
    def iter_updates(self, start_resolution, end_resolution=2**32):
        c = self._con.cursor()
        c.execute('SELECT key,value,resolution FROM kv WHERE resolution>? AND resolution<?  ORDER BY resolution',
                  (start_resolution, end_resolution))
        return c



class KeyValNode (node.BasicNode):
    '''
    This class implements the Paxos logic for KeyValueDB. It extends
    node.BasicNode in two significant ways. First, it embeds within
    each heartbeat message the current multi-paxos sequence number. This
    allows late-joining/recovering nodes to quickly discover that their
    databases are out of sync and begin the catchup process. Second,
    this node drops all Paxos messages while the database is catching
    up. This prevents newly chosen values from entering the database
    prior to the completion of the catchup process. 
    '''

    chatty = False

    def __init__(self,
                 kvdb,
                 node_uid,
                 durable_dir,
                 object_id):

        super(KeyValNode,self).__init__(node_uid, durable_dir, object_id)
        
        self.kvdb = kvdb


    def getHeartbeatData(self):
        return dict( seq_num = self.currentInstanceNum() )
    
    
    def onHeartbeat(self, data):
        #if self.node_uid in 'd':
        #    print 'Heartbeat Received', self.node_uid, data['seq_num'] - 1, self.kvdb.getMaxDBSequenceNumber()
            
        if data['seq_num'] - 1 > self.kvdb.getMaxDBSequenceNumber():
            if data['seq_num'] > self.currentInstanceNum():
                self.slewSequenceNumber( data['seq_num'] )

            self.kvdb.catchup()


    # Override the sequence checking function in the baseclass to cause all
    # packets to be dropped from our Paxos implementation while our database
    # is behind
    def checkSequence(self, header):
        return super(KeyValNode,self).checkSequence(header) and not self.kvdb.isCatchingUp()


    #def onShutdown(self):
    #    print self.node_uid, 'Shutdown!'
        
    def onLeadershipAcquired(self):
        print self.node_uid, 'I have the leader!'


    def onLeadershipLost(self):
        print self.node_uid, 'I LOST the leader!'


    #def onLeadershipChanged(self, prev_leader_uid, new_leader_uid):
    #    print '*** Change of guard: ', prev_leader_uid, new_leader_uid


    def onBehindInSequence(self):
        self.kvdb.catchup()

        
    def onProposalResolution(self, instance_num, value):
        # This method is only called when our database is current
        if self.chatty:
            print '*** Resolution! ', instance_num, repr(value)

        #print '** DECODED: ', json.loads(value)

        key, value = json.loads(value)

        self.kvdb.onValueSet( key, value, instance_num )
        



class KeyValueDB (node.JSONResponder):
    '''
    This class implements a distributed key=value database that uses
    Paxos to coordinate database updates. Unlike the replacated state
    machine design typically discussed in Paxos literature, this implementation
    takes a simpler approach to ensure consistency. Each key/value update
    includes in the database the Multi-Paxos instance number used to set
    the value for that key. When a node sees an instance number ahead of what
    it thinks is the current number, it continually requests key-value pairs
    with greater instance ids from peer nodes. These are returned in sorted
    order. When the node detects that the current Paxos instance is 1 greater
    than it's most recent database entry, it knows that it is consistent
    with it's peers and will begin participating in Paxos instance resolutions.
    '''

    _node_klass = KeyValNode
    chatty = None
    
    def __init__(self, node_uid,
                 database_dir,
                 database_filename=None,
                 catchup_retry_delay=2.0,
                 catchup_num_items=2):

        if database_filename is None:
            database_filename = os.path.join(database_dir, 'db.sqlite')

        if database_filename == ':memory:':
            durable_dir = None
            durable_id  = None
        else:
            durable_dir = database_dir
            durable_id  = os.path.basename(database_filename + '.paxos')

        self.node_uid = node_uid
        self.kv_node = self._node_klass(self, node_uid, durable_dir, durable_id)

        self.catchup_retry_delay = catchup_retry_delay
        self.catchup_num_items   = catchup_num_items
            
        self.db            = SqliteDB( database_filename )
        self.db_seq        = self.db.get_last_resolution()
        self.catching_up   = False
        self.catchup_retry = None

        self.rep_addr      = None
        self.other_reps    = None
            
        self.rep           = None
        self.dlr           = None


    def onCaughtUp(self):
        pass

    def _loadConfiguration(self, cfg_str=None):
        if cfg_str is None:
            cfg_str = self.db.get_value(_ZPAX_CONFIG_KEY)

        cfg = json.loads(cfg_str)

        zpax_nodes = dict()
        kv_reps    = set()
        my_addr    = None

        for n in cfg['nodes']:
            zpax_nodes[ n['uid'] ] = (n['pax_rep_addr'], n['pax_pub_addr'])
            
            if self.kv_node.node_uid == n['uid']:
                my_addr = n['kv_rep_addr']
            else:
                kv_reps.add( n['kv_rep_addr'] )

        if my_addr is None:
            raise MissingConfiguration('Configuration is missing configuration for this node')

        if self.rep_addr is None or self.rep_addr != my_addr:
            self.rep_addr = my_addr
            if self.rep is not None:
                self.rep.close()
            #print '****** REP CREATED: ', self.node_uid, self.rep_addr
            self.rep = tzmq.ZmqRepSocket()
            self.rep.bind(self.rep_addr)
            self.rep.messageReceived = self._generateResponder( '_REP_' )

        if self.other_reps is None or not self.other_reps == kv_reps:
            self.other_reps = kv_reps
            if self.dlr is not None:
                self.dlr.close()
            self.dlr = tzmq.ZmqDealerSocket()
            self.dlr.messageReceived = self._generateResponder( '_DLR_',
                                                                lambda x : x[1:])

            #print 'REQ ', self.node_uid
            for x in kv_reps:
                self.dlr.connect(x)
                #print '     Connecting to: ', x

        if 'quorum_size' in cfg:
            quorum_size = cfg['quorum_size']
        else:
            quorum_size = len(cfg['nodes'])/2 + 1

        print 'QUORUM_SIZE:', self.node_uid, self.kv_node.quorum_size, quorum_size
        if not self.kv_node.is_initialized():
            self.kv_node.initialize( quorum_size )

        elif self.kv_node.quorum_size != quorum_size:
            print '      CHANGING!'
            self.kv_node.changeQuorumSize( quorum_size )

        self.kv_node.connect( zpax_nodes )


    def isInitialized(self):
        return self.db.get_value(_ZPAX_CONFIG_KEY) is not None

    
    def initialize(self, config_str):
        if self.isInitialized():
            raise Exception('Node already initialized')
        
        self.db.update_key(_ZPAX_CONFIG_KEY, config_str, -1)
        self._loadConfiguration()

                
    def shutdown(self):
        self.dlr.close()
        self.rep.close()
        if self.catchup_retry and self.catchup_retry.active():
            self.catchup_retry.cancel()
        self.kv_node.shutdown()

        
    def getMaxDBSequenceNumber(self):
        return self.db_seq

    
    def isCatchingUp(self):
        return self.catching_up


    def onValueSet(self, key, value, instance_num):
        print 'VALUE SET!', self.node_uid, key
        if key == _ZPAX_CONFIG_KEY:
            self._loadConfiguration( value )
        self.db.update_key( key, value, instance_num )        
        self.db_seq = instance_num


    def catchup(self):
        if self.catching_up or self.db_seq == self.kv_node.currentInstanceNum() - 1:
            return 
        
        self._catchup()

        
    def _catchup(self):
        if self.chatty:
            print '_catchup(): ',
        self.catching_up = self.db_seq != self.kv_node.currentInstanceNum() - 1
        
        if not self.catching_up:
            self.onCaughtUp()
            if self.chatty:
                print '*** CAUGHT UP!! ***'
            return

        if self.chatty:
            print 'Requesting next batch from ', self.db_seq

        self.catchup_retry = reactor.callLater(self.catchup_retry_delay,
                                               self._catchup)

        try:
            self.dlr.send( '', json.dumps( dict(type='catchup_request', last_known_seq=self.db_seq) ) )
        except Exception, e:
            print 'EXCEPT! ', str(e)

        
    #--------------------------------------------------------------------------
    # REQ Socket
    #
    def _DLR_catchup_data(self, msg):
        if self.db_seq != msg['from_seq']:
            # Reply to an old request. Ignore it.
            return
            
        if False:#self.chatty:
            print 'CATCHUP MSG: ', msg
            print 'Catchup Data({},{}): '.format(msg['from_seq'], self.db_seq),
            print ', '.join( '({}{}{})'.format(*t) for t in msg['key_val_seq_list'] )
        
        if self.catchup_retry and self.catchup_retry.active():
            self.catchup_retry.cancel()
            self.catchup_retry = None
            
        if msg['from_seq'] == self.db_seq:
            for key, val, seq_num in msg['key_val_seq_list']:
                # XXX Convert to updating all keys in one commit
                if key == _ZPAX_CONFIG_KEY:
                    try:
                        self._loadConfiguration(val)
                    except MissingConfiguration:
                        # We've been removed from the loop :(
                        pass
                self.db.update_key(key, val, seq_num)
                
            self.db_seq = self.db.get_last_resolution()

            if self.chatty:
                print 'LAST RESOLUTION: ', self.db.get_last_resolution()
                    
        self._catchup()

    #--------------------------------------------------------------------------
    # REP Socket
    #           
    def rep_reply(self, **kwargs):
        self.rep.send( json.dumps(kwargs) )

        
    def _REP_propose_value(self, header):
        if self.chatty:
            print 'Proposing ', header
        try:
            #if header['key'] == _ZPAX_CONFIG_KEY:
            #    raise Exception('Access Denied')
            jstr = json.dumps( [header['key'], header['value']] )
            self.kv_node.proposeValue(jstr)
            self.rep_reply( proposed = True )
        except node.ProposalFailed, e:
            print 'Proposal FAILED: ', str(e)
            self.rep_reply(proposed=False, message=str(e))

            
    def _REP_query_value(self, header):
        if self.chatty:
            print 'Querying ', header
        if header['key'] == _ZPAX_CONFIG_KEY:
            #self.rep_reply(error='Access Denied')
            self.rep_reply( value = self.db.get_value(header['key']) )
        else:
            self.rep_reply( value = self.db.get_value(header['key']) )


    def _REP_catchup_request(self, header):
        l = list()
        
        for tpl in self.db.iter_updates(header['last_known_seq']):
            l.append( tpl )
            if len(l) == self.catchup_num_items:
                break

        self.rep_reply( type             = 'catchup_data',
                        from_seq         = header['last_known_seq'],
                        key_val_seq_list = l )


        

        
