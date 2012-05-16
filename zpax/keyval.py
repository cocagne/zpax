import os.path
import json

from zpax import tzmq, db, node
from paxos import multi, basic
from paxos.leaders import heartbeat

from twisted.internet import defer, task, reactor



class KeyValNode (node.BasicNode):

    CATCHUP_RETRY_DELAY  = 2.0
    CATCHUP_NUM_ITEMS    = 2

    def __init__(self, node_uid,
                 local_pub_sub_addr, local_rtr_addr, local_rep_addr,
                 remote_pub_sub_addrs,
                 remote_rep_addrs,
                 quorum_size,
                 database_dir,
                 database_filename=None):

        self.rtr_addr = local_rtr_addr
        self.rep_addr = local_rep_addr
        
        seq_num = 0 # XXX Integrate Durable Objects into BasicNode
        
        super(KeyValNode,self).__init__( node_uid,
                                         local_pub_sub_addr,
                                         remote_pub_sub_addrs,
                                         quorum_size,
                                         seq_num )

        if database_filename is None:
            database_filename = os.path.join(database_dir, 'db.sqlite')
            
        self.db            = db.DB( database_filename )
        self.db_seq        = self.db.get_last_resolution()
        self.catching_up   = False
        self.catchup_retry = None
            
        #self.router        = tzmq.ZmqRouterSocket()
        self.rep           = tzmq.ZmqRepSocket()
        self.req           = tzmq.ZmqReqSocket()

        #self.router.messageReceived = self._on_router_received
        self.rep.messageReceived    = self._on_rep_received
        self.req.messageReceived    = self._on_req_received
        
        #self.router.bind(self.rtr_addr)
        self.rep.bind(self.rep_addr)

        for x in remote_rep_addrs:
            self.req.connect(x)

            
    def onShutdown(self):
        self.req.close()
        self.rep.close()
        if self.catchup_retry and self.catchup_retry.active():
            self.catchup_retry.cancel()
            

    def getHeartbeatData(self):
        return dict( seq_num = self.mpax.instance_num )
    
    
    def onHeartbeat(self, data):
        if data['seq_num'] - 1 > self.db_seq:
            if data['seq_num'] > self.mpax.instance_num:
                self.slewSequenceNumber( data['seq_num'] )

            self.catchup()


    # Override the sequence checking function in the baseclass to cause all
    # packets to be dropped from our Paxos implementation while our database
    # is behind
    def _check_sequence(self, header):
        return super(KeyValNode,self)._check_sequence(header) and not self.catching_up

    
    def onLeadershipAcquired(self):
        print self.node_uid, 'I have the leader!', self.mpax.node.proposer.value


    def onLeadershipLost(self):
        print self.node_uid, 'I LOST the leader!'


    def onLeadershipChanged(self, prev_leader_uid, new_leader_uid):
        print '*** Change of guard: ', prev_leader_uid, new_leader_uid


    def onBehindInSequence(self):
        self.catchup()

        
    def onProposalResolution(self, instance_num, value):
        # This method is only called when our database is current
        
        print '*** Resolution! ', instance_num, repr(value)

        print '** DECODED: ', json.loads(value)

        key, value = json.loads(value)
        
        self.db.update_key( key, value, instance_num )

        self.db_seq = instance_num
        

    #-------------------------------
    # Req Socket Messaging
    #
    def _on_req_received(self, msg_parts):
        #print 'Req Rec: ', msg_parts
        try:
            parts = [ json.loads(p) for p in msg_parts ]
        except ValueError:
            print 'Invalid JSON: ', msg_parts
            return

        if not parts or not 'type' in parts[0]:
            print 'Missing message type', parts
            return

        fobj = getattr(self, '_on_req_' + parts[0]['type'], None)
        
        if fobj:
            fobj(*parts)


    def _on_req_catchup_data(self, msg):
        print 'CATCHUP MSG: ', msg
        print 'Catchup Data({},{}): '.format(msg['from_seq'], self.db_seq),
        print ', '.join( '({}{}{})'.format(*t) for t in msg['key_val_seq_list'] )
        
        if self.catchup_retry and self.catchup_retry.active():
            self.catchup_retry.cancel()
            self.catchup_retry = None
            
        if msg['from_seq'] == self.db_seq:
            for key, val, seq_num in msg['key_val_seq_list']:
                # XXX Convert to updating all keys in one commit
                self.db.update_key(key, val, seq_num)
                
            self.db_seq = self.db.get_last_resolution()

            print 'LAST RESOLUTION: ', self.db.get_last_resolution()
                    
        self._catchup()


    def catchup(self):
        if self.catching_up or self.db_seq == self.mpax.instance_num - 1:
            return 
        
        self._catchup()

        
    def _catchup(self):
        print '_catchup(): ',
        self.catching_up = self.db_seq != self.mpax.instance_num - 1
        
        if not self.catching_up:
            print '*** CAUGHT UP!! ***'
            return
        print 'Requesting next batch from ', self.db_seq

        self.catchup_retry = reactor.callLater(self.CATCHUP_RETRY_DELAY,
                                               self._catchup)

        self.req.send( json.dumps( dict(type='catchup_request', last_known_seq=self.db_seq) ) )
    
            
    #-------------------------------
    # Rep Socket Messaging
    #
    def _on_rep_received(self, msg_parts):
        print 'Rep Rec: ', msg_parts
        try:
            parts = [ json.loads(p) for p in msg_parts ]
        except ValueError:
            print 'Invalid JSON: ', msg_parts
            return

        if not parts or not 'type' in parts[0]:
            print 'Missing message type', parts
            return

        fobj = getattr(self, '_on_rep_' + parts[0]['type'], None)
        
        if fobj:
            fobj(*parts)

            
    def rep_reply(self, **kwargs):
        self.rep.send( json.dumps(kwargs) )

        
    def _on_rep_propose_value(self, header):
        print 'Proposing ', header
        try:
            jstr = json.dumps( [header['key'], header['value']] )
            self.proposeValue(self.mpax.instance_num, jstr)
            self.rep_reply( proposed = True )
        except node.ProposalFailed, e:
            print 'Proposal FAILED: ', str(e)
            self.rep_reply(proposed=False, message=str(e))

            
    def _on_rep_query_value(self, header):
        print 'Querying ', header
        print 'DB RESULT: ', self.db.get_value(header['key'])
        self.rep_reply( value = self.db.get_value(header['key']) )


    def _on_rep_catchup_request(self, header):
        l = list()
        
        for tpl in self.db.iter_updates(header['last_known_seq']):
            l.append( tpl )
            if len(l) == self.CATCHUP_NUM_ITEMS:
                break

        self.rep_reply( type             = 'catchup_data',
                        from_seq         = header['last_known_seq'],
                        key_val_seq_list = l )
        
