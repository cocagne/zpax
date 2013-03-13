'''
This module provides a Paxos Commit implementation.

Implementation Strategy:

Liveness: To ensure forward progress, each transaction manager is assigned
          a MultiPaxos instance. The leader of the MultiPaxos session is
          responsible for driving all outstanding transactions to conclusion.
          In essence, this re-uses the liveness solution for MultiPaxos.

Parallelism: All transactions are executed in parallel and no guarantees
             are made about the order in which they complete.

Durability: To achieve any measure of performance with multiple,
            concurrent transactions, some manner of batched writes to
            disk will be required.  The transaction manager requires a
            "durability" object that implements the durable.IDurableStateStore
            interface. When TransactionNodes need to save their state
            prior to sending promise/accepted messages, they will call
            the "save_state()" method of the durable object and send
            the message when the returned deferred fires.  

Threshold: In traditional Paxos Commit, every node must respond with the
           commit/abort decision before the overall transaction may be
           considered complete. For quorum-based applications that can
           tolerate one or more "aborts" and still consider the overall
           transaction a success, a "threshold" parameter may be used to
           indicate the required number of "comitted" messages needed
           for overall success. This defaults to the total number of
           nodes.

Messaging: This implementation avoids the final "commit"/"abort" message
           by having all nodes listen to each other directly. When a node
           sees that enough of its peers have responded with their own
           "commit"/"abort" decision, it can determine the overall result
           for itself.

Recovery: Each node maintains a cache of transaction results. When a
          message arrives for an already concluded transaction that is
          contained within the cache, the result is sent directly.
          Applications should override the default cache implementation
          and provide a "check" function that can look further back
          in time than the in-memory cache allows. Otherwise,
          old transactions from a recovered node may either appear to
          be new or may never complete.
'''

import time

from   twisted.internet import defer

import paxos.practical
from   paxos.practical import ProposalID


class TransactionHistory(object):

    max_entries = 1000
    
    def __init__(self):
        self.recent_list = list()
        self.recent_map  = dict()

    def add(self, tx_uuid, result):
        self.recent_list.append( tx_uuid )
        self.recent_map[ tx_uuid ] = result
        if len(self.recent_list) > self.max_entries:
            del self.recent_map[ self.recent_list.pop(0) ]

    def lookup(self, tx_uuid):
        return self.recent_map.get(tx_uuid, None)




class TransactionManager (object):
    '''
    
    Transaction Managers monitor and ensure the progress of Transaction
    instances. Battles for Paxos leadership are avoided through the use of a
    dedicated MultiPaxos instance. Only the leader of the MultiPaxos session
    will attempt to drive timed-out transactions to resolution.  Leadership
    battles are still possible as multiple MultiPaxos instances may
    simultaneously believe themselves to be the leader.  However, the durations
    of these battles should be brief and it greatly simplifies the logic behind
    this implementation.

    A further use of the MultiPaxos instance is that it may be used to make
    decisions for the nodes in the transaction pool. To add or remove a node
    from the pool, for example, the MultiPaxos instance could be used to
    resolve "pause_new_transaction_acceptance", the instances would then
    disable sending "accepted" messages on the MultiPaxos instance until all of
    the outstanding transactions are complete. Then, "transactions_paused"
    would be resolved. At this point, the new node configuration would be
    resolved and all of the nodes would transition to the new
    configuration. Finally, "resume_new_transactions" would be resolved to
    re-enable normal behavior.
    
    '''

    timeout_duration   = 30.0 # seconds

    get_current_time   = time.time

    tx_history_factory = TransactionHistory

    # all_node_ids = set() of node ids
    def __init__(self, net_channel, quorum_size,
                 all_node_ids, threshold, durable):
        self.net           = net_channel
        self.node_uid      = net_channel.node_uid
        self.quorum_size   = quorum_size
        self.all_node_ids  = all_node_ids
        self.threshold     = threshold
        self.durable       = durable
        self.transactions  = dict()       # uuid => Transaction instance
        self.results_cache = self.tx_history_factory()

        self.net.add_message_handler( self )


    def heartbeat(self, current_time, transaction_leader=False):
        for tx in self.transactions.values():
            tx.heartbeat(current_time, transaction_leader)
            

    def get_transaction(self, tx_uuid):
        return self.transactions.get(tx_uuid, None)


    def get_result(self, tx_uuid):
        d = defer.Deferred()
        
        r = self.results_cache.lookup(tx_uuid)
        if r:
            d.callback(r)
            return d

        tx = self.transactions.get(tx_uuid)
        if tx:
            tx.dcomplete.addCallback( lambda *ignore: d.callback( tx.result ) )
            return d

        d.errback(KeyError('Unknown Transaction Id'))
        return d
            

    def propose_result(self, tx_uuid, result, **kwargs):
        assert result in ('commit', 'abort')

        r = self.results_cache.lookup(tx_uuid)
        if r:
            return defer.succeed( (tx_uuid, r) )
        
        tx = self.transactions.get(tx_uuid, None)

        if tx is None:
            tx = self.create_transaction(tx_uuid, kwargs)

        tx.tx_nodes[ self.node_uid ].set_proposal( result )

        return tx.dcomplete

        
    def get_transaction_node(self, from_uid, msg):
        try:
            tx_uuid = msg['tx_uuid']
            tx_node = msg['tx_node']
        except KeyError:
            return


        if not tx_node in self.all_node_ids:
            return
        
        tx = self.transactions.get(tx_uuid, None)

        if tx is not None:
            return tx.tx_nodes[ tx_node ]
        
        r = self.results_cache.lookup( tx_uuid )
        
        if r is not None:
            self.net.unicast(from_uid, 'transaction_result',
                             dict( tx_uuid=tx_uuid, result=r ))
            return
        
        tx = self.create_transaction( tx_uuid, msg )

        if tx is None:
            return
        
        return tx.tx_nodes[ tx_node ]

        
    def create_transaction(self, tx_uuid, msg):
        tx =  Transaction(self, tx_uuid,
                          self.get_current_time() + self.timeout_duration,
                          self.node_uid,
                          self.quorum_size, self.threshold)
        self.transactions[ tx_uuid ] = tx
        tx.dcomplete.addCallback( self.transaction_complete )
        return tx

    
    def transaction_complete(self, tpl):
        tx_uuid, result = tpl
        del self.transactions[ tx_uuid ]
        self.results_cache.add( tx_uuid, result )
        return tpl

    
    def receive_transaction_result(self, from_uid, msg):
        try:
            tx_uuid = msg['tx_uuid']
            result  = msg['result']
            tx      = self.transactions[ tx_uuid ]
        except KeyError:
            return
        
        tx.set_resolution( result )
        

    def _tx_handler(func):
        def wrapper(self, from_uid, msg):
            txn = self.get_transaction_node( from_uid, msg )
            if txn:
                func(self, txn, from_uid, msg)
        return wrapper

    @_tx_handler
    def receive_prepare(self, txn, from_uid, msg):
        txn.receive_prepare(from_uid, msg)

    @_tx_handler
    def receive_accept(self, txn, from_uid, msg):
        txn.receive_accept(from_uid, msg)

    @_tx_handler
    def receive_promise(self, txn, from_uid, msg):
        txn.receive_promise(from_uid, msg)

    @_tx_handler
    def receive_prepare_nack(self, txn, from_uid, msg):
        txn.receive_prepare_nack(from_uid, msg)

    @_tx_handler
    def receive_accept_nack(self, txn, from_uid, msg):
        txn.receive_accept_nack(from_uid, msg)

    @_tx_handler
    def receive_accepted(self, txn, from_uid, msg):
        txn.receive_accepted(from_uid, msg)


    
class TransactionNode(paxos.practical.Node):
    '''
    For each transaction, there will be once instance of this class for each
    node participating in the transaction. Only two proposed values are
    permitted: "commit" and "abort". Only the node with the id matching
    the node id assigned to the TransactionNode instance may propose
    "commit". All other nodes may propose "abort" (and only "abort") once
    the overall transaction timeout has expired.
    '''

    def __init__(self, tx, tx_node_id, node_uid, quorum_size):
        super(TransactionNode,self).__init__(self, node_uid, quorum_size)
        self.tx           = tx
        self.tx_node_id   = tx_node_id
        self.is_this_node = tx_node_id == node_uid
        self.saving       = False

        if self.is_this_node:
            self.active = False # Disable to prevent prepare() from sending a message
            self.prepare()      # Allocate the first proposal id
            self.active = True  # Re-enable messaging
            self.leader = True  # Own-node is the leader for the first message

        
    def heartbeat(self, drive_to_abort):
        '''
        Called periodically to retransmit messages and drive the node to completion.

        If drive_to_abort is True, the TransactionNode will attempt to assume
        leadership of the Paxos instance and drive the "abort" decision to
        concensus.
        '''
        if drive_to_abort:
            if self.proposed_value is None:
                self.set_proposal('abort')
                
            if self.leader:                
                self.resend_accept()
            else:
                self.prepare()
                
        elif self.is_this_node:
            self.resend_accept()


    def set_proposal(self, value):
        #print 'SETTING PROPOSAL for Node {0}: {1} leader={2} active={3}'.format(self.tx_node_id, value, self.leader, self.active)
        super(TransactionNode,self).set_proposal(value)
        

    def observe_proposal(self, from_uid, proposal_id):
        #TODO: Does this need to be called from 'recv_accept_request()' ?
        super(TransactionNode,self).observe_proposal(from_uid, proposal_id)
        if self.leader and proposal_id > self.proposal_id:
            self.leader = False

            
    @defer.inlineCallbacks
    def save_state(self):
        if not self.saving:
            self.saving = True

            yield self.tx.manager.durable.set_state( (self.tx.uuid, self.tx_node_id), (self.promised_id,
                                                     self.accepted_id, self.accepted_value) )
            self.saving = False
            
            self.persisted()

            
    def recv_prepare(self, *args):
        super(TransactionNode, self).recv_prepare(*args)
        if self.persistance_required:
            self.save_state()

            
    def recv_accept_request(self, *args):
        super(TransactionNode, self).recv_accept_request(*args)
        if self.persistance_required:
            self.save_state()

            
    # --- Messenger Interface ---

    
    def broadcast(self, message_type, **kwargs):
        kwargs.update( { 'tx_uuid' : self.tx.uuid,
                         'tx_node' : self.tx_node_id } )
        self.tx.manager.net.broadcast( message_type, kwargs )

    def unicast(self, to_uid, message_type, **kwargs):
        kwargs.update( { 'tx_uuid' : self.tx.uuid,
                         'tx_node' : self.tx_node_id } )
        self.tx.manager.net.unicast( to_uid, message_type, kwargs )
                                           
    def send_prepare(self, proposal_id):
        self.broadcast( 'prepare', proposal_id = proposal_id )
        
    def send_promise(self, proposer_uid, proposal_id, previous_id, accepted_value):
        self.unicast( proposer_uid, 'promise',
                      proposal_id    = proposal_id,
                      previous_id    = previous_id,
                      accepted_value = accepted_value )
        
    def send_accept(self, proposal_id, proposal_value):
        self.broadcast( 'accept',
                        proposal_id    = proposal_id,
                        proposal_value = proposal_value )
        
    def send_accepted(self, proposal_id, accepted_value):
        self.broadcast( 'accepted',
                        proposal_id    = proposal_id,
                        accepted_value = accepted_value )

    def send_prepare_nack(self, to_uid, proposal_id, promised_id):
        self.unicast( to_uid, 'prepare_nack',
                      proposal_id = proposal_id )

    def send_accept_nack(self, to_uid, proposal_id, promised_id):
        self.unicast( to_uid, 'accept_nack',
                      proposal_id = proposal_id,
                      promised_id = promised_id )
        
    def receive_prepare(self, from_uid, msg):
        self.recv_prepare( from_uid, ProposalID(*msg['proposal_id']) )

    def receive_promise(self, from_uid, msg):
        self.recv_promise( from_uid, ProposalID(*msg['proposal_id']),
                         ProposalID(*msg['previous_id']) if msg['previous_id'] else None,
                         msg['accepted_value'] )

    def receive_prepare_nack(self, from_uid, msg):
        self.recv_prepare_nack( from_uid, ProposalID(*msg['proposal_id']) )
        
    def receive_accept(self, from_uid, msg):
        #print 'GOT ACCEPT!', self.tx.manager.node_uid, from_uid
        self.recv_accept_request( from_uid, ProposalID(*msg['proposal_id']), msg['proposal_value'] )

    def receive_accept_nack(self, from_uid, msg):
        self.recv_accept_nack( from_uid, ProposalID(*msg['proposal_id']), ProposalID(*msg['promised_id']) )

    def receive_accepted(self, from_uid, msg):
        self.recv_accepted( from_uid, ProposalID(*msg['proposal_id']), msg['accepted_value'] )
        
    def on_resolution(self, proposal_id, value):
        #print 'NODE RESOLVED!!!', (self.tx.manager.node_uid, self.tx.uuid, self.tx_node_id), value
        self.tx.node_resolved(self)

    def on_leadership_acquired(self):
        pass



class Transaction (object):
    '''
    
    This class represents a single transaction. Each transaction has a UUID
    assigned to it and uses a modified version of Paxos Commit to arrive at
    concensus on the commit/abort decision. A TransactionNode instance (which
    encapsulates a single Paxos instance) is created for each node
    participatring in the decision. Once a node has made it's commit/abort
    decision, it proposes that value for it's corresponding TransactionNode.

    The transaction's heartbeat method should be called periodically to
    ensure forward progress on the transaction. 
    
    '''

    def __init__(self, tx_mgr, tx_uuid, timeout_time, node_uid, quorum_size, threshold):
        self.manager        = tx_mgr
        self.uuid           = tx_uuid
        self.timeout_time   = timeout_time
        self.quorum_size    = quorum_size
        self.threshold      = threshold
        self.tx_nodes       = dict()
        self.num_committed  = 0
        self.num_aborted    = 0
        self.result         = None
        self.dcomplete      = defer.Deferred()
        
        for pax_node_id in self.manager.all_node_ids:
            self.tx_nodes[ pax_node_id ] = TransactionNode(self, pax_node_id, node_uid, quorum_size)

        self.this_node = self.tx_nodes[ node_uid ]


    @property
    def channel_name(self):
        return self.manager.channel_name

            
    def heartbeat(self, current_time, transaction_leader=False):
        if self.timeout_time <= current_time and transaction_leader:
            
            for tx in self.tx_nodes.itervalues():
                if not tx.complete:
                    tx.heartbeat( drive_to_abort=True )
        else:
            if not self.this_node.complete and self.this_node.proposed_value is not None:
                self.this_node.resend_accept()


    def set_resolution(self, final_value):
        '''
        Used when concensus on the final value has already been reached
        '''
        if self.result is None:
            #print '********** RESOLVED: ', self.manager.node_uid, self.uuid, final_value
            self.result = final_value
            self.dcomplete.callback( (self.uuid, self.result) )
        
        
    def node_resolved(self, txn):
        if self.result is not None:
            return

        if txn.final_value == 'commit':
            self.num_committed += 1
        else:
            self.num_aborted += 1
            
        if self.num_committed >= self.manager.threshold:
            self.set_resolution( 'committed' )
                
        elif self.num_aborted > len(self.manager.all_node_ids) - self.manager.threshold:
            self.set_resolution('aborted')

            
