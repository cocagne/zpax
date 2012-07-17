'''
This module provides an implementation for a node in a basic, Multi-Paxos
distributed consensus network.The intent is to create both a useful
implementation for applications without strong performance requirements as well
as an easily-understandable Paxos implementation. 

The overriding goals of this implementation are simplicity and
correctness. Performance is, of course, a consideration but, for this
particular implementation, simple solutions that can get the job done are
generally preferred over more complex and efficient approaches.

With distributed consensus, there are quite a few performance considerations
that must be taken into account by both the design and implementation. A
general-purpose solution can provide, at best, mediocre performance for most
application domains. When good performance is required, a domain-specific
implementation is needed. This code is intended to serve as a simple reference
implementation for those development efforts.
'''

import os
import json
import random
import hashlib
import hmac
import base64

from zpax import tzmq

from paxos import multi, basic
from paxos.proposers import heartbeat

from twisted.internet import defer, task, reactor

try:
    from Crypto.Cipher import AES
except ImportError:
    AES = None


class ProposalFailed(Exception):
    pass



class SequenceMismatch(ProposalFailed):

    def __init__(self, current):
        super(SequenceMismatch,self).__init__('Sequence Number Mismatch')
        self.current_seq_num = current

        
        
class ValueAlreadyProposed(ProposalFailed):
    
    def __init__(self):
        super(ValueAlreadyProposed,self).__init__('Value Already Proposed')



def _encrypt_value( key, value ):
    if AES is None:
        raise Exception('Missing Pycrypto module. Encryption is not supported')
    
    iv = os.urandom(AES.block_size)

    c = AES.new(key, AES.MODE_CBC, iv)

    # Trailing spaces are legal in JSON so we'll use that for padding
    if len(value) % c.block_size != 0:
        value += ' ' * (c.block_size - len(value)%c.block_size)

    return 'ENC:' + base64.b64encode(iv + c.encrypt(value))


def _decrypt_value( key, cipher_txt ):
    if AES is None:
        raise Exception('Missing Pycrypto module. Encryption is not supported')

    assert cipher_txt.startswith('ENC:')

    cipher_txt = base64.b64decode(cipher_txt[4:])

    iv = cipher_txt[:AES.block_size]
    cv = cipher_txt[AES.block_size:]

    c = AES.new(key, AES.MODE_CBC, iv)
    
    return c.decrypt(cv).rstrip()



class BasicHeartbeatProposer (heartbeat.Proposer):
    '''
    This class extends paxos.proposers.heartbeat.Proposer and forwards all
    calls to it's BasicNode instance. Instances of this class are pickled
    and written to disk by the paxos.multi module to ensure correctness
    in the presense of crashes/power failures.
    '''
    hb_period       = 0.5
    liveness_window = 1.5

    def __init__(self, basic_node, node_uid, quorum_size, leader_uid):
        self.node = basic_node

        super(BasicHeartbeatProposer, self).__init__(node_uid,
                                                     quorum_size,
                                                     leader_uid = leader_uid)

    def __getstate__(self):
        # Called by the pickling process. Our 'node' instance cannot be
        # pickled so we omit it in our returned state. BasicNode will
        # reset this attribute after unpickling occurs.
        d = dict(self.__dict__)
        del d['node']
        return d

    def send_prepare(self, proposal_id):
        self.node._paxos_send_prepare(proposal_id)

    def send_accept(self, proposal_id, proposal_value):
        self.node._paxos_send_accept(proposal_id, proposal_value)

    def send_heartbeat(self, leader_proposal_id):
        self.node._paxos_send_heartbeat(leader_proposal_id)

    def schedule(self, msec_delay, func_obj):
        pass

    def on_leadership_acquired(self):
        self.node._paxos_on_leadership_acquired()

    def on_leadership_lost(self):
        self.node._paxos_on_leadership_lost()

    def on_leadership_change(self, prev_leader_uid, new_leader_uid):
        self.node._paxos_on_leadership_change(prev_leader_uid, new_leader_uid)
        


class BasicMultiPaxos(multi.MultiPaxos):
    '''
    This class extends paxos.multi.MultiPaxos to omit unpicklable instance
    state and forward the on_proposal_resolution callback to the callable
    object stored in the instances' on_resolution_cb member variable (which
    will be a bound method to BasicNode._on_proposal_resolution)
    '''
    on_resolution_cb = None # Must be set to a callable object

    def __getstate__(self):
        d = dict( self.__dict__ )
        del d['node_factory']
        del d['on_resolution_cb']
        return d
                
    def on_proposal_resolution(self, instance_num, value):
        self.on_resolution_cb(instance_num, value)
    


class JSONResponder (object):
    '''
    Mixin class providing a simple mechanism for handling ZeroMQ messages
    that are encoded in JSON notation. 
    '''
    def _generateResponder(self, prefix, parts_transform = lambda x : x):
        '''
        Returns a message dispatching function suitable for assignment to a
        ZmqSocket's onMessageReceived member. The message parts must be in JSON
        format and the first message part must include a "type" field that
        names the message type (as a string). The returned function will call a
        member function of the same name with the supplied prefix argument.

        prefix - String added to the message type that names the message
                 handling method

        parts_transform - Optional function that may be used to manipulate the
                          message parts received from ZeroMQ prior to JSON
                          decoding
        '''
        def on_rcv(msg_parts):
            msg_parts = parts_transform(msg_parts)
            
            if msg_parts is None:
                return
            
            try:
                parts = [ json.loads(p) for p in msg_parts ]
            except ValueError:
                print 'Invalid JSON: ', msg_parts
                return

            if not parts or not 'type' in parts[0]:
                print 'Missing message type', parts
                return

            fobj = getattr(self, prefix + parts[0]['type'], None)
        
            if fobj:
                fobj(*parts)
        return on_rcv



class ProposalAdvocate (JSONResponder):
    '''
    An essential component for successful use of Paxos is ensuring that, once a
    proposal has been made, concensus must be continually pursued until it is
    reached. This class assists in that endeavor by ensuring that the current
    leader is always informed of a node's proposal if it has one. Every time
    leadership changes, the new leader is notified of the proposed value.
    
    This approach adds an additional message delay when proposals are sent
    to nodes other than the leader and can result in duplicate messages in
    the presence of failures. However, the implementation is straight-forward
    and easy to understand.
    '''

    def __init__(self, retry_delay):
        '''
        retry_delay - Floating point delay in seconds between retry attempts
        '''
        self.retry_delay      = retry_delay
        self.leader_rep_addr  = None
        self.current_proposal = None
        self.instance_number  = None
        self.proposal_retry   = None
        self.req              = None 

        
    def shutdown(self):
        self._cancel_retry()
        self._close_req()
        

    def _close_req(self):
        if self.req is not None:
            self.req.close()
            self.req = None

            
    def _cancel_retry(self):
        if self.proposal_retry and self.proposal_retry.active():
            self.proposal_retry.cancel()
        self.proposal_retry = None

        
    def _connect(self):
        self._close_req()

        if self.leader_rep_addr:
            self.req = tzmq.ZmqReqSocket()

            self.req.messageReceived = self._generateResponder('_REQ_')
        
            self.req.connect( self.leader_rep_addr )

            
    def cancel_proposal(self, instance_number):
        if self.instance_number <= instance_number:
            self._cancel_retry()
            self.current_proposal = None


    def leadership_changed(self, new_leader_rep_addr):
        self.leader_rep_addr = new_leader_rep_addr
        self._connect()
        self._propose()
    

    def set_proposal(self, node_uid, sequence_number, value):
        if self.current_proposal is None:
            d = dict( type     = 'propose_value',
                      node_uid = node_uid,
                      seq_num  = sequence_number,
                      value    = value)
            
            self.current_proposal = json.dumps(d)
            self.instance_number  = sequence_number

            self._propose()

            
    def _propose(self, retry=False):
        if self.proposal_retry and self.proposal_retry.active():
            self.proposal_retry.cancel()
        self.proposal_retry = None
        
        if self.current_proposal and self.req is not None:
            self.proposal_retry = reactor.callLater(self.retry_delay,
                                                    self._propose,
                                                    True)
            if retry:
                # ZeroMQ REQ sockets do not allow two send() calls in sequence.
                # Consequently, we have to shutdown and recreate the socket for
                # retry attempts
                self._connect()

            self.req.send( self.current_proposal )

            
    def _REQ_value_proposed(self, header):
        if self.proposal_retry and self.proposal_retry.active():
            self.proposal_retry.cancel()
            self.proposal_retry = None


            
class BasicNode (JSONResponder):
    '''
    This class provides the basic functionality required for Multi-Paxos over
    ZeroMQ Publish/Subscribe sockets. This class follows the GoF95 template
    design pattern and delegates all application level logic to a subclass.

    In addition to using the Publish/Subscribe sockets for sending the Paxos
    protocol messages, a pair of Req/Rep sockets is used for communicating
    proposals. Clients may connect to any node and issue a proposal. If the
    receiving node is not the current leader, it will forward the proposal to
    the current leader rather than try and assume leadership itself (constant
    leadership battles can greatly reduce performance).

    To ensure forward progress, the ProposalAdvocate class is used to inform
    each newly-elected leader of the node's current proposal if it has one.

    This class optionally supports both HMAC message authentication and
    encryption of proposed values to provide some measure of security when used
    over insecure networks. As the encryption keys for both of these class
    member variables must always be kept in perfect sync with the rest of the
    Paxos network, it is highly recommended that changes to these values go
    through the full Paxos protocol. The zpax.keyval implementation contains
    and example of how this can be done.
    '''

    hb_proposer_klass = BasicHeartbeatProposer

    def __init__(self, node_uid, durable_dir=None, object_id=None):

        self.node_uid           = node_uid
        self.accept_retry       = None
        self.delayed_prepare    = None

        self.proposal_advocate  = ProposalAdvocate( self.hb_proposer_klass.hb_period )

        self.last_value         = None
        self.last_seq_num       = None

        self.zpax_nodes         = None # Dictionary of node_uid -> (rep_addr, pub_addr)

        self.pax_rep            = None
        self.pax_pub            = None
        self.pax_sub            = None

        self.mpax                    = BasicMultiPaxos(durable_dir, object_id)
        self.mpax.node_factory       = self._node_factory
        self.mpax.on_resolution_cb   = self._on_proposal_resolution

        if self.mpax.node:
            #
            # We're recovering durable state from disk
            #
            self.mpax.node.proposer.node = self

            self.mpax.node.proposer.on_recover()
            
            if self.mpax.node.proposer.value:
                # If recovered node has a proposed value, the crash/shutdown
                # occured while a value was being negotiated. The value must be
                # given to the proposal advocate to ensure that the negotiation
                # completes.
                self.proposal_advocate.set_proposal( self.node_uid,
                                                     self.sequence_number,
                                                     self.mpax.node.proposer.value )

        self.heartbeat_poller = task.LoopingCall( self._poll_heartbeat         )
        self.heartbeat_pulser = task.LoopingCall( self._pulse_leader_heartbeat )
        
        self.hmac_key  = None # Optional Message Authentication 
        self.value_key = None # and Encryption attributes

        
    quorum_size     = property( lambda self: self.mpax.quorum_size  )
    sequence_number = property( lambda self: self.mpax.instance_num )
    initialized     = property( lambda self: self.mpax.quorum_size is not None )

    
    def initialize(self, quorum_size):
        '''
        This method should only be called once during the initial setup.
        Afterwards, the quorum size may be changed with the changeQuorumSize()
        method
        '''
        assert not self.initialized, 'MultiPaxos instance already initialized'
        
        self.mpax.initialize( self.node_uid, quorum_size )

        if self.pax_sub is not None:
            self.heartbeat_poller.start( self.hb_proposer_klass.liveness_window )


    def connect(self, zpax_nodes):
        '''
        Connects this node to the nodes contained in zpax_nodes. This method
        may be called on-the-fly to update the node's connections as the
        Paxos configuration changes. Note that the nodes need to agree on what
        the Paxos membership should be in order for dynamic reconfiguration to
        work. It is highly recommended that changes to the Paxos membership be
        negotiated through the Paxos protocol itself. The keyvalue module
        provides and example of how to accomplish this.

        zpax_nodes - Dictionary of node_uid => (zmq_rep_addr, zmq_pub_addr)
        '''
        
        # Note: This method looks more complex than it actually is. Essentially
        #       all that's being done is updating the current ZeroMQ sockets
        #       to connect to the list of nodes in the zpax nodes argument. The
        #       verbose implementation comes from trying to avoid unnessary
        #       socket closures/reconnects.
        
        if self.zpax_nodes == zpax_nodes:
            return # No change

        if not self.node_uid in zpax_nodes:
            raise Exception('Missing local node configuration')

        if self.zpax_nodes is None:
            self.zpax_nodes = zpax_nodes

        if not self.initialized:
            # If no explicit quorum size has been set. Default to the minimum
            self.initialize( len(zpax_nodes)/2 + 1 )
                    
        def local_addr_changed(i):
            return self.zpax_nodes[self.node_uid][i] != zpax_nodes[self.node_uid][i]
        
        if self.pax_rep is None or local_addr_changed(0):
            if self.pax_rep:
                self.pax_rep.close()
            self.pax_rep = tzmq.ZmqRepSocket()
            self.pax_rep.linger = 0
            self.pax_rep.bind(zpax_nodes[self.node_uid][0])
            self.pax_rep.messageReceived = self._generateResponder('_REP_')

        if self.pax_pub is None or local_addr_changed(1):
            if self.pax_pub:
                self.pax_pub.close()
            self.pax_pub = tzmq.ZmqPubSocket()
            self.pax_pub.linger = 0
            self.pax_pub.bind(zpax_nodes[self.node_uid][1])

        prev_remote = set( t[1] for t in self.zpax_nodes.itervalues() )
        cur_remote  = set( t[1] for t in zpax_nodes.itervalues()      )

        if self.pax_sub is None or prev_remote != cur_remote:
            if self.pax_sub is not None:
                self.pax_sub.close()

            self.pax_sub = tzmq.ZmqSubSocket()
            self.pax_sub.linger = 0
            self.pax_sub.subscribe       = 'zpax'        
            self.pax_sub.messageReceived = self._generateResponder('_SUB_', self._check_hmac)

            for x in cur_remote:
                self.pax_sub.connect(x)

        if self.mpax.quorum_size is not None and not self.heartbeat_poller.running:
            self.heartbeat_poller.start( self.hb_proposer_klass.liveness_window )

        self.zpax_nodes = zpax_nodes


    def onLeadershipAcquired(self):
        '''
        Called when this node acquires Paxos leadership.

        Note, this is not reliable as a unique distiction between
        nodes. Multiple nodes may simultaneously believe themselves to be the
        leader. Also, it is possible for another leader to have been elected
        and several proposals resolved before this method is even called. Use
        caution when employing this or any other leadership-related method.
        '''

    def onLeadershipLost(self):
        '''
        Called when this node looses Paxos leadership.

        See note on onLeadershipAcquired() about the reliability of this method.
        '''

    def onLeadershipChanged(self, prev_leader_uid, new_leader_uid):
        '''
        Called whenver Paxos leadership changes.

        See note on onLeadershipAcquired() about the reliability of this method.
        '''

    def onBehindInSequence(self, old_sequence_number, new_sequnce_number):
        '''
        Called when this node's sequence number is observed to be behind a more
        recent value. Note that there is no guarantee that the new sequence
        number is the one currently under negotiation. It would well be
        hundreds of iterations behind the truly current sequence number.
        '''

    def onProposalResolution(self, sequence_num, value):
        '''
        Called when an instance of the Paxos algorithm agrees on a value.  Note
        that this does not mean that this sequence number is the most recent.
        Due to network delays, system restarts, OS hibernation, etc, it is
        possible that thousands of additional resolutions may have taken place
        by the time this method is invoked.
        '''

    def onHeartbeat(self, data):
        '''
        data - Dictionary of key=value pairs contained in the heartbeat message
        '''

    def onShutdown(self):
        '''
        Called immediately before shutting down. Resource cleanups should occur
        here.
        '''

    def getHeartbeatData(self):
        '''
        Returns a dictionary of key=value parameters to be included in the
        heartbeat message. Subclasses may override this method to include
        application-specific content in the heartbeat messages. The
        onHeartbeat() method on the receiving nodes method will be given
        the content returned here.
        '''
        return {}

    
    def getCurrentSequenceNumber(self):
        '''
        Returns the sequence number for the multi-paxos instance this node
        believes is currently under negotiation. This may or not be the true
        current sequence number.
        '''
        return self.mpax.instance_num

    
    def setCurrentSequenceNumber(self, new_sequence_number):
        '''
        Sets the current sequence number to the new value. This may be used by
        subclasses to advance an out-of-date node to the current Paxos
        instance. Great care must be taken when using this method, however.  If
        state from the previous Paxos instances is of importance, the subclass
        must ensure that it has completely caught up with its peers and is
        ready to engage in negotiation of the new sequence number.
        '''
        assert new_sequence_number > self.sequence_number

        self.proposal_advocate.cancel_proposal( new_sequence_number - 1 )
        
        if self.mpax.node.proposer.leader:
            self._paxos_on_leadership_lost()
            
        self.mpax.set_instance_number(new_sequence_number)

        
    def proposeValue(self, value, sequence_number=None):
        '''
        Proposes a value for the current Paxos round. If a sequence_number is
        specified in the argument list, an exception will be thrown if it does
        not match the current value. An exception will also be thrown if this
        node is aware that a value has already been proposed. This is for
        early-failure-detection only. The lack of a thrown exception does not
        mean that the value will be chosen as the Paxos round's final result.
        '''
        if sequence_number is not None and not sequence_number == self.sequence_number:
            raise SequenceMismatch( self.sequence_number )

        if self.mpax.node.proposer.value is not None:
            raise ValueAlreadyProposed()

        if self.mpax.node.acceptor.accepted_value is not None:
            raise ValueAlreadyProposed()

        if self.value_key:
            value = _encrypt_value( self.value_key, value )

        self.proposal_advocate.set_proposal(self.node_uid,
                                            self.sequence_number,
                                            value)


    def shutdown(self):
        self.onShutdown()
        self.proposal_advocate.shutdown()
        if self.accept_retry is not None and self.accept_retry.active():
            self.accept_retry.cancel()
        if self.delayed_prepare is not None and self.delayed_prepare.active():
            self.delayed_prepare.cancel()
        self.pax_rep.close()
        self.pax_pub.close()
        self.pax_sub.close()
        self.pax_rep = None
        self.pax_pub = None
        self.pax_sub = None
        if self.heartbeat_poller.running:
            self.heartbeat_poller.stop()
        if self.heartbeat_pulser.running:
            self.heartbeat_pulser.stop()


    def checkSequence(self, header):
        '''
        Checks the sequence number of the header. If the instance's current
        sequence number is behind, onBehindInSequence() is called.

        The return value is True if the header's squence number matches this
        node's and False otherwise. If False is returned, the message will be
        considered invalid and will not be processed. Overloading this method
        and artificially returning False may be used by subclasses to
        temporarily disable participation in Paxos messaging.
        '''
        seq = header['seq_num']
        
        if seq > self.sequence_number:
            self.onBehindInSequence(self.sequence_number, seq)
            
        return seq == self.sequence_number


    def changeQuorumSize(self, new_quorum_size):
        '''
        Changes the quorum size to the new value. Note that this should be done
        by all nodes at approximately the same time to ensure consistency.  A
        simple way to ensure correctness is to use the Paxos algorithm itself
        to choose the new quorum size (along with the corresponding
        configuration information for the added and or removed nodes)
        '''
        self.mpax.change_quorum_size( new_quorum_size )

        
    #--------------------------------------------------------------------------
    # Helper Methods
    #
    def _node_factory(self, node_uid, leader_uid, quorum_size, resolution_callback):
        return basic.Node( self.hb_proposer_klass(self, node_uid, quorum_size, leader_uid),
                           basic.Acceptor(),
                           basic.Learner(quorum_size),
                           resolution_callback )

    
    def _check_hmac(self, msg_parts):
        if self.hmac_key:
            h = hmac.new(self.hmac_key, digestmod=hashlib.sha1)
            for jp in msg_parts[2:]:
                h.update(jp)
            if h.digest() == msg_parts[1]:
                return msg_parts[2:]
            else:
                return None # Invalid HMAC, drop message
        else:
            return msg_parts[1:]

        
    #--------------------------------------------------------------------------
    # Heartbeats 
    #
    def _poll_heartbeat(self):
        self.mpax.node.proposer.poll_liveness()

        
    def _pulse_leader_heartbeat(self):
        self.mpax.node.proposer.pulse()


    #--------------------------------------------------------------------------
    # Reply Socket Messaging 
    #
    # Methods named '_REP_<message_type>' are called when packets of that type
    # are recieved over the ZeroMQ Reply socket
    #
    def _REP_propose_value(self, header):
        if header['seq_num'] == self.sequence_number:
            if self.mpax.node.acceptor.accepted_value is None:
                value = header['value']
                if self.value_key and not value.startswith('ENC:'):
                    value = _encrypt_value(self.value_key, value)
                
                self.mpax.set_proposal(self.sequence_number, value)
                
        self.pax_rep.send( json.dumps(dict(type='value_proposed')) )

        
    #--------------------------------------------------------------------------
    # Paxos Leadership Changes 
    #
    def _paxos_on_leadership_acquired(self):
        self.heartbeat_pulser.start( self.hb_proposer_klass.hb_period )
        self.onLeadershipAcquired()

        
    def _paxos_on_leadership_lost(self):
        if self.accept_retry and self.accept_retry.active():
            self.accept_retry.cancel()
            self.accept_retry = None
            
        if self.heartbeat_pulser.running:
            self.heartbeat_pulser.stop()

        self.onLeadershipLost()


    def _paxos_on_leadership_change(self, prev_leader_uid, new_leader_uid):
        if self.pax_rep is None:
            return # Ignore this if shutdown() has been called

        rep_addr = self.zpax_nodes[new_leader_uid][0] if new_leader_uid is not None else None
        
        self.proposal_advocate.leadership_changed( rep_addr )
        
        self.onLeadershipChanged(prev_leader_uid, new_leader_uid)


    #--------------------------------------------------------------------------
    # Paxos Proposal Resolution 
    #
    def _on_proposal_resolution(self, instance_num, value):
        self.last_seq_num = instance_num
        self.last_value   = value
        
        self.proposal_advocate.cancel_proposal( instance_num )
        
        if self.accept_retry is not None:
            self.accept_retry.cancel()
            self.accept_retry = None
            
        if self.value_key:
            value = _decrypt_value(self.value_key, value)

        self.onProposalResolution(instance_num, value)

                
    #--------------------------------------------------------------------------
    # Paxos Messaging
    #
    # Methods named '_SUB_<message_type>' are called when packets of that type
    # are recieved over the ZeroMQ Subscribe socket
    #
    def _publish(self, message_type, *parts, **kwargs):
        if not parts:
            parts = [{}]

        seq_num = self.sequence_number if not 'sequence_number' in kwargs else kwargs['sequence_number']
            
        parts[0]['type'    ] = message_type
        parts[0]['node_uid'] = self.node_uid
        parts[0]['seq_num' ] = seq_num
        
        msg_stack = [ 'zpax' ]

        jparts = [ json.dumps(p) for p in parts ]

        if self.hmac_key:
            h = hmac.new(self.hmac_key, digestmod=hashlib.sha1)
            for jp in jparts:
                h.update(jp)
            msg_stack.append( h.digest() )

        msg_stack.extend( jparts )
        
        self.pax_pub.send( msg_stack )
        self.pax_sub.messageReceived( msg_stack )

        
    def _SUB_paxos_heartbeat(self, header, pax):
        self.mpax.node.proposer.recv_heartbeat( tuple(pax[0]) )
        self.onHeartbeat( header )

    
    def _SUB_paxos_prepare(self, header, pax):
        if self.checkSequence(header):
            r = self.mpax.recv_prepare(header['seq_num'], tuple(pax[0]))
            if r:
                self._publish( 'paxos_promise', {}, r )

            
    def _SUB_paxos_promise(self, header, pax):
        if self.checkSequence(header):
            r = self.mpax.recv_promise(header['seq_num'],
                                       header['node_uid'],
                                       tuple(pax[0]),
                                       tuple(pax[1]) if pax[1] else None, pax[2])
            if r and r[1] is not None:
                self._paxos_send_accept( *r )
            

    def _SUB_paxos_accept(self, header, pax):
        if header['seq_num'] == self.last_seq_num:
            self._publish( 'value_accepted', dict(value=self.last_value), sequence_number=self.last_seq_num)
            return
        
        if self.checkSequence(header):
            r = self.mpax.recv_accept_request(header['seq_num'],
                                              tuple(pax[0]),
                                              pax[1])
            if r:
                self._publish( 'paxos_accepted', {}, r )
            else:
                self._publish( 'paxos_accepted_nack', dict( proposal_id = tuple(pax[0]),
                                                            new_proposal_id = self.mpax.node.acceptor.promised_id) )
        

    def _SUB_paxos_accepted(self, header, pax):
        if self.checkSequence(header):
            self.mpax.recv_accepted(header['seq_num'], header['node_uid'],
                                    tuple(pax[0]), pax[1])


    def _SUB_paxos_accepted_nack(self, header):
        if self.checkSequence(header):
            self.mpax.node.proposer.recv_accept_nack( header['node_uid'],
                                                      tuple(header['proposal_id']),
                                                      tuple(header['new_proposal_id']) )


    def _SUB_value_accepted(self, header):
        if self.checkSequence(header):
            self.setCurrentSequenceNumber(self.sequence_number + 1)
            self._on_proposal_resolution(header['seq_num'], header['value'])
            
            
    def _paxos_send_prepare(self, proposal_id):
        #
        # To reduce the potential for leadership battles, we'll introduce a
        # small, random delay before the prepare message as sent and recheck
        # the status of the leader prior to sending. If a leader has been
        # elected in this time, we'll cancel the send
        #
        def recheck_send():
            if not self.mpax.node.proposer.leader_is_alive():
                self._publish( 'paxos_prepare', {}, [proposal_id,] )

        w = self.mpax.node.proposer.liveness_window
        w = w - (w * 0.1)
        self.delayed_prepare = reactor.callLater( random.random() * w,
                                                  recheck_send )

        
    def _paxos_send_accept(self, proposal_id, proposal_value):
        outstanding_retry = self.accept_retry and self.accept_retry.active()
        
        if self.mpax.have_leadership() and not outstanding_retry:
            self._publish( 'paxos_accept', {}, [proposal_id, proposal_value] )

            retry_delay = self.mpax.node.proposer.hb_period
            
            self.accept_retry = reactor.callLater(retry_delay,
                                                  self._paxos_send_accept,
                                                  proposal_id,
                                                  proposal_value)
            

    def _paxos_send_heartbeat(self, leader_proposal_id):
        self._publish( 'paxos_heartbeat', self.getHeartbeatData(), [leader_proposal_id,] )

