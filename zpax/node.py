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



def encrypt_value( key, value ):
    if AES is None:
        raise Exception('Missing Pycrypto module. Encryption is not supported')
    
    iv = os.urandom(AES.block_size)

    c = AES.new(key, AES.MODE_CBC, iv)

    # Trailing spaces are legal in JSON so we'll use that for padding
    if len(value) % c.block_size != 0:
        value += ' ' * (c.block_size - len(value)%c.block_size)

    return 'ENC:' + base64.b64encode(iv + c.encrypt(value))


def decrypt_value( key, cipher_txt ):
    if AES is None:
        raise Exception('Missing Pycrypto module. Encryption is not supported')

    assert cipher_txt.startswith('ENC:')

    cipher_txt = base64.b64decode(cipher_txt[4:])

    iv = cipher_txt[:AES.block_size]
    cv = cipher_txt[AES.block_size:]

    c = AES.new(key, AES.MODE_CBC, iv)
    
    return c.decrypt(cv).rstrip()


class BasicHeartbeatProposer (heartbeat.Proposer):
    hb_period       = 0.5
    liveness_window = 1.5

    def __init__(self, basic_node, node_uid, quorum_size, leader_uid):
        self.node = basic_node

        super(BasicHeartbeatProposer, self).__init__(node_uid,
                                                     quorum_size,
                                                     leader_uid = leader_uid)

    def __getstate__(self):
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

    node_factory     = None
    on_resolution_cb = None

    def __getstate__(self):
        d = dict( self.__dict__ )
        del d['node_factory']
        del d['on_resolution_cb']
        return d
                
    def on_proposal_resolution(self, instance_num, value):
        self.on_resolution_cb(instance_num, value)
    


class JSONResponder (object):
    '''
    Mixin class providing a simple mechanism for dispatching message handling
    functions. 
    '''
    def _generateResponder(self, prefix, parts_transform = lambda x : x):
        '''
        Returns a message dispatching function suitable for assignment to a
        ZmqSocket instance. The message parts must be in JSON format and the
        first message part must include a "type" field that names the message
        type (as a string). The returned function will call a member function
        of the same name with the supplied prefix argument.
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
    This class is responsible for ensuring that the current leader is
    aware of our proposal if we have one. Whenever leadership changes,
    the proposal is sent to the leader.
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
        #print 'CONNECT REQ: ', self.node_uid, self.current_leader_uid, self.current_leader_uid in self.zpax_nodes
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
    This class provides the basic functionality required for Multi-Paxos
    over ZeroMQ Publish/Subscribe sockets. This class follows the GoF95
    template design pattern and delegates all application level logic to
    a subclass.

    This class uses two paris of sockets. One is the Publish/Subscribe pair
    for sending Paxos protocol messages. The second is a Req/Rep pair used
    for communication with the current leader. As the leader is the one that
    must propose the value, the proposeValue() method uses the Req socket to
    forward the request to the current leader's Rep socket. This is infinitely
    retried until an acknoledgement is received from the leader. When leadership
    changes and the proposed value is still outstanding, the proposal is sent
    to the new leader and infinitely retried until an acknowledgement is
    received.
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
        self.pax_req            = None # Assigned on leadership change
        self.pax_pub            = None
        self.pax_sub            = None

        self.mpax                    = BasicMultiPaxos(durable_dir, object_id)
        self.mpax.node_factory       = self._node_factory
        self.mpax.on_resolution_cb   = self._on_proposal_resolution

        if self.mpax.node:
            self.mpax.node.proposer.node = self
            
            if self.mpax.node.proposer.value:
                # If recovered node has a proposed value, give this to the proposal
                # advocate
                self.proposal_advocate.set_proposal( self.node_uid,
                                                     self.sequence_number,
                                                     self.mpax.node.proposer.value )

        self.heartbeat_poller = task.LoopingCall( self._poll_heartbeat         )
        self.heartbeat_pulser = task.LoopingCall( self._pulse_leader_heartbeat )

        # Optional Message Authentication & Encryption attributes
        self.hmac_key  = None
        self.value_key = None

        
    quorum_size     = property( lambda self: self.mpax.quorum_size  )
    sequence_number = property( lambda self: self.mpax.instance_num )

    
    def is_initialized(self):
        return self.mpax.quorum_size is not None

    
    def initialize(self, quorum_size):
        assert not self.is_initialized(), 'MultiPaxos instance already initialized'
        
        self.mpax.initialize( self.node_uid, quorum_size )

        if self.pax_sub is not None:
            self.heartbeat_poller.start( self.hb_proposer_klass.liveness_window )


    def connect(self, zpax_nodes):
        # Dictionary of node_uid -> (rep_addr, pub_addr)
        
        if self.zpax_nodes == zpax_nodes:
            return # already connected

        if not self.node_uid in zpax_nodes:
            raise Exception('Missing local node configuration')

        if self.zpax_nodes is None:
            self.zpax_nodes = zpax_nodes

        if not self.is_initialized():
            # If no explicit quorum size has been set. Default to the minimum
            self.initialize( len(zpax_nodes)/2 + 1 )
                    
        def local_addr_changed(i):
            return self.zpax_nodes[self.node_uid][i] != zpax_nodes[self.node_uid][i]
        
        if self.pax_rep is None or local_addr_changed(0):
            if self.pax_rep:
                self.pax_rep.close()
            self.pax_rep = tzmq.ZmqRepSocket()
            self.pax_rep.bind(zpax_nodes[self.node_uid][0])
            self.pax_rep.messageReceived = self._generateResponder('_REP_')

        if self.pax_pub is None or local_addr_changed(1):
            if self.pax_pub:
                self.pax_pub.close()
            self.pax_pub = tzmq.ZmqPubSocket()
            self.pax_pub.bind(zpax_nodes[self.node_uid][1])

        prev_remote = set( t[1] for t in self.zpax_nodes.itervalues() )
        cur_remote  = set( t[1] for t in zpax_nodes.itervalues()      )

        if self.pax_sub is None or prev_remote != cur_remote:
            if self.pax_sub is not None:
                self.pax_sub.close()

            self.pax_sub = tzmq.ZmqSubSocket()

            self.pax_sub.subscribe       = 'zpax'        
            self.pax_sub.messageReceived = self._generateResponder('_SUB_', self._check_hmac)

            for x in cur_remote:
                self.pax_sub.connect(x)

        if self.mpax.quorum_size is not None and not self.heartbeat_poller.running:
            self.heartbeat_poller.start( self.hb_proposer_klass.liveness_window )

        self.zpax_nodes = zpax_nodes


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
    # Subclass API
    #
    def onLeadershipAcquired(self):
        '''
        Called when this node acquires Paxos leadership.

        Note, this is not reliable as a unique distiction between
        nodes. Multiple nodes may simultaneously believe themselves to be the
        leader. Also, it is possible for another leader to have been elected
        and several proposals resolved before this method is even called. Be
        wary of using this method for anything beyond starting a timer for
        sending heartbeats.
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

    def onBehindInSequence(self):
        '''
        Called when this node's sequence number is behind the most recently observed
        value.
        '''

    def onProposalResolution(self, instance_num, value):
        '''
        Called when an instance of the Paxos algorithm agrees on a value.
        '''

    def onHeartbeat(self, data):
        '''
        data - Dictionary of key=value paris in the heartbeat message
        '''

    def onShutdown(self):
        '''
        Called immediately before shutting down
        '''

    def getHeartbeatData(self):
        '''
        Returns a dictionary of key=value parameters to be included
        in the heartbeat message
        '''
        return {}

    
    def currentInstanceNum(self):
        return self.mpax.instance_num

    
    def slewSequenceNumber(self, new_sequence_number):
        assert new_sequence_number > self.sequence_number

        self.proposal_advocate.cancel_proposal( new_sequence_number - 1 )
        
        if self.mpax.node.proposer.leader:
            self._paxos_on_leadership_lost()
            
        self.mpax.set_instance_number(new_sequence_number)

        
    def proposeValue(self, value, sequence_number=None):
        if sequence_number is not None and not sequence_number == self.sequence_number:
            raise SequenceMismatch( self.sequence_number )

        if self.mpax.node.proposer.value is not None:
            raise ValueAlreadyProposed()

        if self.mpax.node.acceptor.accepted_value is not None:
            raise ValueAlreadyProposed()

        if self.value_key:
            value = encrypt_value( self.value_key, value )

        self.proposal_advocate.set_proposal(self.node_uid,
                                            self.sequence_number,
                                            value)


    def publish(self, message_type, *parts, **kwargs):
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


    def shutdown(self):
        self.onShutdown()
        self.proposal_advocate.shutdown()
        if self.accept_retry is not None and self.accept_retry.active():
            self.accept_retry.cancel()
        if self.delayed_prepare is not None and self.delayed_prepare.active():
            self.delayed_prepare.cancel()
        self.pax_rep.close()
        self.pax_rep = None
        self.pax_pub.close()
        self.pax_sub.close()
        if self.heartbeat_poller.running:
            self.heartbeat_poller.stop()
        if self.heartbeat_pulser.running:
            self.heartbeat_pulser.stop()


    def checkSequence(self, header):
        '''
        Checks the sequence number of the header. If our sequence number is behind,
        it calls onBehindInSequence().

        The return value is True if the header's squence number matches our own and
        False otherwise. If False is returned, the message will be considered invalid
        and will not be processed. Overloading this method and artificially returning
        False may be used by subclasses to temporarily disable participation in
        Paxos messaging.
        '''
        seq = header['seq_num']
        
        if seq > self.sequence_number:
            self.onBehindInSequence()
            
        return seq == self.sequence_number


    def changeQuorumSize(self, new_quorum_size):
        '''
        Changes the quorum size to the new value. Note that this should only be
        done by all nodes at approximately the same time to ensure consistency.
        A simple way to ensure correctness is to use the Paxos algorithm itself
        to choose the new quorum size (along with the corresponding configuration
        information for the added and or removed nodes)
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


    #--------------------------------------------------------------------------
    # Heartbeats 
    #
    def _poll_heartbeat(self):
        self.mpax.node.proposer.poll_liveness()

        
    def _pulse_leader_heartbeat(self):
        self.mpax.node.proposer.pulse()

        
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
            value = decrypt_value(self.value_key, value)

        self.onProposalResolution(instance_num, value)
        
    #--------------------------------------------------------------------------
    # Paxos Messaging 
    #
    def _SUB_paxos_heartbeat(self, header, pax):
        self.mpax.node.proposer.recv_heartbeat( tuple(pax[0]) )
        self.onHeartbeat( header )

    
    def _SUB_paxos_prepare(self, header, pax):
        #print self.node_uid, 'got prepare', header, pax
        if self.checkSequence(header):
            r = self.mpax.recv_prepare(header['seq_num'], tuple(pax[0]))
            if r:
                #print 'SND %s:  ' % self.node_uid[-5], (r[0][0], str(r[0][1][-5]))
                self.publish( 'paxos_promise', {}, r )

            
    def _SUB_paxos_promise(self, header, pax):
        #print 'RCV %s:' % self.node_uid[-5], header['node_uid'][-5], (pax[0][0], str(pax[0][1][-5]))
        if self.checkSequence(header):
            r = self.mpax.recv_promise(header['seq_num'],
                                       header['node_uid'],
                                       tuple(pax[0]),
                                       tuple(pax[1]) if pax[1] else None, pax[2])
            if r and r[1] is not None:
                #print self.node_uid, 'sending accept', r
                self._paxos_send_accept( *r )
            

    def _SUB_paxos_accept(self, header, pax):
        #print 'Got Accept(%s)!' % self.node_uid, (self.sequence_number, header['seq_num']), header['node_uid'], pax[0]
        if header['seq_num'] == self.last_seq_num:
            self.publish( 'value_accepted', dict(value=self.last_value), sequence_number=self.last_seq_num)
            return
        
        if self.checkSequence(header):
            r = self.mpax.recv_accept_request(header['seq_num'],
                                              tuple(pax[0]),
                                              pax[1])
            if r:
                self.publish( 'paxos_accepted', {}, r )
            else:
                self.publish( 'paxos_accepted_nack', dict( proposal_id = tuple(pax[0]),
                                                           new_proposal_id = self.mpax.node.acceptor.promised_id) )
        

    def _SUB_paxos_accepted(self, header, pax):
        #print 'Got accepted(%s)' % self.node_uid, (self.sequence_number, header['seq_num']), header['node_uid']#, pax[0]
        if self.checkSequence(header):
            self.mpax.recv_accepted(header['seq_num'], header['node_uid'],
                                    tuple(pax[0]), pax[1])


    def _SUB_paxos_accepted_nack(self, header):
        if self.checkSequence(header):
            self.mpax.node.proposer.recv_accept_nack( header['node_uid'],
                                                      tuple(header['proposal_id']),
                                                      tuple(header['new_proposal_id']) )


    def _SUB_value_accepted(self, header):
        #print 'RECV VAL ACCEPTED: ', self.node_uid
        if self.checkSequence(header):
            #print '      TIS GOOD', header['value']
            self.slewSequenceNumber(self.sequence_number + 1)
            self._on_proposal_resolution(header['seq_num'], header['value'])
            
            
        

    def _paxos_send_prepare(self, proposal_id):
        #print self.node_uid, 'sending prepare: ', proposal_id
        
        # Add a random delay before sending this message and recheck to see if a
        # leader is active. This should reduce battles for supremacy.
        def recheck_send():
            if not self.mpax.node.proposer.leader_is_alive():
                #print '### THERE CAN BE ONLY ONE! ###'
                self.publish( 'paxos_prepare', {}, [proposal_id,] )

        w = self.mpax.node.proposer.liveness_window
        w = w - (w * 0.1)
        self.delayed_prepare = reactor.callLater( random.random() * w,
                                                  recheck_send )

        
    def _paxos_send_accept(self, proposal_id, proposal_value):
        if self.mpax.have_leadership() and (
            self.accept_retry is None or not self.accept_retry.active()
            ):
            #print 'Sending accept', self.node_uid, self.mpax.have_leadership()
            self.publish( 'paxos_accept', {}, [proposal_id, proposal_value] )

            retry_delay = self.mpax.node.proposer.hb_period
            
            self.accept_retry = reactor.callLater(retry_delay,
                                                  self._paxos_send_accept,
                                                  proposal_id,
                                                  proposal_value)
            

    def _paxos_send_heartbeat(self, leader_proposal_id):
        self.publish( 'paxos_heartbeat', self.getHeartbeatData(), [leader_proposal_id,] )


    #--------------------------------------------------------------------------
    # Reply Messaging 
    #   
    def _REP_propose_value(self, header):
        #print 'Proposal made (%s)'% self.node_uid, 'Seq = ', self.sequence_number, 'Req: ', header
        if header['seq_num'] == self.sequence_number:
            if self.mpax.node.acceptor.accepted_value is None:
                value = header['value']
                if self.value_key and not value.startswith('ENC:'):
                    value = encrypt_value(self.value_key, value)
                #print 'Setting proposal'
                self.mpax.set_proposal(self.sequence_number, value)
                
        self.pax_rep.send( json.dumps(dict(type='value_proposed')) )

