import os
import random

from twisted.internet import defer, task, reactor

from paxos import heartbeat
from zpax import tzmq


class ProposalFailed(Exception):
    pass

class SequenceMismatch(ProposalFailed):

    def __init__(self, current):
        super(SequenceMismatch,self).__init__('Sequence Number Mismatch')
        self.current_seq_num = current

class ValueAlreadyProposed(ProposalFailed):
    
    def __init__(self):
        super(ValueAlreadyProposed,self).__init__('Value Already Proposed')


class ProposalAdvocate (object):
    '''
    Instances of this class ensure that the leader receives the
    proposed value.
    '''

    def __init__(self, mnode, retry_delay):
        '''
        retry_delay - Floating point delay in seconds between retry attempts
        '''
        self.mnode       = mnode
        self.retry_delay = retry_delay
        self.proposal    = None
        self.proposal_id = None
        self.retry_cb    = None

        
    def cancel(self):
        '''
        Called at:
           * Application shutdown
           * On acceptance of Accept! message
           * On instance resolution & prior to switching to the next instance
        '''
        if self.retry_cb and self.retry_cb.active():
            self.retry_cb.cancel()
            
        self.retry_cb    = None
        self.proposal    = None
        self.proposal_id = None

        
    def on_leadership_changed(self):
        self._send_proposal()
    

    def set_proposal(self, proposal_id, proposed_value):
        if self.proposal is None:
            self.proposal        = proposed_value
            self.proposal_id     = proposal_id

            self._send_proposal()

            
    def _send_proposal(self):
        if self.proposal is None:
            return
        
        if self.retry_cb and self.retry_cb.active():
            self.retry_cb.cancel()
            
        self.retry_cb = reactor.callLater(self.retry_delay,
                                          self._send_proposal)

        self.req.send_proposal_to_leader(self.instance_number, self.proposal_id, self.proposal)
