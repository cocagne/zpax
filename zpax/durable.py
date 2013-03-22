'''
The Paxos algorithm requires state to be flushed to stable media prior to sending
certain messages over the network. This module provides an interface for doing so.

As these operations occur frequent and slow to complete, buffering must be used
to achieve any level of performance. The IDurableStateStore provides an interface
that uses deferreds with the get/set methods to abstract the backend implementation.

MemoryOnlyStateStore may be used in unit tests to avoid the performance penalty
associated with file I/O.
'''

from twisted.internet import defer



class IDurableStateStore(object):
    
    def set_state(self, data_id, new_state):
        '''
        Replaces the previous (if any) state associated with 'data_id' with the
        new state. If 'new_state' is None, the data_id is deleted from
        the store.

        Returns a deferred to the new data (or deletion flag) being written
        to disk. 
        '''

    def get_state(self, data_id):
        '''
        Returns a Deferred to the data associated with the id. Returns None if
        no state is associated with the data_id
        '''

    def flush(self):
        '''
        Flushes state to stable media. Returns a deferred that will fire once the
        data is at rest and all "set_state" deferreds have been fired.
        '''



class _DItem(object):
    __slots__ = ['data_id', 'data']
        
    def __init__(self, data_id, data):
        self.data_id  = data_id
        self.data     = data

        
class MemoryOnlyStateStore(object):

    def __init__(self):
        self.data       = dict()
        self.dflush     = dict() # maps data_id => Deferred
        self.auto_flush = True

        
    def set_state(self, data_id, new_state):
        di = self.data.get(data_id, None)
        
        if di:
            di.data = new_state
        else:
            self.data[ data_id ] = _DItem(data_id, new_state)

        if new_state is None:
            del self.data[ data_id ]
            
        dflush = self.dflush.get(data_id, None)
        
        if dflush is None:
            dflush = defer.Deferred()
            self.dflush[data_id] = dflush

            def onflush(_):
                del self.dflush[data_id]
                return _

            dflush.addCallback(onflush)
            

        if self.auto_flush:
            dflush.callback(None)
            
        return dflush

        
    def get_state(self, data_id):
        return defer.succeed(self.data[data_id].data if data_id in self.data else None)

    
    def flush(self):
        t = self.dflush
        self.dflush = dict()
        for d in t.itervalues():
            d.callback(None)
        return defer.succeed(None)

            
