'''
This module wraps the pyzmq interface in a thin layer that integrates with
the Twisted reactor. Unlike txZMQ (upon which this code is based) no
attempt is made to change the API to conform with Twisted norms. The
goal of this module is simply to integrate ZeroMQ sockets as-is into the
Twisted reactor.
'''

from collections import deque

from zmq.core         import constants, error
from zmq.core.socket  import Socket
from zmq.core.context import Context

from zope.interface import implements

from twisted.internet.interfaces import IFileDescriptor, IReadDescriptor
from twisted.internet import reactor
from twisted.python   import log

from zmq.core.constants import PUB, SUB, REQ, REP, PUSH, PULL, ROUTER, DEALER, PAIR

# TODO: Support the Producer/Consumer interface

_context = None # ZmqContext singleton

POLL_IN_OUT = constants.POLLOUT & constants.POLLIN

def getContext():
    if _context is not None:
        return _context
    else:
        return _ZmqContext()

def _cleanup():
    global _context
    if _context:
        _context.shutdown()
    

class _ZmqContext(object):
    """
    This class wraps a ZeroMQ Context object and integrates it into the
    Twisted reactor framework.
    
    @cvar reactor: reference to Twisted reactor used by all the sockets

    @ivar sockets: set of instanciated L{ZmqSocket}s
    @type sockets: C{set}
    @ivar _zctx: ZeroMQ context
    @type _zctx: L{Context}
    """

    reactor = reactor

    def __init__(self, io_threads=1):
        """
        @param io_threads; Passed through to zmq.core.context.Context
        """
        global _context

        assert _context is None, 'Only one ZmqContext instance is permitted'
        
        self._sockets = set()
        self._zctx    = Context(io_threads)

        _context = self

        reactor.addSystemEventTrigger('during', 'shutdown', _cleanup)

    def shutdown(self):
        """
        Shutdown the ZeroMQ context and all associated sockets.
        """
        global _context

        if self._zctx is not None:
        
            for socket in self._sockets.copy():
                socket.close()

            self._sockets = None

            self._zctx.term()
            self._zctx = None

        _context = None
        



class ZmqSocket(object):
    """
    Wraps a ZeroMQ socket and integrates it into the Twisted reactor

    @ivar zsock: ZeroMQ Socket
    @type zsock: L{zmq.core.socket.Socket}
    @ivar queue: output message queue
    @type queue: C{deque}
    """
    implements(IReadDescriptor, IFileDescriptor)

    socketType = None
    
    def __init__(self, socketType=None):
        """
        @param context: Context this socket is to be associated with
        @type factory: L{ZmqFactory}
        @param socketType: Type of socket to create
        @type socketType: C{int}
        """
        if socketType is not None:
            self.socketType = socketType
            
        assert self.socketType is not None
        
        self._ctx   = getContext()
        self._zsock = Socket(getContext()._zctx, self.socketType)
        self._queue = deque()

        self.fd     = self._zsock.getsockopt(constants.FD)
        
        self._ctx._sockets.add(self)

        self._ctx.reactor.addReader(self)

        
    def _sockopt_property( i, totype=int):
        return property( lambda zs: zs._zsock.getsockopt(i),
                         lambda zs,v: zs._zsock.setsockopt(i,totype(v)) )

    
    linger     = _sockopt_property( constants.LINGER         )
    mcast_loop = _sockopt_property( constants.MCAST_LOOP     )
    rate       = _sockopt_property( constants.RATE           )
    hwm        = _sockopt_property( constants.HWM            )
    identity   = _sockopt_property( constants.IDENTITY,  str )
    subscribe  = _sockopt_property( constants.SUBSCRIBE, str )


    def close(self):
        self._ctx.reactor.removeReader(self)

        self._ctx._sockets.discard(self)

        self._zsock.close()
        
        self._zsock = None
        self._ctx   = None


    def __repr__(self):
        return "ZmqSocket(%s)" % repr(self._zsock)


    def fileno(self):
        """
        Part of L{IFileDescriptor}.

        @return: The platform-specified representation of a file descriptor
                 number.
        """
        return self.fd

    
    def connectionLost(self, reason):
        """
        Called when the connection was lost. This will only be called during
        reactor shutdown with active ZeroMQ sockets.

        Part of L{IFileDescriptor}.

        """
        if self._ctx:
            self._ctx.reactor.removeReader(self)

    
    def doRead(self):
        """
        Some data is available for reading on your descriptor.

        ZeroMQ is signalling that we should process some events,
        we're starting to send queued messages and to receive
        incoming messages.

        Note that the ZeroMQ FD is used in an edge-triggered manner.
        Consequently, this function must read all pending messages
        before returning.

        Part of L{IReadDescriptor}.
        """

        events = self._zsock.getsockopt(constants.EVENTS)

        #print 'doRead()', events
        
        while self._queue and (events & constants.POLLOUT) == constants.POLLOUT:
            try:
                self._zsock.send_multipart( self._queue[0], constants.NOBLOCK )
                self._queue.popleft()
                events = self._zsock.getsockopt(constants.EVENTS)
            except error.ZMQError as e:
                if e.errno == constants.EAGAIN:
                    break
                self._queue.popleft() # Failed to send, discard message
                raise e
        
        while (events & constants.POLLIN) == constants.POLLIN:
            if self._ctx is None:  # disconnected
                return
            
            try:
                msg_list = self._zsock.recv_multipart( constants.NOBLOCK )
            except error.ZMQError as e:
                if e.errno == constants.EAGAIN:
                    break
                raise e

            log.callWithLogger(self, self.messageReceived, msg_list)

            # Callback can cause the socket to be closed
            if self._zsock is not None:
                events = self._zsock.getsockopt(constants.EVENTS)

                
    def logPrefix(self):
        """
        Part of L{ILoggingContext}.

        @return: Prefix used during log formatting to indicate context.
        @rtype: C{str}
        """
        return 'ZMQ'

    
    def send(self, *message_parts):
        """
        Sends a ZeroMQ message. Each positional argument is converted into a message part
        """
        if len(message_parts) == 1 and isinstance(message_parts[0], (list, tuple)):
            message_parts = message_parts[0]
            
        if self._zsock.getsockopt(constants.EVENTS) & constants.POLLOUT == constants.POLLOUT:
            self._zsock.send_multipart( message_parts, constants.NOBLOCK )
        else:
            self._queue.append( message_parts )

        # The following call is requried to ensure that the socket's file descriptor
        # will signal new data as being available.
        self._zsock.getsockopt(constants.EVENTS)
        
        # 
        #if self._zsock.getsockopt(constants.EVENTS) & POLL_IN_OUT:
        #    self.doRead()


    def connect(self, addr):
        return self._zsock.connect(addr)

    
    def bind(self, addr):
        return self._zsock.bind(addr)

    
    def bindToRandomPort(self, addr, min_port=49152, max_port=65536, max_tries=100):
        return self._zsock.bind_to_random_port(addr, min_port, max_port, max_tries)

    
    def messageReceived(self, message_parts):
        """
        Called on incoming message from ZeroMQ.

        @param message_parts: list of message parts
        """
        raise NotImplementedError(self)


class ZmqPubSocket(ZmqSocket):
    socketType = PUB

class ZmqSubSocket(ZmqSocket):
    socketType = SUB

class ZmqReqSocket(ZmqSocket):
    socketType = REQ

class ZmqRepSocket(ZmqSocket):
    socketType = REP

class ZmqPushSocket(ZmqSocket):
    socketType = PUSH

class ZmqPullSocket(ZmqSocket):
    socketType = PULL

class ZmqRouterSocket(ZmqSocket):
    socketType = ROUTER

class ZmqDealerSocket(ZmqSocket):
    socketType = DEALER

class ZmqPairSocket(ZmqSocket):
    socketType = PAIR
