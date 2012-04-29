from collections import deque

from zmq.core         import constants, error
from zmq.core.socket  import Socket
from zmq.core.context import Context

from zope.interface import implements

from twisted.internet.interfaces import IFileDescriptor, IReadDescriptor
from twisted.internet import reactor
from twisted.python   import log

# TODO: Support the Producer/Consumer interface

_context = None # ZmqContext singleton

def getContext():
    if _context is not None:
        return _context
    else:
        return ZmqContext()
    

class ZmqContext(object):
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

    def shutdown(self):
        """
        Shutdown the ZeroMQ context and all associated sockets.
        """
        global _context

        if self._zctx is not None:
        
            for socket in self._sockets.copy():
                socket.shutdown()

            self._sockets = None

            self._zctx.term()
            self._zctx = None

        _context = None

    def registerForShutdown(self):
        """
        Register factory to be automatically shut down
        on reactor shutdown.
        """
        reactor.addSystemEventTrigger('during', 'shutdown', self.shutdown)



class ZmqSocket(object):
    """
    Wraps a ZeroMQ socket and integrates it into the Twisted reactor

    @ivar zsock: ZeroMQ Socket
    @type zsock: L{zmq.core.socket.Socket}
    @ivar queue: output message queue
    @type queue: C{deque}
    """
    implements(IReadDescriptor, IFileDescriptor)

    socket_type = None
    
    def __init__(self):
        """
        @param context: Context this socket is to be associated with
        @type factory: L{ZmqFactory}
        @param socket_type: Type of socket to create
        @type socket_type: C{int}
        """
        assert self.socket_type is not None
        
        self._ctx   = getContext()
        self._zsock = Socket(context._zctx, self.socket_type)
        self._queue = deque()

        self.fd     = self._zsock.getsockopt(constants.FD)
        
        self._ctx._sockets.add(self)

        self._ctx.reactor.addReader(self)

        
    def _sockopt_property( i, totype=int):
        return property( lambda zs: zs._zsock.getsockopt(i),
                         lambda zs,v: zs._zsock.setsockopt(i,totype(v)) )

    
    linger     = _sockopt_property( constants.LINGER        )
    mcast_loop = _sockopt_property( constatns.MCAST_LOOP    )
    rate       = _sockopt_property( constatns.RATE          )
    hwm        = _sockopt_property( constatns.HWM           )
    identity   = _sockopt_property( constatns.IDENTITY, str )


    def shutdown(self):
        """
        Shutdown the socket.
        """
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
        Called when the connection was lost.

        Part of L{IFileDescriptor}.

        This is called when the connection on a selectable object has been
        lost.  It will be called whether the connection was closed explicitly,
        an exception occurred in an event handler, or the other end of the
        connection closed it first.

        @param reason: A failure instance indicating the reason why the
                       connection was lost.  L{error.ConnectionLost} and
                       L{error.ConnectionDone} are of special note, but the
                       failure may be of other classes as well.
        """
        log.err(reason, "Connection to ZeroMQ lost in %r" % (self))
        
        if self._ctx:
            self._ctx.reactor.removeReader(self)

    
    def doRead(self):
        """
        Some data is available for reading on your descriptor.

        ZeroMQ is signalling that we should process some events,
        we're starting to send queued messages and to receive
        incoming messages.

        Part of L{IReadDescriptor}.
        """

        events = self._zsock.getsockopt(constants.EVENTS)
        
        if (events & constants.POLLOUT) == constants.POLLOUT:
            while self._queue:
                try:
                    self._zsock.send_multipart( self._queue[0], constants.NOBLOCK )
                    self._queue.popleft()
                except error.ZMQError as e:
                    if e.errno == constants.EAGAIN:
                        break
                    self._queue.popleft() # Failed to send, discard message
                    raise e

                
        if (events & constants.POLLIN) == constants.POLLIN:
            while True:
                if self._ctx is None:  # disconnected
                    return
                try:
                    msg_list = self._zsock.recv_multipart( constants.NOBLOCK )
                except error.ZMQError as e:
                    if e.errno == constants.EAGAIN:
                        break
                    raise e

                log.callWithLogger(self, self.messageReceived, msg_list)

                
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
        if self._zsock.getsockopt(constants.EVENTS) & constants.POLLOUT == constants.POLLOUT:
            self._zsock.send_multipart( message_parts, constants.NOBLOCK )
        else:
            self._queue.append( message_parts )


    def connect(self, addr):
        return self._zsock.connect(addr)

    
    def bind(self, addr):
        return self._zsock.bind(addr)

    
    def bind_to_random_port(self, addr, min_port=49152, max_port=65536, max_tries=100):
        return self._zsock.bind_to_random_port(addr, min_port, max_port, max_tries)

    
    def close(self):
        self._zsock.close()

        
    def messageReceived(self, message_parts):
        """
        Called on incoming message from ZeroMQ.

        @param message_parts: list of message parts
        """
        raise NotImplementedError(self)

