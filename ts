#!/usr/bin/python

import zmq

from twisted.internet import reactor, defer

from zpax import tzmq


def straight():
    c = zmq.Context()
    s = c.socket(zmq.SUB)
    
    s.connect('tcp://localhost:5556')
    s.setsockopt(zmq.SUBSCRIBE, 'foo')
    
    while True:
        print s.recv()


def tz():

    class Sub (tzmq.ZmqSubSocket):

        def messageReceived(self, msg_parts):
            print 'Msg: ', msg_parts

    def doit():
        #s = Sub()
        def recv(msg_parts):
            print 'Recv: ', msg_parts
            
        s = tzmq.ZmqSubSocket()

        s.messageReceived = recv
        
        s.subscribe = 'foo'

        s.connect('tcp://localhost:5556')
        
    reactor.callWhenRunning(doit)
    reactor.run()

def tz2():

    def doit():
        def recv(msg_parts):
            print 'Recv: ', msg_parts
            
        s = tzmq.ZmqReqSocket()

        s.messageReceived = recv

        s.connect('tcp://localhost:5556')
        
        s.send('', 'foo')

        p = zmq.Poller()
        p.register(s._zsock, zmq.POLLIN)
        x = p.poll()
        print 'Poll done'
        print s._zsock.recv_multipart()

        
    reactor.callWhenRunning(doit)
    reactor.run()

tz2()

