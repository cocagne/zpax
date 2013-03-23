zpax
====
Tom Cocagne &lt;tom.cocagne@gmail.com&gt;  


Overview
--------

zpax provides generic Python implementations of Multi-Paxos and Paxos-Commit
on top of Twisted and ZeroMQ.


Dependencies
------------

* [Plain Paxos](https://github.com/cocagne/paxos)
* [Twisted](http://twistedmatrix.com/trac/)
* [pyzmq](https://github.com/zeromq/pyzmq)


Examples
--------

### single_value.py

Multi-Paxos used to coordinate changes to a single, shared value.


### key_value.py

Multi-Paxos used to implement a simple, distributed Key-Value database. Of
particular note is that this implementation includes the ability to dynamically
add and remove nodes from the Paxos group.

