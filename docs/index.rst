C-Raft
======

Production grade asynchronous C implementation of the Raft consensus protocol.

Design
------

The library a has modular design: its core part implements only the core Raft
algorithm logic (no I/O and no system calls). On top of that, various drivers
are provided that implement actual network communication and persistent data
storage.

The core part of the library is designed to work well with asynchronous or
non-blocking I/O engines (such as `libuv`_ and `io_uring`_), although it can be
used in threaded or blocking contexts as well.

.. _libuv: http://libuv.org
.. _io_uring: https://en.wikipedia.org/wiki/Io_uring

Features
--------

C-Raft implements all the basic features described in the Raft dissertation:

* Leader election
* Log replication
* Log compaction
* Membership changes

It also includes a few optional enhancements:

* Optimistic pipelining to reduce log replication latency
* Writing to the leader's disk in parallel
* Automatic stepping down when the leader loses quorum
* Leadership transfer extension
* Non-voting servers

Source
------

The source tree is available on `github`_.

.. _github: https://github.com/cowsql/raft

Licence
-------

This raft C library is released under a slightly modified version of LGPLv3,
that includes a copyright exception letting users to statically link the library
code in their project and release the final work under their own terms. See the
full `license`_ text.

.. _license: https://github.com/cowsql/raft/blob/main/LICENSE

.. toctree::
   :hidden:
   :maxdepth: 1

   quick-start
   core
   disk-format
