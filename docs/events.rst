.. _events:

:c:struct:`raft_event` --- External events
==========================================

Data types
----------

.. c:struct:: raft_event

    Represents an external event (for example receiving an RPC message from another
    server). These events are what drive a :c:struct:`raft` object forward.
