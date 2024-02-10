.. _updates:

:c:struct:`raft_update` --- State updates
=========================================

Data types
----------

.. c:struct:: raft_update

    Hold information about changes that user code must perform after a call to
    :c:func:`raft_step()` returns (e.g. new entries that must be persisted, new
    messages that must be sent, etc).
