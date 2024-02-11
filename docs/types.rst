.. _types:

Basic types
===========

Basic types and utilities.

Data types
----------

.. c:type:: raft_id

   Hold the value of a Raft server ID. Guaranteed to be at least 64-bit long.

.. c:type:: raft_term

   Hold the value of a Raft term. Guaranteed to be at least 64-bit long.

.. c:type:: raft_index

   Hold the value of a raft entry index. Guaranteed to be at least 64-bit long.

.. c:type:: raft_time

    Hold a time value expressed in milliseconds since the epoch.

.. c:struct:: raft_buffer

    A data buffer.

    .. code-block:: C

       struct raft_buffer
       {
           void *base; /* Pointer to the buffer data */
           size_t len; /* Length of the buffer */
       };

.. c:enum:: raft_entry_type

    Log entry type codes.

    .. code-block:: C

       enum raft_entry_type {
           RAFT_COMMAND = 1, /* Command for the application FSM. */
           RAFT_BARRIER,     /* Wait for all previous commands to be applied. */
           RAFT_CHANGE       /* Raft configuration change. */
       };

.. c:struct:: raft_entry

    A single entry in the raft log.

    .. code-block:: C

       struct raft_entry
       {
           raft_term term;         /* Term in which the entry was created */
           unsigned short type;    /* Type (FSM command, barrier, config change) */
           struct raft_buffer buf; /* Entry data */
           void *batch;            /* Batch that buf's memory points to, if any */
       };
