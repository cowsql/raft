.. _events:

:c:struct:`raft_event` --- External events
==========================================

Information about new events that should be passed to :c:func:`raft_step()`,

Data types
----------

.. c:enum:: raft_event_type

    Event type codes.

    .. code-block:: C

       enum raft_event_type {
           RAFT_START = 1,          /* Initial event starting loading persisted data */
           RAFT_RECEIVE,            /* A message has been received from another server */
           RAFT_PERSISTED_ENTRIES,  /* Some entries have been persisted to disk */
           RAFT_PERSISTED_SNAPSHOT, /* A snapshot has been persisted */
           RAFT_CONFIGURATION,      /* A new committed configuration must be applied */
           RAFT_SNAPSHOT,           /* A snapshot has been taken */
           RAFT_TIMEOUT,            /* The timeout has expired */
           RAFT_SUBMIT,             /* New entries have been submitted */
           RAFT_CATCH_UP,           /* Start catching-up a server */
           RAFT_TRANSFER            /* Start transferring leadership to another server */
       };

.. c:struct:: raft_event

    The :c:struct:`raft_event` struct holds information about events such as:

    - a new message has been received from another server
    - disk I/O has been completed for persisting data
    - new entries have been submitted for replication

    Users of the core :c:struct:`raft` struct are responsible for implementing
    an I/O layer that watches for the above events, filling :c:struct:`raft_event`
    objects as appropriate and passing them to the :c:func:`raft_step()`
    function.

    Each :c:enum:`raft_event_type` has an associated sub-struct, whose fields
    are described separately in the sections below.

    .. code-block:: C

       struct raft_event
       {
           raft_time time;            /* Must be filled with the current time */
           enum raft_event_type type; /* Must be filled with the type code of the event */
           unsigned char unused;
           unsigned short capacity;   /* Disk capacity that has been reserved */
           unsigned char reserved[4];
           union {                    /* Additional data about a specific event type */
               struct { ... } start;
               struct { ... } receive;
               struct { ... } persisted_entries;
               struct { ... } persisted_snapshot;
               struct { ... } configuration;
               struct { ... } snapshot;
               struct { ... } submit;
               struct { ... } catch_up;
               struct { ... } transfer;
           };
       };
       
Common fields
^^^^^^^^^^^^^

.. c:member:: raft_time raft_event.time

    Event timestamp. Must always be filled with the current time.

.. c:member:: enum raft_event_type raft_event.type

    Event type. Must be filled with the type code of the event.

.. c:member:: unsigned short raft_event.capacity

    Disk capacity that has been reserved and is guaranteed to be available.

Start
^^^^^

.. c:member:: struct @0 raft_event.start

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_START`.

    It contains all state persisted on disk by the server.

    .. code-block:: C

       struct
       {
            raft_term term;                          /* Current term */
            raft_id voted_for;                       /* Current vote */
            struct raft_snapshot_metadata *metadata; /* Last snapshot, if any */
            raft_index start_index;                  /* Index of first entry */
            struct raft_entry *entries;              /* Array of persisted entries */
            unsigned n_entries;                      /* Length of entries array */
       } start;

    The memory of the :c:struct:`raft_event.start.metadata` structure and of its
    nested fields all belongs to the caller of :c:func:`raft_step()`. The raft will
    make a copy of any data that it needs to retain, so the caller can free the
    :c:struct:`raft_event.start.metadata` structure after :c:func:`raft_step()`
    returns.

    Log entries passed via the :c:struct:`raft_event.start.entries` array should
    be loaded from disk at startup. They can be loaded in one or more batches
    (for example the persisted log entries could be distributed in multiple
    files, and each batch might contain all the entries of a single file).

    Each batch is expected to be a single contiguous region of allocated memory
    containing the data of all log entries in that batch.

    For example, if there are 7 log entries placed in two batches, the
    :c:struct:`raft_entry` items in the :c:struct:`raft_event.start.entries`
    array should look like:

    .. code-block:: C

       /* First batch */
       entries[0].batch = <mem-location-1>; /* Allocation for the first 4 entries */
       entries[0].buf.base = <mem-location-1> + <offset-1-relative-to-mem-location-1>;
       entries[1].batch = <mem-location-1>;
       entries[1].buf.base = <mem-location-1> + <offset-2-relative-to-mem-location-1>;
       entries[2].batch = <mem-location-1>;
       entries[2].buf.base = <mem-location-1> + <offset-3-relative-to-mem-location-1>;
       entries[3].batch = <mem-location-1>;
       entries[3].buf.base = <mem-location-1> + <offset-4-relative-to-mem-location-1>;
       /* Second batch */
       entries[4].batch = <mem-location-2>; /* Allocation for the last 3 entries */
       entries[4].buf.base = <mem-location-2> + <offset-1-relative-to-mem-location-2>;
       entries[5].batch = <mem-location-2>;
       entries[5].buf.base = <mem-location-2> + <offset-2-relative-to-mem-location-2>;
       entries[6].batch = <mem-location-2>;
       entries[6].buf.base = <mem-location-2> + <offset-2-relative-to-mem-location-2>;

    The batch pointer does not need to point to the start of the data of the
    first entry in the batch (for example it might hold some extra metadata),
    the only requirement is that free-ing it will release the memory of the
    entries in the batch.

    The memory of the :c:struct:`raft_event.start.entries` array and of all its
    batches belongs to the caller of :c:func:`raft_step()` and the raft library
    doesn't mantain any reference to them.

Receive
^^^^^^^

.. c:member:: struct @0 raft_event.receive

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_RECEIVE`.

    It contains the :c:struct:`raft_message` being received.

    .. code-block:: C

       struct
       {
           struct raft_message *message; /* Message being received */
       } receive;

    The memory of the :c:struct:`raft_event.receive.message` struct itself
    always belongs to the caller of :c:func:`raft_step()`.

    If the message is of type :c:enum:`RAFT_APPEND_ENTRIES`, the log entries
    passed via the :c:struct:`raft_event.receive.message.append_entries.entries`
    array should have been received over the network.

    **IMPORTANT**: The log entries of a :c:enum:`RAFT_APPEND_ENTRIES` message
    must all belong to a single batch (see the `Start event`_ documentation a
    description of what a batch is)

    The memory of the
    :c:struct:`raft_event.receive.message.append_entries.entries` array belongs
    to the caller and can be freed after :c:func:`raft_step()` returns.

    When :c:func:`raft_step()` succeeds and returns no error the memory
    ownership of the log entries batch data is transferred to raft and must not
    be freed by the caller. However if an error is returned then raft does not
    maintain any reference to the batch and ownership goes back to the caller.

    If the message is of type :c:enum:`RAFT_INSTALL_SNAPSHPOT`, the snapshot
    data passed via the
    :c:struct:`raft_event.receive.message.install_snapshot.data` field should
    have been received over the network.

    When :c:func:`raft_step()` succeeds and returns no error the memory
    ownership of the snapshot data is transferred to raft and must not be freed
    by the caller. However if an error is returned then raft does not maintain
    any reference to the snapshot data and ownership goes back to the caller.

    The memory of the
    :c:struct:`raft_event.receive.message.install_snapshot.conf` field always
    belongs to the caller and raft maintains no reference to it.


.. _Start event: ./events.html#start

Persisted entries
^^^^^^^^^^^^^^^^^

.. c:member:: struct @0 raft_event.persisted_entries

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_PERSISTED_ENTRIES`.

    It contains the latest log index that has been successfully persisted.

    .. code-block:: C

       struct
       {
           raft_index index; /* Highest index persisted */
       } persisted_entries;

Persisted snapshot
^^^^^^^^^^^^^^^^^^

.. c:member:: struct @0 raft_event.persisted_snapshot

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_PERSISTED_SNAPSHOT`.

    It contains metadata about the latest snapshot that has been successfully
    persisted.

    .. code-block:: C

       struct
       {
           struct raft_snapshot_metadata metadata;
           size_t offset;
           bool last;
       } persisted_snapshot;

Configuration
^^^^^^^^^^^^^

.. c:member:: struct @0 raft_event.configuration

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_CONFIGURATION`.

    It contains the last committed configuration that has been processed.

    .. code-block:: C

       struct
       {
           raft_index index;
           struct raft_configuration conf;
       } configuration;

Snapshot taken
^^^^^^^^^^^^^^

.. c:member:: struct @0 raft_event.snapshot

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_SNAPSHOT`.

    It contains metadata about the last snapshot that has been taken.

    .. code-block:: C

       struct
       {
           struct raft_snapshot_metadata metadata; /* Snapshot metadata */
           unsigned trailing;                      /* Trailing entries kept */
       } snapshot;

Submit
^^^^^^

.. c:member:: struct @0 raft_event.submit

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_SUBMIT`.

    It contains new entries that have been submitted for replication.

    .. code-block:: C

       struct
       {
           struct raft_entry *entries;
           unsigned n;
       } submit;

Catch-up server
^^^^^^^^^^^^^^^

.. c:member:: struct @0 raft_event.catch_up

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_CATCH_UP`.

    It contains the ID of a server that should be caught-up with the leader log.

    .. code-block:: C

       struct
       {
           raft_id server_id;
       } catch_up;

Transfer leadership
^^^^^^^^^^^^^^^^^^^

.. c:member:: struct @0 raft_event.transfer

    To be filled when :c:struct:`raft_event.type` is :c:enum:`RAFT_TRANSFER`.

    It contains the ID of a server that leadership should be transfered to.

    .. code-block:: C

       struct
       {
           raft_id server_id;
       } transfer;
