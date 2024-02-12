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
