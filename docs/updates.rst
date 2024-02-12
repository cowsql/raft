.. _updates:

:c:struct:`raft_update` --- State updates
=========================================

State changes or actions to handle after calling :c:func:`raft_step()`.

Data types
----------

.. c:struct:: raft_update

    The :c:struct:`raft_update` struct holds information about new state changes
    or actions that a user should handle after a call to :c:func:`raft_step()`,
    such as:

    - New data to persist on disk (e.g. new entries or snapshot)
    - New messages to send to other Raft servers
    - New term, vote, commit index, etc

    Users of the core :c:struct:`raft` struct are responsible for implementing
    I/O and application code that manages the above state updates.


    .. code-block:: C

       struct raft_update
       {
           unsigned flags;
           struct { ,.. } entries;
           struct { ,.. } snapshot;
           struct { ,.. } messages;
           struct
       };

Public members
^^^^^^^^^^^^^^

.. c:member:: unsigned raft_update.flags

    Bit flags that indicate which particular state change or action should be
    processed:

    .. code-block:: C

       #define RAFT_UPDATE_CURRENT_TERM 1 << 0
       #define RAFT_UPDATE_VOTED_FOR 1 << 1
       #define RAFT_UPDATE_ENTRIES 1 << 2
       #define RAFT_UPDATE_SNAPSHOT 1 << 3
       #define RAFT_UPDATE_MESSAGES 1 << 4
       #define RAFT_UPDATE_STATE 1 << 5
       #define RAFT_UPDATE_COMMIT_INDEX 1 << 6
       #define RAFT_UPDATE_TIMEOUT 1 << 7

Current Term
^^^^^^^^^^^^

.. c:macro:: RAFT_UPDATE_CURRENT_TERM

    If this bit flag is on, the current term of :c:struct:`raft` struct has
    changed and must be durably persisted to disk. This has to be done before
    processing any other change or action (i.e. no messages must be sent until
    the new term has been persisted).

    User code can use :c:func:`raft_current_term()` to get the new term that
    should be persisted.

Voted for
^^^^^^^^^

.. c:macro:: RAFT_UPDATE_VOTED_FOR

    If this bit flag is on, the server that the :c:struct:`raft` struct has
    voted for has changed and must be durably persisted to disk. This has to be
    done before processing any other change or action (i.e. no messages must be
    sent until the new vote has been persisted).

    User code can use :c:func:`raft_voted_for()` to get the server ID that
    should be persisted as new vote.

Entries
^^^^^^^

.. c:macro:: RAFT_UPDATE_ENTRIES

    If this bit flag is on, a new batch of log entries should be persisted to
    disk, as described by the :c:struct:`raft_update.entries` field.

.. c:member:: struct @0 raft_update.entries

    Details about new entries to persist.

    .. code-block:: C

       struct
       {
           raft_index index;         /* Index of first entry in the batch */
           struct raft_entry *batch; /* Array of entries to persist */
           unsigned n;               /* Number of entries in the array */
       } entries;

Snapshot
^^^^^^^^

.. c:macro:: RAFT_UPDATE_SNAPOSHOT

    If this bit flag is on, a new snapshot chunk should be persisted to disk, as
    described by the :c:struct:`raft_update.snapshot` field.

.. c:member:: struct @0 raft_update.snapshot

    Details about new entries to persist.

    .. code-block:: C

       struct
       {
           struct raft_snapshot_metadata metadata; /* Snapshot metadata */
           size_t offset;                          /* Chunk offset */
           struct raft_buffer chunk;               /* Data chunk */
           bool last;                              /* True if last chunk */
       } snapshot;

Messages
^^^^^^^^

.. c:macro:: RAFT_UPDATE_MESSAGES

    If this bit flag is on, new messages should be sent, as described by the
    :c:struct:`raft_update.messages` field.

.. c:member:: struct @0 raft_update.messages

    Details about new entries to persist.

    .. code-block:: C

       struct
       {
           struct raft_message *batch; /* Array of messages to send */
           unsigned n;                 /* Number of messages in the array */
       } messages;

State
^^^^^

.. c:macro:: RAFT_UPDATE_STATE

    If this bit flag is on, the :c:enum:`raft_state` of the :c:struct:`raft`
    struct has changed. The new state can be obtained with
    :c:func:`raft_state()`.

Commit index
^^^^^^^^^^^^

.. c:macro:: RAFT_UPDATE_COMMIT_INDEX

    If this bit flag is on, the commit index has changed. The new commit index
    can be obtained with :c:func:`raft_commit_index()`.

Timeout
^^^^^^^

.. c:macro:: RAFT_UPDATE_TIMEOUT

    If this bit flag is on, the time at which the next :c:macro:`RAFT_TIMEOUT`
    event should be fired has changed. The new time can be obtained with
    :c:func:`raft_timeout()`.

