.. _messages:

:c:struct:`raft_message` --- RPC messages
=========================================

The :c:struct:`raft_message` struct holds information about a single RPC message
being received or sent over the network.

Data types
----------

.. c:enum:: raft_message_type

    RPC message type codes.

    .. code-block:: C

       enum raft_event_type {
           RAFT_APPEND_ENTRIES = 1,
           RAFT_APPEND_ENTRIES_RESULT,
           RAFT_REQUEST_VOTE,
           RAFT_REQUEST_VOTE_RESULT,
           RAFT_INSTALL_SNAPSHOT,
           RAFT_TIMEOUT_NOW
       };

.. c:struct:: raft_message

    Union of all RPC message structs, plus information about the sender or the
    receiver (depending on whether the message is being sent or received).

    .. code-block:: C

       struct raft_message
       {
           enum raft_message_type type; /* RPC type code */
           raft_id server_id;           /* ID of sending or destination server */
           const char *server_address;  /* Address of sending or destination server */
           union {                      /* Type-specific data */
               struct raft_request_vote request_vote;
               struct raft_request_vote_result request_vote_result;
               struct raft_append_entries append_entries;
               struct raft_append_entries_result append_entries_result;
               struct raft_install_snapshot install_snapshot;
               struct raft_timeout_now timeout_now;
           };
       };

RequestVote
^^^^^^^^^^^

.. c:struct:: raft_request_vote

    Holds the parameters of a `RequestVote` RPC message.

    .. code-block:: C

       struct raft_request_vote
       {
           unsigned char version;     /* Message format version. */
           raft_term term;            /* Candidate's term */
           raft_id candidate_id;      /* ID of the server requesting the vote */
           raft_index last_log_index; /* Index of candidate's last log entry */
           raft_index last_log_term;  /* Term of log entry at last_log_index */
           bool disrupt_leader;       /* True if current leader should be discarded */
           bool pre_vote;             /* True if this is a pre-vote request */
       };

RequestVote result
^^^^^^^^^^^^^^^^^^

.. c:struct:: raft_request_vote_result

    Holds the parameters of a `RequestVote` RPC result message.

    .. code-block:: C

       struct raft_append_entries_result
       {
           unsigned char version;     /* Message format version */
           raft_term term;            /* Receiver's current_term */
           raft_index rejected;       /* If non-zero, the index that was rejected */
           raft_index last_log_index; /* Receiver's last log entry index, as hint */
           unsigned short features;   /* Feature flags (since version 1) */
           unsigned short capacity;   /* Reserved disk capacity for log entries */
       };

AppendEntries
^^^^^^^^^^^^^
.. c:struct:: raft_append_entries

    Holds the parameters of an `AppendEntries` RPC request message.

    .. code-block:: C

       struct raft_append_entries
       {
           unsigned char version;      /* Message format version */
           raft_term term;             /* Leader's term */
           raft_index prev_log_index;  /* Index of log entry preceeding new ones */
           raft_term prev_log_term;    /* Term of entry at prev_log_index */
           raft_index leader_commit;   /* Leader's commit index */
           struct raft_entry *entries; /* Log entries to append */
           unsigned n_entries;         /* Size of the log entries array */
       };

AppendEntries result
^^^^^^^^^^^^^^^^^^^^

.. c:struct:: raft_append_entries_result

    Holds the parameters of an `AppendEntries` RPC result message.

    .. code-block:: C

       struct raft_append_entries_result
       {
           unsigned char version;     /* Message format version */
           raft_term term;            /* Receiver's current_term */
           raft_index rejected;       /* If non-zero, the index that was rejected */
           raft_index last_log_index; /* Receiver's last log entry index, as hint */
           unsigned short features;   /* Feature flags (since version 1) */
           unsigned short capacity;   /* Reserved disk capacity for log entries */
       };

InstallSnapshot
^^^^^^^^^^^^^^^

.. c:struct:: raft_install_snapshot

    Holds the parameters of an `InstallSnapshot` RPC request message.

    .. code-block:: C

       struct raft_install_snapshot
       {
           unsigned char version;          /* Message format version */
           raft_term term;                 /* Leader's term */
           raft_index last_index;          /* Index of last entry in the snapshot */
           raft_term last_term;            /* Term of last_index */
           struct raft_configuration conf; /* Config as of last_index */
           raft_index conf_index;          /* Commit index of conf */
           struct raft_buffer data;        /* Raw snapshot data */
       };

TimeoutNow
^^^^^^^^^^

.. c:struct:: raft_timeout_now

    Holds the parameters of a `TimeoutNow` RPC request message.

    .. code-block:: C

       struct raft_timeout_now
       {
           unsigned char version;     /* Message format version */
           raft_term term;            /* Leader's term */
           raft_index last_log_index; /* Index of leader's last log entry */
           raft_index last_log_term;   /* Term of log entry at last_log_index */
       };
