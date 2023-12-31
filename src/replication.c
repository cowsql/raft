#include <limits.h>
#include <string.h>

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#include "legacy.h"
#ifdef __GLIBC__
#include "error.h"
#endif
#include "err.h"
#include "flags.h"
#include "heap.h"
#include "log.h"
#include "membership.h"
#include "message.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "restore.h"
#include "tracing.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

/* Maximum number of entries that will be included in an AppendEntries
 * message.
 *
 * TODO: Make this number configurable. */
#define MAX_APPEND_ENTRIES 32

/* Context of a RAFT_IO_APPEND_ENTRIES request that was submitted with
 * raft_io_>send(). */
struct sendAppendEntries
{
    struct raft *raft;          /* Instance sending the entries. */
    struct raft_io_send send;   /* Underlying I/O send request. */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
    raft_id server_id;          /* Destination server. */
};

/* Callback invoked after request to send an AppendEntries RPC has completed. */
int replicationSendAppendEntriesDone(struct raft *r,
                                     struct raft_message *message,
                                     int status)
{
    struct raft_append_entries *args = &message->append_entries;
    unsigned i = configurationIndexOf(&r->configuration, message->server_id);

    if (r->state == RAFT_LEADER && i < r->configuration.n) {
        if (status != 0) {
            tracef("failed to send append entries to server %llu: %s",
                   message->server_id, raft_strerror(status));
            /* Go back to probe mode. */
            if (progressState(r, i) != PROGRESS__PROBE) {
                progressToProbe(r, i);
            }
        }
    }

    /* Tell the log that we're done referencing these entries. */
    logRelease(r->log, args->prev_log_index + 1, args->entries,
               args->n_entries);

    return 0;
}

/* Send an AppendEntries message to the i'th server, including all log entries
 * from the given point onwards. */
static int sendAppendEntries(struct raft *r,
                             const unsigned i,
                             const raft_index prev_index,
                             const raft_term prev_term,
                             int max)
{
    struct raft_server *server = &r->configuration.servers[i];
    struct raft_message message;
    struct raft_append_entries *args = &message.append_entries;
    raft_index next_index = prev_index + 1;
    int rv;

    args->term = r->current_term;
    args->prev_log_index = prev_index;
    args->prev_log_term = prev_term;

    /* TODO: implement a limit to the total *size* of the entries being sent,
     * not only the number. Also, make the number and the size configurable. */
    rv = logAcquireAtMost(r->log, next_index, max, &args->entries,
                          &args->n_entries);
    if (rv != 0) {
        goto err;
    }

    /* From Section 3.5:
     *
     *   The leader keeps track of the highest index it knows to be committed,
     *   and it includes that index in future AppendEntries RPCs (including
     *   heartbeats) so that the other servers eventually find out. Once a
     *   follower learns that a log entry is committed, it applies the entry to
     *   its local state machine (in log order)
     */
    args->leader_commit = r->commit_index;

    if (args->n_entries == 0) {
        infof("%s server %llu sending a heartbeat (no entries)",
              progressStateName(r, i), server->id);
    } else if (args->n_entries == 1) {
        infof("%s server %llu sending 1 entry (%llu^%llu)",
              progressStateName(r, i), server->id, next_index,
              args->entries[0].term);
    } else {
        infof("%s server %llu sending %u entries (%llu^%llu..%llu^%llu)",
              progressStateName(r, i), server->id, args->n_entries, next_index,
              args->entries[0].term, next_index + args->n_entries - 1,
              args->entries[args->n_entries - 1].term);
    }

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.server_id = server->id;
    message.server_address = server->address;

    rv = MessageEnqueue(r, &message);
    if (rv != 0) {
        goto err_after_entries_acquired;
    }

    if (progressState(r, i) == PROGRESS__PIPELINE) {
        /* Optimistically update progress. */
        progressSetNextIndex(r, i, args->prev_log_index + 1 + args->n_entries);
    }

    progressUpdateLastSend(r, i);
    return 0;

err_after_entries_acquired:
    logRelease(r->log, next_index, args->entries, args->n_entries);
err:
    assert(rv != 0);
    return rv;
}

/* Context of a RAFT_IO_INSTALL_SNAPSHOT request that was submitted with
 * raft_io_>send(). */
struct sendInstallSnapshot
{
    struct raft *raft;               /* Instance sending the snapshot. */
    struct raft_io_snapshot_get get; /* Snapshot get request. */
    struct raft_io_send send;        /* Underlying I/O send request. */
    raft_id server_id;               /* Destination server. */
};

int replicationSendInstallSnapshotDone(struct raft *r,
                                       struct raft_message *message,
                                       int status)
{
    const struct raft_server *server;

    server = configurationGet(&r->configuration, message->server_id);

    if (status != 0) {
        tracef("send install snapshot: %s", raft_strerror(status));
        if (r->state == RAFT_LEADER && server != NULL) {
            unsigned i;
            i = configurationIndexOf(&r->configuration, message->server_id);
            progressAbortSnapshot(r, i);
        }
    }

    configurationClose(&message->install_snapshot.conf);

    return 0;
}

/* Send the latest snapshot to the i'th server */
static int sendSnapshot(struct raft *r, const unsigned i)
{
    struct raft_message message;
    struct raft_install_snapshot *args = &message.install_snapshot;
    struct raft_server *server;
    int rv;

    progressToSnapshot(r, i);
    progressUpdateSnapshotLastSend(r, i);

    server = &r->configuration.servers[i];

    message.type = RAFT_IO_INSTALL_SNAPSHOT;
    message.server_id = server->id;
    message.server_address = server->address;

    args->term = r->current_term;
    args->last_index = logSnapshotIndex(r->log);
    args->last_term = logTermOf(r->log, args->last_index);
    args->conf_index = r->configuration_last_snapshot_index;

    rv = configurationCopy(&r->configuration_last_snapshot, &args->conf);
    if (rv != 0) {
        goto err;
    }

    infof("sending snapshot (%llu^%llu) to server %llu", args->last_index,
          args->last_term, server->id);

    rv = MessageEnqueue(r, &message);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    progressAbortSnapshot(r, i);
    assert(rv != 0);
    return rv;
}

int replicationProgress(struct raft *r, unsigned i)
{
    struct raft_server *server = &r->configuration.servers[i];
    bool progress_state_is_snapshot = progressState(r, i) == PROGRESS__SNAPSHOT;
    raft_index next_index = progressNextIndex(r, i);
    raft_index prev_index;
    raft_term prev_term;
    int max = MAX_APPEND_ENTRIES;
    raft_time last_recv = progressGetLastRecv(r, i);
    bool is_online;
    bool needs_snapshot = false;

    assert(r->state == RAFT_LEADER);
    assert(server->id != r->id);
    assert(next_index >= 1);

    if (last_recv == ULLONG_MAX) {
        is_online = false;
    } else {
        assert(r->now >= last_recv);
        is_online = r->now - last_recv < r->election_timeout;
    }

    /* From Section 3.5:
     *
     *   When sending an AppendEntries RPC, the leader includes the index and
     *   term of the entry in its log that immediately precedes the new
     *   entries. If the follower does not find an entry in its log with the
     *   same index and term, then it refuses the new entries. The consistency
     *   check acts as an induction step: the initial empty state of the logs
     *   satisfies the Log Matching Property, and the consistency check
     *   preserves the Log Matching Property whenever logs are extended. As a
     *   result, whenever AppendEntries returns successfully, the leader knows
     *   that the follower's log is identical to its own log up through the new
     *   entries (Log Matching Property in Figure 3.2).
     */
    prev_index = next_index - 1;

    if (next_index == 1) {
        /* This is the first entry, so prevIndex and prevTerm are null. */
        prev_term = 0;

        /* If we don't have entry 1 anymore in our log, we need to send a
         * snapshot. */
        if (logTermOf(r->log, 1) == 0) {
            needs_snapshot = true;
        }
    } else {
        /* Set prevIndex and prevTerm to the index and term of the entry at
         * next_index - 1. */
        prev_term = logTermOf(r->log, prev_index);

        /* We need to send a snapshot if one of the following two cases is true:
         *
         * - prev_term is 0: In this case we don't have information about the
         *   previous entry (i.e. it's not in the log and also it's not the last
         *   entry included in the last snapshot)
         *
         * - prev_index is not the last entry in the log and we don't have
         *   anymore the entry at next_index.
         */
        if (prev_term == 0 || (logLastIndex(r->log) > prev_index &&
                               logTermOf(r->log, next_index) == 0)) {
            needs_snapshot = true;
        }
    }

    /* If we have to send entries that are not anymore in our log, send the last
     * snapshot if we're not doing so already. */
    if (needs_snapshot || progress_state_is_snapshot) {
        raft_index snapshot_index = logSnapshotIndex(r->log);

        infof("missing previous entry at index %lld -> needs snapshot",
              prev_index);

        assert(snapshot_index > 0);

        if (!progress_state_is_snapshot && is_online) {
            return sendSnapshot(r, i);
        }

        /* Set the next index to the snapshot index + 1, so when we receive
         * the AppendEntries result for the empty heartbeat, we don't
         * consider the result as stale. */
        progressSetNextIndex(r, i, snapshot_index + 1);

        /* Send heartbeats anchored to the snapshot index */
        prev_index = snapshot_index;
        prev_term = logSnapshotTerm(r->log);
        assert(prev_term > 0);
        max = 0;
    }

    return sendAppendEntries(r, i, prev_index, prev_term, max);
}

/* Possibly trigger I/O requests for newly appended log entries or heartbeats.
 *
 * This function loops through all followers and triggers replication on them.
 *
 * It must be called only by leaders. */
static int triggerAll(struct raft *r)
{
    unsigned i;
    int rv;

    assert(r->state == RAFT_LEADER);

    /* Trigger replication for servers we didn't hear from recently. */
    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id) {
            continue;
        }
        /* Skip spare servers, unless they're being promoted. */
        if (server->role == RAFT_SPARE &&
            server->id != r->leader_state.promotee_id) {
            continue;
        }
        if (!progressShouldReplicate(r, i)) {
            continue;
        }
        rv = replicationProgress(r, i);
        if (rv != 0 && rv != RAFT_NOCONNECTION) {
            /* This is not a critical failure, let's just log it. */
            tracef("failed to send append entries to server %llu: %s (%d)",
                   server->id, raft_strerror(rv), rv);
        }
    }

    return 0;
}

int replicationHeartbeat(struct raft *r)
{
    return triggerAll(r);
}

/* Called after a successful append entries I/O request to update the index of
 * the last entry stored on disk. Return how many new entries that are still
 * present in our in-memory log were stored. */
static size_t updateLastStored(struct raft *r,
                               raft_index first_index,
                               struct raft_entry *entries,
                               size_t n_entries)
{
    size_t i;

    /* Check which of these entries is still in our in-memory log */
    for (i = 0; i < n_entries; i++) {
        struct raft_entry *entry = &entries[i];
        raft_index index = first_index + i;
        raft_term local_term = logTermOf(r->log, index);

        /* If we have no entry at this index, or if the entry we have now has a
         * different term, it means that this entry got truncated, so let's stop
         * here. */
        if (local_term == 0 || (local_term > 0 && local_term != entry->term)) {
            break;
        }

        /* If we do have an entry at this index, its term must match the one of
         * the entry we wrote on disk. */
        assert(local_term != 0 && local_term == entry->term);
    }

    r->last_stored += i;
    return i;
}

static void replicationQuorum(struct raft *r, const raft_index index);

/* Invoked once a disk write request for new entries has been completed. */
static int leaderPersistEntriesDone(struct raft *r,
                                    raft_index index,
                                    struct raft_entry *entries,
                                    unsigned n,
                                    int status)
{
    size_t server_index;

    assert(r->state == RAFT_LEADER);

    /* In case of a failed disk write, convert immediately to follower state,
     * giving the cluster a chance to elect another leader that doesn't have a
     * full disk (or whatever caused our write error). */
    if (status != 0) {
        if (r->io != NULL) {
            ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        }
        convertToFollower(r);
        goto out;
    }

    updateLastStored(r, index, entries, n);

    /* Only update the next index if we are part of the current
     * configuration. The only case where this is not true is when we were
     * asked to remove ourselves from the cluster.
     *
     * From Section 4.2.2:
     *
     *   there will be a period of time (while it is committing Cnew) when a
     *   leader can manage a cluster that does not include itself; it
     *   replicates log entries but does not count itself in majorities.
     */
    server_index = configurationIndexOf(&r->configuration, r->id);
    if (server_index < r->configuration.n) {
        r->leader_state.progress[server_index].match_index = r->last_stored;
    }

    /* Check if we can commit some new entries. */
    replicationQuorum(r, r->last_stored);

out:
    return 0;
}

static int followerPersistEntriesDone(struct raft *r,
                                      raft_index index,
                                      struct raft_entry *entries,
                                      unsigned n,
                                      int status);

/* Invoked once a disk write request for new entries has been completed. */
int replicationPersistEntriesDone(struct raft *r,
                                  raft_index index,
                                  struct raft_entry *entries,
                                  unsigned n,
                                  int status)
{
    int rv;

    assert(n > 0);

    switch (r->state) {
        case RAFT_LEADER:
            rv = leaderPersistEntriesDone(r, index, entries, n, status);
            break;
        case RAFT_FOLLOWER:
            rv = followerPersistEntriesDone(r, index, entries, n, status);
            break;
        default:
            if (status == 0) {
                updateLastStored(r, index, entries, n);
            }
            rv = 0;
            break;
    }

    logRelease(r->log, index, entries, n);

    if (status != 0) {
        if (index <= logLastIndex(r->log)) {
            logTruncate(r->log, index);
        }
    }

    if (rv != 0) {
        return rv;
    }

    return 0;
}

static void persistEntries(struct raft *r,
                           raft_index index,
                           struct raft_entry entries[],
                           unsigned n)
{
    /* This must be the first time during this raft_step() call where we set new
     * entries to be persisted. */
    assert(!(r->update->flags & RAFT_UPDATE_ENTRIES));

    r->update->flags |= RAFT_UPDATE_ENTRIES;

    r->update->entries.index = index;
    r->update->entries.batch = entries;
    r->update->entries.n = n;
}

/* Submit a disk write for all entries from the given index onward. */
static int appendLeader(struct raft *r, raft_index index)
{
    struct raft_entry *entries = NULL;
    unsigned n;
    int rv;

    assert(r->state == RAFT_LEADER);
    assert(index > 0);
    assert(index > r->last_stored);

    /* Acquire all the entries from the given index onwards. */
    rv = logAcquire(r->log, index, &entries, &n);
    if (rv != 0) {
        goto err;
    }

    /* We expect this function to be called only when there are actually
     * some entries to write. */
    if (n == 0) {
        assert(false);
        ErrMsgPrintf(r->errmsg, "No log entries found at index %llu", index);
        rv = RAFT_SHUTDOWN;
        goto err_after_entries_acquired;
    }

    persistEntries(r, index, entries, n);

    return 0;

err_after_entries_acquired:
    logRelease(r->log, index, entries, n);
err:
    assert(rv != 0);
    return rv;
}

int replicationTrigger(struct raft *r, raft_index index)
{
    int rv;

    rv = appendLeader(r, index);
    if (rv != 0) {
        return rv;
    }

    return triggerAll(r);
}

int replicationUpdate(struct raft *r,
                      const struct raft_server *server,
                      const struct raft_append_entries_result *result)
{
    bool is_being_promoted;
    raft_index last_index;
    unsigned i;
    int rv;

    assert(r->state == RAFT_LEADER);
    assert(server->id != 0);

    i = configurationIndexOf(&r->configuration, server->id);

    assert(i < r->configuration.n);

    progressUpdateLastRecv(r, i);

    progressSetFeatures(r, i, result->features);

    /* If the RPC failed because of a log mismatch, retry.
     *
     * From Figure 3.1:
     *
     *   [Rules for servers] Leaders:
     *
     *   - If AppendEntries fails because of log inconsistency:
     *     decrement nextIndex and retry.
     */
    if (result->rejected > 0) {
        bool retry;
        retry = progressMaybeDecrement(r, i, result->rejected,
                                       result->last_log_index);
        if (retry) {
            /* Retry, ignoring errors. */
            infof("log mismatch -> send old entries");
            replicationProgress(r, i);
        }
        return 0;
    }

    /* In case of success the remote server is expected to send us back the
     * value of prevLogIndex + len(entriesToAppend). If it has a longer log, it
     * might be a leftover from previous terms. */
    last_index = result->last_log_index;
    if (last_index > logLastIndex(r->log)) {
        last_index = logLastIndex(r->log);
    }

    /* If the RPC succeeded, update our counters for this server.
     *
     * From Figure 3.1:
     *
     *   [Rules for servers] Leaders:
     *
     *   If successful update nextIndex and matchIndex for follower.
     */
    if (!progressMaybeUpdate(r, i, last_index)) {
        return 0;
    }

    switch (progressState(r, i)) {
        case PROGRESS__SNAPSHOT:
            /* If a snapshot has been installed, transition back to probe */
            if (progressSnapshotDone(r, i)) {
                progressToPipeline(r, i);
            }
            break;
        case PROGRESS__PROBE:
            /* Transition to pipeline */
            progressToPipeline(r, i);
    }

    /* If the server is currently being promoted and is catching with logs,
     * update the information about the current catch-up round, and possibly
     * proceed with the promotion. */
    is_being_promoted = r->leader_state.promotee_id != 0 &&
                        r->leader_state.promotee_id == server->id;
    if (is_being_promoted) {
        bool is_up_to_date = membershipUpdateCatchUpRound(r);
        if (is_up_to_date) {
            r->leader_state.promotee_id = 0;
        }
    }

    /* If we are transferring leadership to this follower, check if its log
     * is now up-to-date and, if so, send it a TimeoutNow RPC (unless we
     * already did). */
    if (r->leader_state.transferee == server->id) {
        if (progressIsUpToDate(r, i) && !r->leader_state.transferring) {
            rv = membershipLeadershipTransferStart(r);
            if (rv != 0) {
                r->leader_state.transferee = 0;
            }
        }
    }

    /* If this follower is in pipeline mode, send it more entries if
     * needed. */
    if (progressState(r, i) == PROGRESS__PIPELINE &&
        progressShouldReplicate(r, i)) {
        replicationProgress(r, i);
    }

    /* Check if we can commit some new entries. */
    replicationQuorum(r, last_index);

    return 0;
}

static void sendAppendEntriesResult(
    struct raft *r,
    const struct raft_append_entries_result *result)
{
    struct raft_message message;
    raft_id id = r->follower_state.current_leader.id;
    const char *address = r->follower_state.current_leader.address;
    int rv;

    assert(r->state == RAFT_FOLLOWER);

    /* There are two cases in which a follower can have no leader:
     *
     * - If it never had a leader before (e.g. it just started)
     *
     * - If has become a follower after stepping down from leader because
     *   it could not contact a majority of servers.
     *
     * In the first case we can't reach this function because no entries have
     * been received.
     *
     * In the second case we might call this function as part of the raft_step()
     * logic, namely when a raft_persist_entries task originally created by a
     * leader gets completed after the leader has stepped down and has now
     * become a follower. In this circumstance current_leader.address will be
     * NULL and we don't send any message.
     */
    if (r->follower_state.current_leader.address == NULL) {
        return;
    }

    if (result->rejected == 0) {
        infof("send success result to %llu", id);
    }

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.append_entries_result = *result;

    message.server_id = id;
    message.server_address = address;

    rv = MessageEnqueue(r, &message);
    if (rv != 0) {
        /* This is not fatal, we'll retry. */
        (void)rv;
    }
}

static int followerPersistEntriesDone(struct raft *r,
                                      raft_index first_index,
                                      struct raft_entry *entries,
                                      unsigned n,
                                      int status)
{
    struct raft_append_entries_result result;
    size_t i;
    size_t j;
    int rv;

    assert(r->state == RAFT_FOLLOWER);

    assert(entries != NULL);
    assert(n > 0);

    result.term = r->current_term;
    result.version = RAFT_APPEND_ENTRIES_RESULT_VERSION;
    result.features = RAFT_DEFAULT_FEATURE_FLAGS;
    if (status != 0) {
        result.rejected = first_index;
        goto respond;
    }

    i = updateLastStored(r, first_index, entries, n);

    /* We received an InstallSnapshot RPC while these entries were being
     * persisted to disk */
    if (replicationInstallSnapshotBusy(r)) {
        goto out;
    }

    /* If none of the entries that we persisted is present anymore in our
     * in-memory log, there's nothing to report or to do. We just discard
     * them. */
    if (i == 0) {
        goto out;
    }

    /* Possibly apply configuration changes as uncommitted. */
    for (j = 0; j < i; j++) {
        struct raft_entry *entry = &entries[j];
        raft_index index = first_index + j;
        raft_term local_term = logTermOf(r->log, index);

        assert(local_term != 0 && local_term == entry->term);

        if (entry->type == RAFT_CHANGE) {
            rv = membershipUncommittedChange(r, index, entry);
            if (rv != 0) {
                return rv;
            }
        }
    }

    result.rejected = 0;

respond:
    result.last_log_index = r->last_stored;
    sendAppendEntriesResult(r, &result);

out:
    return 0;
}

/* Check the log matching property against an incoming AppendEntries request.
 *
 * From Figure 3.1:
 *
 *   [AppendEntries RPC] Receiver implementation:
 *
 *   2. Reply false if log doesn't contain an entry at prevLogIndex whose
 *   term matches prevLogTerm.
 *
 * Return 0 if the check passed.
 *
 * Return 1 if the check did not pass and the request needs to be rejected.
 *
 * Return -1 if there's a conflict and we need to shutdown. */
static int checkLogMatchingProperty(struct raft *r,
                                    const struct raft_append_entries *args)
{
    raft_term local_prev_term;

    /* If this is the very first entry, there's nothing to check. */
    if (args->prev_log_index == 0) {
        assert(args->prev_log_term == 0);
        return 0;
    }

    local_prev_term = logTermOf(r->log, args->prev_log_index);
    if (local_prev_term == 0) {
        infof("missing previous entry (%llu^%llu) -> reject",
              args->prev_log_index, args->prev_log_term);
        return 1;
    }

    if (local_prev_term != args->prev_log_term) {
        if (args->prev_log_index <= r->commit_index) {
            /* Should never happen; something is seriously wrong! */
            infof(
                "conflicting terms %llu and %llu for entry %llu (commit "
                "index %llu) -> shutdown",
                local_prev_term, args->prev_log_term, args->prev_log_index,
                r->commit_index);
            return -1;
        }
        infof("previous term mismatch -> reject");
        return 1;
    }

    return 0;
}

/* Delete from our log all entries that conflict with the ones in the given
 * AppendEntries request.
 *
 * From Figure 3.1:
 *
 *   [AppendEntries RPC] Receiver implementation:
 *
 *   3. If an existing entry conflicts with a new one (same index but
 *   different terms), delete the existing entry and all that follow it.
 *
 * The i output parameter will be set to the array index of the first new log
 * entry that we don't have yet in our log, among the ones included in the given
 * AppendEntries request. */
static int deleteConflictingEntries(struct raft *r,
                                    const struct raft_append_entries *args,
                                    size_t *i)
{
    size_t j;
    int rv;

    for (j = 0; j < args->n_entries; j++) {
        struct raft_entry *entry = &args->entries[j];
        raft_index entry_index = args->prev_log_index + 1 + j;
        raft_term local_term = logTermOf(r->log, entry_index);

        if (local_term > 0 && local_term != entry->term) {
            if (entry_index <= r->commit_index) {
                /* Should never happen; something is seriously wrong! */
                return RAFT_SHUTDOWN;
            }

            infof("log mismatch (%llu^%llu vs %llu^%llu) -> truncate",
                  entry_index, local_term, entry_index, entry->term);

            /* Possibly discard uncommitted configuration changes. */
            if (r->configuration_uncommitted_index >= entry_index) {
                rv = membershipRollback(r);
                if (rv != 0) {
                    return rv;
                }
            }

            /* Delete all entries from this index on because they don't
             * match. */
            logTruncate(r->log, entry_index);

            /* Drop information about previously stored entries that have just
             * been discarded. */
            if (r->last_stored >= entry_index) {
                r->last_stored = entry_index - 1;
            }

            /* We want to append all entries from here on, replacing anything
             * that we had before. */
            break;
        } else if (local_term == 0) {
            /* We don't have an entry at this index, so we want to append this
             * new one and all the subsequent ones. */
            break;
        }
    }

    *i = j;

    return 0;
}

int replicationAppend(struct raft *r,
                      const struct raft_append_entries *args,
                      raft_index *rejected,
                      bool *async)
{
    raft_index index;
    struct raft_entry *entries;
    unsigned n_entries;
    int match;
    size_t n;
    size_t i;
    size_t j;
    int rv;

    assert(r != NULL);
    assert(args != NULL);
    assert(rejected != NULL);
    assert(async != NULL);

    assert(r->state == RAFT_FOLLOWER);

    *rejected = args->prev_log_index;
    *async = false;

    /* Check the log matching property. */
    match = checkLogMatchingProperty(r, args);
    if (match != 0) {
        assert(match == 1 || match == -1);
        return match == 1 ? 0 : RAFT_SHUTDOWN;
    }

    /* Delete conflicting entries. */
    rv = deleteConflictingEntries(r, args, &i);
    if (rv != 0) {
        return rv;
    }

    n = args->n_entries - i; /* Number of new entries */

    /* Update our in-memory log to reflect that we received these entries. We'll
     * notify the leader of a successful append once the write entries request
     * that we issue below actually completes.  */
    for (j = 0; j < n; j++) {
        struct raft_entry *entry = &args->entries[i + j];
        /* TODO This copy should not strictly be necessary, as the batch logic
         * will take care of freeing the batch buffer in which the entries are
         * received. However, this would lead to memory spikes in certain edge
         * cases. https://github.com/canonical/dqlite/issues/276
         */
        struct raft_entry copy = {0};
        rv = entryCopy(entry, &copy);
        if (rv != 0) {
            goto err;
        }

        rv = logAppend(r->log, copy.term, copy.type, &copy.buf, NULL);
        if (rv != 0) {
            raft_free(copy.buf.base);
            goto err;
        }
    }

    *rejected = 0;

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: If leaderCommit >
     *   commitIndex, set commitIndex = min(leaderCommit, index of last new
     *   entry).
     */
    if (args->leader_commit > r->commit_index &&
        r->last_stored >= r->commit_index) {
        r->commit_index = min(args->leader_commit, r->last_stored);
        r->update->flags |= RAFT_UPDATE_COMMIT_INDEX;
    }

    if (n == 0) {
        infof("no new entries to persist");
        return 0;
    }

    *async = true;

    /* Index of first new entry */
    index = args->prev_log_index + 1 + i;

    /* Acquire the relevant entries from the log. */
    rv = logAcquire(r->log, index, &entries, &n_entries);
    if (rv != 0) {
        goto err_after_log_append;
    }

    /* The number of entries we just acquired must be exactly n, which is the
     * number of new entries present in the message (here "new entries" means
     * entries that we don't have yet in our in-memory log).  That's because we
     * call logAppend above exactly n times, once for each new log entry, so
     * logAcquire will return exactly n entries. */
    assert(n_entries == n);

    /* The n == 0 case is handled above. */
    assert(n_entries > 0);

    if (n_entries == 1) {
        infof("start persisting 1 new entry (%llu^%llu)", index,
              entries[0].term);
    } else {
        infof("start persisting %u new entries (%llu^%llu..%llu^%llu)",
              n_entries, index, entries[0].term, index + n_entries - 1,
              entries[n_entries - 1].term);
    }

    persistEntries(r, index, entries, n_entries);

    entryBatchesDestroy(args->entries, args->n_entries);

    return 0;

err_after_log_append:
    /* Release all entries added to the in-memory log, making
     * sure the in-memory log and disk don't diverge, leading
     * to future log entries not being persisted to disk.
     */
    if (j != 0) {
        logTruncate(r->log, index);
    }

err:
    assert(rv != 0);
    return rv;
}

int replicationPersistSnapshotDone(struct raft *r,
                                   struct raft_snapshot_metadata *metadata,
                                   size_t offset,
                                   struct raft_buffer *chunk,
                                   bool last,
                                   int status)
{
    struct raft_append_entries_result result;
    int rv;

    (void)offset;
    (void)last;
    (void)chunk;

    /* We avoid converting to candidate state while installing a snapshot. */
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_UNAVAILABLE);

    r->snapshot.persisting = false;

    result.term = r->current_term;
    result.version = RAFT_APPEND_ENTRIES_RESULT_VERSION;
    result.features = RAFT_DEFAULT_FEATURE_FLAGS;
    result.rejected = 0;

    /* If we are shutting down, let's discard the result. */
    if (r->state == RAFT_UNAVAILABLE) {
        goto discard;
    }

    if (status != 0) {
        infof("failed to persist snapshot %llu^%llu: %s", metadata->index,
              metadata->term, raft_strerror(status));
        goto discard;
    }

    /* From Figure 5.3:
     *
     *   7. Discard the entire log
     *   8. ... load lastConfig as cluster configuration
     */
    rv = RestoreSnapshot(r, metadata);
    if (rv != 0) {
        tracef("restore snapshot %llu: %s", metadata->index,
               raft_strerror(status));
        goto discard;
    }

    goto respond;

discard:
    /* In case of error we must also free the snapshot data buffer and free the
     * configuration. */
    result.rejected = metadata->index;
    raft_configuration_close(&metadata->configuration);

respond:
    if (r->state == RAFT_FOLLOWER) {
        result.last_log_index = r->last_stored;
        sendAppendEntriesResult(r, &result);
    }

    return 0;
}

int replicationInstallSnapshot(struct raft *r,
                               const struct raft_install_snapshot *args,
                               raft_index *rejected,
                               bool *async)
{
    struct raft_snapshot_metadata metadata;
    raft_term local_term;

    assert(r->state == RAFT_FOLLOWER);

    *rejected = args->last_index;
    *async = false;

    /* If we are taking a snapshot ourselves or installing a snapshot, ignore
     * the request, the leader will eventually retry. TODO: we should do
     * something smarter. */
    if (r->snapshot.taking || r->snapshot.persisting) {
        *async = true;
        infof("already taking or installing snapshot");
        return RAFT_BUSY;
    }

    /* If our last snapshot is more up-to-date, this is a no-op */
    if (r->log->snapshot.last_index >= args->last_index) {
        infof("have more recent snapshot");
        *rejected = 0;
        return 0;
    }

    /* If we already have all entries in the snapshot, this is a no-op */
    local_term = logTermOf(r->log, args->last_index);
    if (local_term != 0 && local_term >= args->last_term) {
        infof("have all entries");
        *rejected = 0;
        return 0;
    }

    *async = true;

    /* Preemptively update our in-memory state. */
    logRestore(r->log, args->last_index, args->last_term);

    r->last_stored = 0;

    assert(!r->snapshot.persisting);
    r->snapshot.persisting = true;

    metadata.index = args->last_index;
    metadata.term = args->last_term;
    metadata.index = args->last_index;
    metadata.configuration_index = args->conf_index;
    metadata.configuration = args->conf;

    assert(!(r->update->flags & RAFT_UPDATE_SNAPSHOT));

    infof("start persisting snapshot (%llu^%llu)", metadata.index,
          metadata.term);

    r->update->flags |= RAFT_UPDATE_SNAPSHOT;
    r->update->snapshot.metadata = metadata;
    r->update->snapshot.offset = 0;
    r->update->snapshot.chunk = args->data;
    r->update->snapshot.last = true;

    return 0;
}

int replicationApplyConfigurationChange(struct raft *r, raft_index index)
{
    assert(index > 0);

    if (r->configuration_uncommitted_index != index) {
        return 0;
    }

    /* If this is an uncommitted configuration that we had already applied when
     * submitting the configuration change (for leaders) or upon receiving it
     * via an AppendEntries RPC (for followers), then reset the uncommitted
     * index, since that uncommitted configuration is now committed. */
    r->configuration_uncommitted_index = 0;
    r->configuration_committed_index = index;

    if (r->state == RAFT_LEADER) {
        const struct raft_server *server;

        /* If we are leader but not part of this new configuration, step
         * down.
         *
         * From Section 4.2.2:
         *
         *   In this approach, a leader that is removed from the configuration
         *   steps down once the Cnew entry is committed.
         */
        server = configurationGet(&r->configuration, r->id);
        if (server == NULL || server->role != RAFT_VOTER) {
            const char *reason;
            if (server == NULL) {
                reason = "leader removed from config";
            } else {
                reason = "leader no longer voter";
            }
            infof("%s -> step down", reason);
            convertToFollower(r);
        }
    }

    return 0;
}

int replicationSnapshot(struct raft *r,
                        struct raft_snapshot_metadata *metadata,
                        unsigned trailing)
{
    int rv;

    (void)trailing;

    /* Cache the configuration contained in the snapshot. While the snapshot was
     * written, new configuration changes could have been committed, these
     * changes will not be purged from the log by this snapshot. However
     * we still cache the configuration for consistency. */
    configurationClose(&r->configuration_last_snapshot);
    rv = configurationCopy(&metadata->configuration,
                           &r->configuration_last_snapshot);
    if (rv != 0) {
        /* TODO: make this a hard fault, because if we have no backup and the
         * log was truncated it will be impossible to rollback an aborted
         * configuration change. */
        tracef("failed to backup last committed configuration.");
    }

    /* Make also a copy of the index of the configuration contained in the
     * snapshot, we'll need it in case we send out an InstallSnapshot RPC. */
    r->configuration_last_snapshot_index = metadata->configuration_index;

    logSnapshot(r->log, metadata->index, r->snapshot.trailing);

    configurationClose(&metadata->configuration);

    return 0;
}

static unsigned replicationCountVotes(struct raft *r, raft_index index)
{
    unsigned votes;
    unsigned i;

    votes = 0;
    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        if (server->role != RAFT_VOTER) {
            continue;
        }
        if (r->leader_state.progress[i].match_index >= index) {
            votes++;
        }
    }

    return votes;
}

/* Check if a quorum has been reached for the given log index or some earlier
 * index, and update the commit index accordingly if so.
 *
 * From Figure 3.1:
 *
 *   [Rules for servers] Leaders:
 *
 *   If there exists an N such that N > commitIndex, a majority of
 *   matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N */
static void replicationQuorum(struct raft *r, raft_index index)
{
    unsigned votes;
    raft_term term;
    unsigned n_voters;
    raft_index uncommitted = 0; /* Lowest uncommitted entry in current term */
    const char *suffix;

    assert(r->state == RAFT_LEADER);
    n_voters = configurationVoterCount(&r->configuration);

    while (index > r->commit_index) {
        term = logTermOf(r->log, index);

        /* TODO: fuzzy-test --seed 0x8db5fccc replication/entries/partitioned
         * fails the assertion below. */
        if (term == 0) {
            return;
        }

        // assert(logTermOf(r->log, index) > 0);
        assert(term <= r->current_term);

        uncommitted = index;
        votes = replicationCountVotes(r, index);

        /* Don't commit entries from previous terms by counting replicas. */
        if (term < r->current_term) {
            break;
        }

        if (votes > n_voters / 2) {
            unsigned n = (unsigned)(index - r->commit_index);
            if (n == 1) {
                infof("commit 1 new entry (%llu^%llu)", index, term);
            } else {
                infof("commit %u new entries (%llu^%llu..%llu^%llu)", n,
                      r->commit_index + 1,
                      logTermOf(r->log, r->commit_index + 1), index, term);
            }
            r->commit_index = index;
            r->update->flags |= RAFT_UPDATE_COMMIT_INDEX;
            return;
        }

        /* Try with the previous uncommitted index, if any. */
        index -= 1;
    }

    if (uncommitted != 0) {
        if (votes == 1) {
            suffix = "";
        } else {
            suffix = "s";
        }
        infof("next uncommitted entry (%llu^%llu) has %u vote%s out of %u",
              uncommitted, logTermOf(r->log, uncommitted), votes, suffix,
              n_voters);
    }
}

inline bool replicationInstallSnapshotBusy(struct raft *r)
{
    return r->last_stored == 0 && r->snapshot.persisting;
}

#undef infof
#undef tracef
