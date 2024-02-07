#include "recv_append_entries.h"

#include "assert.h"
#include "convert.h"
#include "entry.h"
#include "heap.h"
#include "message.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"
#include "trail.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

int recvAppendEntries(struct raft *r,
                      raft_id id,
                      const char *address,
                      const struct raft_append_entries *args)
{
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    raft_index last_index;
    int match;
    bool async;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);
    assert(address != NULL);

    result->rejected = args->prev_log_index;
    result->version = MESSAGE__APPEND_ENTRIES_RESULT_VERSION;
    result->features = MESSAGE__FEATURE_CAPACITY;

    match = recvEnsureMatchingTerms(r, args->term);

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: Reply false if term <
     *   currentTerm.
     */
    if (match < 0) {
        infof("local term is higher (%llu vs %llu) -> reject", r->current_term,
              args->term);
        goto reply;
    }

    /* If we get here it means that the term in the request matches our current
     * term or it was higher and we have possibly stepped down, because we
     * discovered the current leader:
     *
     * From Figure 3.1:
     *
     *   Rules for Servers: Candidates: if AppendEntries RPC is received from
     *   new leader: convert to follower.
     *
     * From Section 3.4:
     *
     *   While waiting for votes, a candidate may receive an AppendEntries RPC
     *   from another server claiming to be leader. If the leader's term
     *   (included in its RPC) is at least as large as the candidate's current
     *   term, then the candidate recognizes the leader as legitimate and
     *   returns to follower state. If the term in the RPC is smaller than the
     *   candidate's current term, then the candidate rejects the RPC and
     *   continues in candidate state.
     *
     * From state diagram in Figure 3.3:
     *
     *   [candidate]: discovers current leader -> [follower]
     *
     * Note that it should not be possible for us to be in leader state, because
     * the leader that is sending us the request should have either a lower term
     * (and in that case we reject the request above), or a higher term (and in
     * that case we step down). It can't have the same term because at most one
     * leader can be elected at any given term.
     */
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    assert(r->current_term == args->term);

    if (r->state == RAFT_CANDIDATE) {
        /* The current term and the peer one must match, otherwise we would have
         * either rejected the request or stepped down to followers. */
        assert(match == 0);
        infof("discovered leader (%llu) -> step down ", id);
        convertToFollower(r);
    }

    assert(r->state == RAFT_FOLLOWER);

    /* Update current leader because the term in this AppendEntries RPC is up to
     * date. */
    rv = recvUpdateLeader(r, id, address);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        return rv;
    }

    /* Reset the election timer. */
    r->election_timer_start = r->now;
    r->update->flags |= RAFT_UPDATE_TIMEOUT;

    /* If we are installing a snapshot, ignore these entries. TODO: we should do
     * something smarter, e.g. buffering the entries in the I/O backend, which
     * should be in charge of serializing everything. */
    if (r->snapshot.installing && args->n_entries > 0) {
        infof("snapshot install in progress -> ignore");
        if (args->n_entries > 0) {
            assert(args->entries[0].batch != NULL);
            raft_free(args->entries[0].batch);
        }
        return 0;
    }

    rv = replicationAppend(r, args, &result->rejected, &async);
    if (rv != 0 && rv != RAFT_BUSY) {
        goto err;
    }

    if (async) {
        return 0;
    }

    /* Set the last_log_index field of the response. */
    last_index = TrailLastIndex(&r->trail);
    if (result->rejected > 0) {
        /*  In case of rejection we have to cases:
         *
         *  1. If our log is shorter and is missing the entry at #rejected, then
         *     we set last_log_index to our actual last log index.
         *  2. If our log is equal or longer, but the entry at #rejected has a
         *     different term, then we set last_log_index to #rejected - 1 and
         *     the leader will eventually retry with that index. */
        result->last_log_index = last_index;
        if (result->last_log_index >= result->rejected) {
            result->last_log_index = result->rejected - 1;
        }
    } else {
        /* In case of synchronous success we expect to have all entries, and no
         * new entry needs to be persisted. However we might still be persisting
         * some of them, so we set last_log_index to the index of the last
         * stored index that is lower or equal than the last index in this
         * message.
         *
         * We use a stored index instead of an in-memory one because the leader
         * will use it to update our match index and to check quorum. */
        result->last_log_index = args->prev_log_index + args->n_entries;
        assert(last_index >= result->last_log_index);
        if (result->last_log_index > r->last_stored) {
            result->last_log_index = r->last_stored;
        }
    }

reply:
    result->term = r->current_term;

    /* Free the entries batch, if any. */
    if (args->n_entries) {
        assert(args->entries[0].batch != NULL);
        raft_free(args->entries[0].batch);
    }

    result->capacity = r->capacity;

    message.type = RAFT_APPEND_ENTRIES_RESULT;
    message.server_id = id;
    message.server_address = address;

    rv = MessageEnqueue(r, &message);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    assert(rv != 0);
    if (args->n_entries) {
        assert(args->entries[0].batch != NULL);
        raft_free(args->entries[0].batch);
    }
    return rv;
}

#undef infof
#undef tracef
