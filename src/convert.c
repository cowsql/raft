#include "convert.h"

#include "assert.h"
#include "client.h"
#include "configuration.h"
#include "election.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "string.h"
#include "tracing.h"
#include "trail.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* Convenience for setting a new state value and asserting that the transition
 * is valid. */
static void convertSetState(struct raft *r, unsigned short new_state)
{
    r->state = new_state;
    r->update->flags |= RAFT_UPDATE_STATE;
}

/* Clear follower state. */
static void convertClearFollower(struct raft *r)
{
    r->follower_state.current_leader.id = 0;
    if (r->follower_state.current_leader.address != NULL) {
        raft_free(r->follower_state.current_leader.address);
    }
    r->follower_state.current_leader.address = NULL;
}

/* Clear candidate state. */
static void convertClearCandidate(struct raft *r)
{
    if (r->candidate_state.votes != NULL) {
        raft_free(r->candidate_state.votes);
        r->candidate_state.votes = NULL;
    }
}

/* Clear leader state. */
static void convertClearLeader(struct raft *r)
{
    if (r->leader_state.progress != NULL) {
        raft_free(r->leader_state.progress);
        r->leader_state.progress = NULL;
    }
}

void convertClear(struct raft *r)
{
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE ||
           r->state == RAFT_LEADER);
    switch (r->state) {
        case RAFT_FOLLOWER:
            convertClearFollower(r);
            break;
        case RAFT_CANDIDATE:
            convertClearCandidate(r);
            break;
        case RAFT_LEADER:
            convertClearLeader(r);
            break;
    }
}

void convertToFollower(struct raft *r)
{
    assert(r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);
    switch (r->state) {
        case RAFT_CANDIDATE:
            convertClearCandidate(r);
            break;
        case RAFT_LEADER:
            convertClearLeader(r);
            break;
    }
    convertSetState(r, RAFT_FOLLOWER);

    /* Reset election timer. */
    electionResetTimer(r);

    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;

    /* The follower's match index tracks the highest index in the local log that
     * is known to match the same index in the leader log, because the leader
     * has sent an AppendEntries request containing that index.
     *
     * This is necessary in order to avoid sending AppendEntries results that
     * contain indexes that were never checked against the log matching
     * property. */
    r->follower_state.match = 0;
}

int convertToCandidate(struct raft *r, const bool disrupt_leader)
{
    const struct raft_server *server;
    size_t n_voters = configurationVoterCount(&r->configuration);

    assert(r->state == RAFT_FOLLOWER);

    (void)server; /* Only used for assertions. */

    /* Check that we're a voter in the current configuration. */
    server = configurationGet(&r->configuration, r->id);
    assert(server != NULL);
    assert(server->role == RAFT_VOTER);

    convertClearFollower(r);
    convertSetState(r, RAFT_CANDIDATE);

    /* Allocate the votes array. */
    r->candidate_state.votes =
        raft_calloc(n_voters, sizeof *r->candidate_state.votes);
    if (r->candidate_state.votes == NULL) {
        return RAFT_NOMEM;
    }
    r->candidate_state.disrupt_leader = disrupt_leader;
    r->candidate_state.in_pre_vote = disrupt_leader ? false : r->pre_vote;

    /* Fast-forward to leader if we're the only voting server in the
     * configuration. */
    if (n_voters == 1) {
        infof("self elect and convert to leader");
        return convertToLeader(r);
    }

    /* Start a new election round */
    electionStart(r);

    return 0;
}

extern char *__progname;

/* Detect if we're being run as dqlite unit test.
 *
 * Those tests assume that a barrier is *always* issued when converting to
 * leader, and since we can't change those tests we maintain that behavior for
 * compability. */
static bool isDqliteUnitTest(void)
{
    return strcmp(__progname, "unit-test") == 0;
}

int convertToLeader(struct raft *r)
{
    struct raft_progress *progress;
    size_t n_voters;
    unsigned i;
    int rv;

    assert(r->state == RAFT_CANDIDATE);

    /* Allocate and initialize the progress array. */
    progress = progressBuildArray(r);
    if (progress == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    n_voters = configurationVoterCount(&r->configuration);
    assert(n_voters > 0);

    /* Copy features and capacity information. */
    for (i = 0; i < n_voters; i++) {
        unsigned j;
        j = configurationActualIndexOfVoter(&r->configuration, i);
        progress[j].features = r->candidate_state.votes[i].features;
        progress[j].capacity = r->candidate_state.votes[i].capacity;
    }

    convertClearCandidate(r);
    convertSetState(r, RAFT_LEADER);

    r->leader_state.progress = progress;

    /* Reset timers */
    r->election_timer_start = r->now;
    r->update->flags |= RAFT_UPDATE_TIMEOUT;

    /* Reset promotion state. */
    r->leader_state.promotee_id = 0;
    r->leader_state.round_number = 0;
    r->leader_state.round_index = 0;
    r->leader_state.round_start = 0;

    /* Reset leadership transfer. */
    r->leader_state.transferee = 0;
    r->leader_state.transferring = false;

    /* If there is only one voter, by definition all entries until the
     * last_stored can be considered committed (and the voter must be us, since
     * no one else could have become leader).
     *
     * Otherwise, if we have some entries in the log that are past our current
     * commit index, they must be from previous terms and we immediately append
     * a barrier entry, in order to finalize any pending transaction in the user
     * state machine or any pending configuration change. */
    if (n_voters == 1) {
        assert(configurationIndexOfVoter(&r->configuration, r->id) == 0);
        if (r->last_stored > r->commit_index) {
            r->commit_index = r->last_stored;
            r->update->flags |= RAFT_UPDATE_COMMIT_INDEX;
        }
    } else if (TrailLastIndex(&r->trail) > r->commit_index ||
               isDqliteUnitTest()) {
        /* Raft Dissertation, paragraph 6.4:
         *
         *   The Leader Completeness Property guarantees that a leader has all
         *   committed entries, but at the start of its term, it may not know
         *   which those are. To find out, it needs to commit an entry from its
         *   term. Raft handles this by having each leader commit a blank no-op
         *   entry into the log at the start of its term. */
        r->barrier.type = RAFT_BARRIER;
        r->barrier.term = r->current_term;
        r->barrier.buf.len = 8;
        r->barrier.buf.base = raft_malloc(r->barrier.buf.len);

        if (r->barrier.buf.base == NULL) {
            rv = RAFT_NOMEM;
            goto err;
        }

        *(uint64_t *)r->barrier.buf.base = 0;

        r->barrier.batch = r->barrier.buf.base;

        rv = ClientSubmit(r, &r->barrier, 1);
        if (rv != 0) {
            /* This call to ClientSubmit can only fail with RAFT_NOMEM, because
             * it's not a RAFT_CHANGE entry (RAFT_MALFORMED can't be returned)
             * and we're leader (RAFT_NOTLEADER can't be returned) */
            assert(rv == RAFT_NOMEM);
            infof("can't submit no-op after converting to leader: %s",
                  raft_strerror(rv));
            raft_free(r->barrier.buf.base);
            goto err;
        }
    }

    return 0;

err:
    assert(rv == RAFT_NOMEM);
    return rv;
}

#undef infof
#undef tracef
