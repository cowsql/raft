#include "convert.h"

#include "assert.h"
#include "client.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "tracing.h"

#define infof(...) Tracef(r->tracer, __VA_ARGS__)
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* Convenience for setting a new state value and asserting that the transition
 * is valid. */
static void convertSetState(struct raft *r, unsigned short new_state)
{
    /* Check that the transition is legal, see Figure 3.3. Note that with
     * respect to the paper we have an additional "unavailable" state, which is
     * the initial or final state. */
    assert(r->state != new_state);
    assert((r->state == RAFT_UNAVAILABLE && new_state == RAFT_FOLLOWER) ||
           (r->state == RAFT_FOLLOWER && new_state == RAFT_CANDIDATE) ||
           (r->state == RAFT_CANDIDATE && new_state == RAFT_FOLLOWER) ||
           (r->state == RAFT_CANDIDATE && new_state == RAFT_LEADER) ||
           (r->state == RAFT_LEADER && new_state == RAFT_FOLLOWER) ||
           (r->state == RAFT_FOLLOWER && new_state == RAFT_UNAVAILABLE) ||
           (r->state == RAFT_CANDIDATE && new_state == RAFT_UNAVAILABLE) ||
           (r->state == RAFT_LEADER && new_state == RAFT_UNAVAILABLE));
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

/* Clear the current state */
static void convertClear(struct raft *r)
{
    assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
           r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);
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
    convertClear(r);
    convertSetState(r, RAFT_FOLLOWER);

    /* Reset election timer. */
    electionResetTimer(r);

    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;
}

int convertToCandidate(struct raft *r, const bool disrupt_leader)
{
    const struct raft_server *server;
    size_t n_voters = configurationVoterCount(&r->configuration);

    (void)server; /* Only used for assertions. */

    /* Check that we're a voter in the current configuration. */
    server = configurationGet(&r->configuration, r->id);
    assert(server != NULL);
    assert(server->role == RAFT_VOTER);

    convertClear(r);
    convertSetState(r, RAFT_CANDIDATE);

    /* Allocate the votes array. */
    r->candidate_state.votes = raft_malloc(n_voters * sizeof(bool));
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

int convertToLeader(struct raft *r)
{
    size_t n_voters;
    int rv;

    convertClear(r);
    convertSetState(r, RAFT_LEADER);

    /* Reset timers */
    r->election_timer_start = r->now;
    r->update->flags |= RAFT_UPDATE_TIMEOUT;

    /* Allocate and initialize the progress array. */
    rv = progressBuildArray(r);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        goto err;
    }

    /* Reset promotion state. */
    r->leader_state.promotee_id = 0;
    r->leader_state.round_number = 0;
    r->leader_state.round_index = 0;
    r->leader_state.round_start = 0;

    /* By definition, all entries until the last_stored entry will be committed
     * if we are the only voter around. */
    n_voters = configurationVoterCount(&r->configuration);
    assert(n_voters > 0);
    if (n_voters == 1) {
        if (r->last_stored > r->commit_index) {
            infof("apply log entries after self election %llu %llu",
                  r->last_stored, r->commit_index);
            r->commit_index = r->last_stored;
            r->update->flags |= RAFT_UPDATE_COMMIT_INDEX;
        }
    } else {
        /* Raft Dissertation, paragraph 6.4:
         *
         *   The Leader Completeness Property guarantees that a leader has all
         *   committed entries, but at the start of its term, it may not know
         *   which those are. To find out, it needs to commit an entry from its
         *   term. Raft handles this by having each leader commit a blank no-op
         *   entry into the log at the start of its term. */
        struct raft_entry entry;

        entry.type = RAFT_BARRIER;
        entry.term = r->current_term;
        entry.buf.len = 8;
        entry.buf.base = raft_malloc(entry.buf.len);

        if (entry.buf.base == NULL) {
            rv = RAFT_NOMEM;
            goto err;
        }

        rv = ClientSubmit(r, &entry, 1);
        if (rv != 0) {
            /* This call to ClientSubmit can only fail with RAFT_NOMEM, because
             * it's not a RAFT_CHANGE entry (RAFT_MALFORMED can't be returned)
             * and we're leader (RAFT_NOTLEADER can't be returned) */
            assert(rv == RAFT_NOMEM);
            infof("can't submit no-op after converting to leader: %s",
                  raft_strerror(rv));
            raft_free(entry.buf.base);
            goto err;
        }
    }

    return 0;

err:
    assert(rv == RAFT_NOMEM);
    return rv;
}

void convertToUnavailable(struct raft *r)
{
    /* Abort any pending leadership transfer request. */
    if (r->transfer != NULL) {
        membershipLeadershipTransferClose(r);
    }
    convertClear(r);
    convertSetState(r, RAFT_UNAVAILABLE);
}

#undef infof
#undef tracef
