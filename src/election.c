#include "election.h"

#include "assert.h"
#include "configuration.h"
#include "heap.h"
#include "log.h"
#include "message.h"
#include "random.h"
#include "tracing.h"

#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* Common fields between follower and candidate state.
 *
 * The follower_state and candidate_state structs in raft.h must be kept
 * consistent with this definition. */
struct followerOrCandidateState
{
    unsigned randomized_election_timeout;
};

/* Return a pointer to either the follower or candidate state. */
struct followerOrCandidateState *getFollowerOrCandidateState(struct raft *r)
{
    struct followerOrCandidateState *state;
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    if (r->state == RAFT_FOLLOWER) {
        state = (struct followerOrCandidateState *)&r->follower_state;
    } else {
        state = (struct followerOrCandidateState *)&r->candidate_state;
    }
    return state;
}

void electionResetTimer(struct raft *r)
{
    struct followerOrCandidateState *state = getFollowerOrCandidateState(r);
    unsigned timeout = RandomWithinRange(&r->random, r->election_timeout,
                                         2 * r->election_timeout);
    assert(timeout >= r->election_timeout);
    assert(timeout <= r->election_timeout * 2);
    state->randomized_election_timeout = timeout;
    r->election_timer_start = r->now;
    r->update->flags |= RAFT_UPDATE_TIMEOUT;
}

bool electionTimerExpired(struct raft *r)
{
    struct followerOrCandidateState *state = getFollowerOrCandidateState(r);
    raft_time now = r->now;
    return now - r->election_timer_start >= state->randomized_election_timeout;
}

raft_time electionTimerExpiration(struct raft *r)
{
    struct followerOrCandidateState *state;
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    state = getFollowerOrCandidateState(r);
    return r->election_timer_start + state->randomized_election_timeout;
}

/* Send a RequestVote RPC to the given server. */
static int electionSend(struct raft *r, const struct raft_server *server)
{
    struct raft_message message;
    raft_term term;
    int rv;
    assert(server->id != r->id);
    assert(server->id != 0);

    /* If we are in the pre-vote phase, we indicate our future term in the
     * request. */
    term = r->current_term;
    if (r->candidate_state.in_pre_vote) {
        term++;
    }

    /* Fill the RequestVote message.
     *
     * Note that we set last_log_index and last_log_term to the index and term
     * of the last persisted entry, to the last entry in our in-memory log
     * cache, because we must advertise only log entries that can't be lost at
     * restart.
     *
     * Also note that, for a similar reason, we apply pending configuration
     * changes only once they are persisted. When running an election we then
     * use only persisted information, which is safe (while using unpersisted
     * information for the log and persisted information for the configuration
     * or viceversa would lead to inconsistencies and violations of Raft
     * invariants).
     */
    message.type = RAFT_IO_REQUEST_VOTE;
    message.request_vote.term = term;
    message.request_vote.candidate_id = r->id;
    message.request_vote.last_log_index = r->last_stored;
    message.request_vote.last_log_term = logTermOf(r->log, r->last_stored);
    message.request_vote.disrupt_leader = r->candidate_state.disrupt_leader;
    message.request_vote.pre_vote = r->candidate_state.in_pre_vote;

    message.server_id = server->id;
    message.server_address = server->address;

    rv = MessageEnqueue(r, &message);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

void electionStart(struct raft *r)
{
    raft_term term;
    size_t n_voters;
    size_t voting_index;
    size_t i;

    assert(r->state == RAFT_CANDIDATE);

    n_voters = configurationVoterCount(&r->configuration);
    voting_index = configurationIndexOfVoter(&r->configuration, r->id);

    /* This function should not be invoked if we are not a voting server, hence
     * voting_index must be lower than the number of servers in the
     * configuration (meaning that we are a voting server). */
    assert(voting_index < r->configuration.n);

    /* Coherence check that configurationVoterCount and
     * configurationIndexOfVoter have returned something that makes sense. */
    assert(n_voters <= r->configuration.n);
    assert(voting_index < n_voters);

    /* During pre-vote we don't increment our term, or reset our vote. Resetting
     * our vote could lead to double-voting if we were to receive a RequestVote
     * RPC during our Candidate state while we already voted for a server during
     * the term. */
    if (!r->candidate_state.in_pre_vote) {
        /* Increment current term and vote for self */
        term = r->current_term + 1;
        tracef("beginning of term %llu", term);

        /* Mark both the current term and vote as changed. */
        r->update->flags |= RAFT_UPDATE_CURRENT_TERM | RAFT_UPDATE_VOTED_FOR;

        /* Update our cache too. */
        r->current_term = term;
        r->voted_for = r->id;
    }

    /* Reset election timer. */
    electionResetTimer(r);

    assert(r->candidate_state.votes != NULL);

    /* Initialize the votes array and send vote requests. */
    for (i = 0; i < n_voters; i++) {
        if (i == voting_index) {
            r->candidate_state.votes[i] = true; /* We vote for ourselves */
        } else {
            r->candidate_state.votes[i] = false;
        }
    }
    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        int rv;

        if (server->id == r->id || server->role != RAFT_VOTER) {
            continue;
        }

        rv = electionSend(r, server);
        if (rv != 0) {
            /* This is not a critical failure, let's just log it. */
            tracef("failed to send vote request to server %llu: %s", server->id,
                   raft_strerror(rv));
        }
    }
}

int electionVote(struct raft *r,
                 const struct raft_request_vote *args,
                 bool *granted)
{
    const struct raft_server *local_server;
    raft_index local_last_index;
    raft_term local_last_term;
    bool is_transferee; /* Requester is the target of a leadership transfer */

    assert(r != NULL);
    assert(args != NULL);
    assert(granted != NULL);

    local_server = configurationGet(&r->configuration, r->id);

    *granted = false;

    if (local_server == NULL || local_server->role != RAFT_VOTER) {
        tracef("local server is not voting -> not granting vote");
        return 0;
    }

    is_transferee =
        r->transfer != NULL && r->transfer->id == args->candidate_id;
    if (!args->pre_vote && r->voted_for != 0 &&
        r->voted_for != args->candidate_id && !is_transferee) {
        tracef("local server already voted -> not granting vote");
        return 0;
    }

    /* Raft Dissertation 9.6:
     * > In the Pre-Vote algorithm, a candidate
     * > only increments its term if it first learns from a majority of the
     * > cluster that they would be willing
     * > to grant the candidate their votes (if the candidate's log is
     * > sufficiently up-to-date, and the voters
     * > have not received heartbeats from a valid leader for at least a
     * baseline > election timeout) Arriving here means that in a pre-vote
     * phase, we will cast our vote if the candidate's log is sufficiently
     * up-to-date, no matter what the candidate's term is. We have already
     * checked if we currently have a leader upon reception of the RequestVote
     * RPC, meaning the 2 conditions will be satisfied if the candidate's log is
     * up-to-date.
     * */
    local_last_index = logLastIndex(r->log);

    /* Our log is definitely not more up-to-date if it's empty! */
    if (local_last_index == 0) {
        tracef("local log is empty -> granting vote");
        goto grant_vote;
    }

    local_last_term = logLastTerm(r->log);

    if (args->last_log_term < local_last_term) {
        /* The requesting server has last entry's log term lower than ours. */
        tracef(
            "local last entry %llu has term %llu higher than %llu -> not "
            "granting",
            local_last_index, local_last_term, args->last_log_term);
        return 0;
    }

    if (args->last_log_term > local_last_term) {
        /* The requesting server has a more up-to-date log. */
        tracef(
            "remote last entry %llu has term %llu higher than %llu -> "
            "granting vote",
            args->last_log_index, args->last_log_term, local_last_term);
        goto grant_vote;
    }

    /* The term of the last log entry is the same, so let's compare the length
     * of the log. */
    assert(args->last_log_term == local_last_term);

    if (local_last_index <= args->last_log_index) {
        /* Our log is shorter or equal to the one of the requester. */
        tracef("remote log equal or longer than local -> granting vote");
        goto grant_vote;
    }

    tracef("remote log shorter than local -> not granting vote");

    return 0;

grant_vote:
    if (!args->pre_vote) {
        /* Mark the vote as changed. */
        r->update->flags |= RAFT_UPDATE_CURRENT_TERM | RAFT_UPDATE_VOTED_FOR;

        r->voted_for = args->candidate_id;

        /* Reset the election timer. */
        r->election_timer_start = r->now;
        r->update->flags |= RAFT_UPDATE_TIMEOUT;
    }

    tracef("vote granted to %llu", args->candidate_id);
    *granted = true;

    return 0;
}

bool electionTally(struct raft *r, size_t voter_index)
{
    size_t n_voters = configurationVoterCount(&r->configuration);
    size_t votes = 0;
    size_t i;
    size_t half = n_voters / 2;

    assert(r->state == RAFT_CANDIDATE);
    assert(r->candidate_state.votes != NULL);

    r->candidate_state.votes[voter_index] = true;

    for (i = 0; i < n_voters; i++) {
        if (r->candidate_state.votes[i]) {
            votes++;
        }
    }

    return votes >= half + 1;
}

#undef tracef
