#include "election.h"

#include "assert.h"
#include "configuration.h"
#include "heap.h"
#include "message.h"
#include "random.h"
#include "tracing.h"
#include "trail.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)

/* Common fields between follower and candidate state.
 *
 * The follower_state and candidate_state structs in raft.h must be kept
 * consistent with this definition. */
struct followerOrCandidateState
{
    unsigned randomized_election_timeout;
};

/* Return a pointer to either the follower or candidate state. */
struct followerOrCandidateState *getFollowerOrCandidateState(
    const struct raft *r)
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

void electionUpdateRandomizedTimeout(struct raft *r)
{
    struct followerOrCandidateState *state = getFollowerOrCandidateState(r);
    unsigned timeout = RandomWithinRange(&r->random, r->election_timeout,
                                         2 * r->election_timeout);
    assert(timeout >= r->election_timeout);
    assert(timeout <= r->election_timeout * 2);
    state->randomized_election_timeout = timeout;
}

void electionResetTimer(struct raft *r)
{
    electionUpdateRandomizedTimeout(r);
    r->election_timer_start = r->now;
    r->update->flags |= RAFT_UPDATE_TIMEOUT;
}

raft_time electionTimerExpiration(const struct raft *r)
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
     * of the last persisted entry, not to the last entry in our in-memory log
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
    message.type = RAFT_REQUEST_VOTE;
    message.request_vote.version = MESSAGE__REQUEST_VOTE_VERSION;
    message.request_vote.term = term;
    message.request_vote.candidate_id = r->id;
    message.request_vote.last_log_index = r->last_stored;
    message.request_vote.last_log_term = TrailTermOf(&r->trail, r->last_stored);
    message.request_vote.disrupt_leader = r->candidate_state.disrupt_leader;
    message.request_vote.pre_vote = r->candidate_state.in_pre_vote;

    message.server_id = server->id;
    message.server_address = server->address;

    rv = MessageEnqueue(r, &message);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        return rv;
    }

    return 0;
}

void electionStart(struct raft *r)
{
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
     * RPC during our Candidate state, while we actually already voted for a
     * server during the term. */
    if (!r->candidate_state.in_pre_vote) {
        /* Increment current term and vote for self */
        r->current_term += 1;
        r->voted_for = r->id;

        /* Mark both the current term and vote as changed. */
        r->update->flags |= RAFT_UPDATE_CURRENT_TERM | RAFT_UPDATE_VOTED_FOR;
    }

    /* Reset election timer. */
    electionResetTimer(r);

    assert(r->candidate_state.votes != NULL);

    /* Initialize the votes array and send vote requests. */
    for (i = 0; i < n_voters; i++) {
        if (i == voting_index) {
            r->candidate_state.votes[i].grant = true; /* Vote for self */
            r->candidate_state.votes[i].features = MESSAGE__FEATURE_CAPACITY;
            r->candidate_state.votes[i].capacity = r->capacity;
        } else {
            r->candidate_state.votes[i].grant = false;
            r->candidate_state.votes[i].features = 0;
            r->candidate_state.votes[i].capacity = 0;
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
            assert(rv == RAFT_NOMEM);
            infof("can't send vote request to server %llu: %s", server->id,
                  raft_strerror(rv));
        }
    }
}

void electionVote(struct raft *r,
                  const struct raft_request_vote *args,
                  bool *granted)
{
    const struct raft_server *local_server;
    const char *grant_text;
    const char *deny_text;
    raft_index local_last_index;
    raft_term local_last_term;

    assert(r != NULL);
    assert(args != NULL);
    assert(granted != NULL);

    local_server = configurationGet(&r->configuration, r->id);

    *granted = false;

    if (args->pre_vote) {
        grant_text = "pre-vote ok";
        deny_text = "deny pre-vote";
    } else {
        grant_text = "grant vote";
        deny_text = "don't grant vote";
    }

    if (local_server == NULL || local_server->role != RAFT_VOTER) {
        infof("local server is not voting -> %s", deny_text);
        return;
    }

    if (!args->pre_vote && r->voted_for != 0 &&
        r->voted_for != args->candidate_id) {
        infof("already voted for server %llu -> %s", r->voted_for, deny_text);
        return;
    }

    /* From Section 9.6:
     *
     *   In the Pre-Vote algorithm, a candidate only increments its term if it
     *   first learns from a majority of the cluster that they would be willing
     *   to grant the candidate their votes (if the candidate's log is
     *   sufficiently up-to-date, and the voters have not received heartbeats
     *   from a valid leader for at least a baseline election timeout).
     *
     * Arriving here means that in a pre-vote phase, we will cast our vote if
     * the candidate's log is sufficiently up-to-date, no matter what the
     * candidate's term is. We have already checked if we currently have a
     * leader upon reception of the RequestVote RPC, meaning the 2 conditions
     * will be satisfied if the candidate's log is up-to-date. */
    local_last_index = TrailLastIndex(&r->trail);

    /* Our log is definitely not more up-to-date if it's empty! */
    if (local_last_index == 0) {
        infof("local log is empty -> %s", grant_text);
        goto grant_vote;
    }

    local_last_term = TrailLastTerm(&r->trail);

    /* If the term of the last entry of the requesting server's log is lower
     * than the term of the last entry of our log, then our log is more
     * up-to-date and we don't grant the vote. */
    if (args->last_log_term < local_last_term) {
        infof("remote log older (%llu^%llu vs %llu^%llu) -> %s",
              args->last_log_index, args->last_log_term, local_last_index,
              local_last_term, deny_text);
        return;
    }

    /* If the term of the last entry of our log is lower than the term of the
     * last entry of the requesting server's log, then the requesting server's
     * log is more up-to-date and we grant our vote. */
    if (local_last_term < args->last_log_term) {
        infof("remote log is more recent (%llu^%llu vs %llu^%llu) -> %s",
              args->last_log_index, args->last_log_term, local_last_index,
              local_last_term, grant_text);
        goto grant_vote;
    }

    /* The term of the last log entry is the same, so let's compare the length
     * of the log. */
    assert(args->last_log_term == local_last_term);

    if (local_last_index <= args->last_log_index) {
        /* Our log is shorter or equal to the one of the requester. */
        if (local_last_index == args->last_log_index) {
            infof("remote log is equal (%llu^%llu) -> %s", args->last_log_index,
                  args->last_log_term, grant_text);
        } else {
            assert(local_last_index < args->last_log_index);
            infof("remote log is longer (%llu^%llu vs %llu^%llu) -> %s",
                  args->last_log_index, args->last_log_term, local_last_index,
                  local_last_term, grant_text);
        }
        goto grant_vote;
    }

    infof("remote log shorter (%llu^%llu vs %llu^%llu) -> %s",
          args->last_log_index, args->last_log_term, local_last_index,
          local_last_term, deny_text);

    return;

grant_vote:
    if (!args->pre_vote) {
        /* Mark the vote as changed. */
        r->update->flags |= RAFT_UPDATE_CURRENT_TERM | RAFT_UPDATE_VOTED_FOR;

        r->voted_for = args->candidate_id;

        /* Reset the election timer. */
        r->election_timer_start = r->now;
        r->update->flags |= RAFT_UPDATE_TIMEOUT;
    }

    *granted = true;
}

bool electionTally(struct raft *r,
                   const size_t voter_index,
                   unsigned *votes,
                   unsigned *n_voters)
{
    size_t half;
    size_t i;

    *n_voters = configurationVoterCount(&r->configuration);
    *votes = 0;

    half = *n_voters / 2;

    assert(r->state == RAFT_CANDIDATE);
    assert(r->candidate_state.votes != NULL);

    r->candidate_state.votes[voter_index].grant = true;

    for (i = 0; i < *n_voters; i++) {
        if (r->candidate_state.votes[i].grant) {
            *votes += 1;
        }
    }

    return *votes >= half + 1;
}

#undef infof
