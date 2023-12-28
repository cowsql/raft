#include "recv_request_vote_result.h"

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)

int recvRequestVoteResult(struct raft *r,
                          raft_id id,
                          const char *address,
                          const struct raft_request_vote_result *result)
{
    size_t votes_index;
    int match;
    int rv;

    (void)address;

    assert(r != NULL);
    assert(id > 0);

    votes_index = configurationIndexOfVoter(&r->configuration, id);
    if (votes_index == r->configuration.n) {
        infof("non-voting or unknown server -> reject");
        return 0;
    }

    /* Ignore responses if we are not candidate anymore */
    if (r->state != RAFT_CANDIDATE) {
        assert(r->state == RAFT_LEADER || r->state == RAFT_FOLLOWER);
        infof("local server is %s -> ignore", raft_state_name(r->state));
        return 0;
    }

    /* If we're in the pre-vote phase, don't actually increment our term right
     * now (we'll do it later, if we start the second phase), and also don't
     * step down if the peer is just one term ahead (this is okay as in the
     * request we sent our current term plus one). */
    if (r->candidate_state.in_pre_vote) {
        recvCheckMatchingTerms(r, result->term, &match);
    } else {
        rv = recvEnsureMatchingTerms(r, result->term, &match);
        if (rv != 0) {
            return rv;
        }
    }

    /* Converted to follower as a result of seeing a higher term. */
    if (r->state != RAFT_CANDIDATE) {
        infof("no longer candidate -> ignore");
        return 0;
    }

    if (match < 0) {
        /* If the term in the result is older than ours, this is an old message
         * we should ignore, because the node who voted for us would have
         * obtained our term.  This happens if the network is pretty choppy. */
        infof("remote term is lower (%llu vs %llu) -> ignore", result->term,
              r->current_term);
        return 0;
    }

    /* Avoid counting pre-vote votes as regular votes. */
    if (result->version > 1 && result->pre_vote &&
        !r->candidate_state.in_pre_vote) {
        infof("receive stale pre-vote response -> ignore");
        return 0;
    }

    /* This can happen when a candidate wins a pre-vote, bumps its term,
     * sends real RequestVote RPCs, crashes, comes online, starts a pre-vote
     * and then receives the response to the RequestVote RPC it sent
     * out before crashing. */
    if (result->version > 1 && !result->pre_vote &&
        r->candidate_state.in_pre_vote) {
        infof("receive vote response during pre-vote -> ignore");
        return 0;
    }

    /* If we're in the pre-vote phase, check that the peer's is at most one term
     * ahead (possibly stepping down). If we're the actual voting phase, we
     * expect our term must to be the same as the response term (otherwise we
     * would have either ignored the result bumped our term). */
    if (r->candidate_state.in_pre_vote) {
        if (match > 0) {
            if (result->term > r->current_term + 1) {
                assert(!result->vote_granted);
                rv = recvBumpCurrentTerm(r, result->term);
                return rv;
            }
        }
    } else {
        assert(result->term == r->current_term);
    }

    /* If the vote was granted and we reached quorum, convert to leader.
     *
     * From Figure 3.1:
     *
     *   If votes received from majority of severs: become leader.
     *
     * From state diagram in Figure 3.3:
     *
     *   [candidate]: receives votes from majority of servers -> [leader]
     *
     * From Section 3.4:
     *
     *   A candidate wins an election if it receives votes from a majority of
     *   the servers in the full cluster for the same term. Each server will
     *   vote for at most one candidate in a given term, on a
     *   firstcome-first-served basis [...]. Once a candidate wins an election,
     *   it becomes leader.
     */
    if (result->vote_granted) {
        unsigned votes;
        unsigned n_voters;
        if (electionTally(r, votes_index, &votes, &n_voters)) {
            if (r->candidate_state.in_pre_vote) {
                infof("votes quorum reached -> pre-vote successful");
                r->candidate_state.in_pre_vote = false;
                electionStart(r);
            } else {
                infof(
                    "quorum reached with %u votes out of %u -> convert to "
                    "leader",
                    votes, n_voters);
                rv = convertToLeader(r);
                if (rv != 0) {
                    return rv;
                }
                /* Send initial heartbeat. */
                replicationHeartbeat(r);
            }
        } else {
            infof("quorum not reached, only %u votes out of %u", votes,
                  n_voters);
        }
    } else {
        infof("vote not granted");
    }

    return 0;
}

#undef infof
