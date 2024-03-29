#include "recv_timeout_now.h"

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "recv.h"
#include "tracing.h"
#include "trail.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)

int recvTimeoutNow(struct raft *r,
                   const raft_id id,
                   const char *address,
                   const struct raft_timeout_now *args)
{
    const struct raft_server *local_server;
    raft_index local_last_index;
    raft_term local_last_term;
    int match;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);

    (void)address;

    /* Ignore the request if we are not voters. */
    local_server = configurationGet(&r->configuration, r->id);
    if (local_server == NULL || local_server->role != RAFT_VOTER) {
        infof("non-voter");
        return 0;
    }

    /* Ignore the request if we are not follower, or we have different
     * leader. */
    if (r->state != RAFT_FOLLOWER ||
        r->follower_state.current_leader.id != id) {
        infof("ignore - r->state:%d current_leader.id:%llu", r->state,
              r->follower_state.current_leader.id);
        return 0;
    }

    /* Possibly update our term. Ignore the request if it turns out we have a
     * higher term. */
    match = recvEnsureMatchingTerms(r, args->term);
    if (match < 0) {
        return 0;
    }

    /* Ignore the request if we our log is not up-to-date. */
    local_last_index = TrailLastIndex(&r->trail);
    local_last_term = TrailLastTerm(&r->trail);
    if (local_last_index != args->last_log_index ||
        local_last_term != args->last_log_term) {
        return 0;
    }

    /* Ignore the request if we're still persisting entries or installing a
     * snapshot. */
    if (r->last_stored < local_last_index || r->snapshot.installing) {
        return 0;
    }

    /* Convert to candidate and start a new election. */
    infof("convert to candidate, start election for term %llu",
          r->current_term + 1);
    rv = convertToCandidate(r, true /* disrupt leader */);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef infof
