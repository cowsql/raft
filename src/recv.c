#include "recv.h"

#include "assert.h"
#include "convert.h"
#include "entry.h"
#include "heap.h"
#include "membership.h"
#include "message.h"
#include "recv_append_entries.h"
#include "recv_append_entries_result.h"
#include "recv_install_snapshot.h"
#include "recv_request_vote.h"
#include "recv_request_vote_result.h"
#include "recv_timeout_now.h"
#include "string.h"
#include "tracing.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)

/* Dispatch a single RPC message to the appropriate handler. */
int recvMessage(struct raft *r, struct raft_message *message)
{
    raft_id id = message->server_id;
    const char *address = message->server_address;
    int rv = 0;

    switch (message->type) {
        case RAFT_APPEND_ENTRIES:
            rv = recvAppendEntries(r, id, address, &message->append_entries);
            break;
        case RAFT_APPEND_ENTRIES_RESULT:
            rv = recvAppendEntriesResult(r, id, address,
                                         &message->append_entries_result);
            break;
        case RAFT_REQUEST_VOTE:
            rv = recvRequestVote(r, id, address, &message->request_vote);
            break;
        case RAFT_REQUEST_VOTE_RESULT:
            rv = recvRequestVoteResult(r, id, address,
                                       &message->request_vote_result);
            break;
        case RAFT_INSTALL_SNAPSHOT:
            rv =
                recvInstallSnapshot(r, id, address, &message->install_snapshot);
            /* Already installing a snapshot, wait for it and ignore this one */
            if (rv == RAFT_BUSY) {
                raft_free(message->install_snapshot.data.base);
                raft_configuration_close(&message->install_snapshot.conf);
                rv = 0;
            }
            break;
        case RAFT_TIMEOUT_NOW:
            rv = recvTimeoutNow(r, id, address, &message->timeout_now);
            break;
        default:
            /* Drop message */
            return 0;
    };

    if (rv != 0 && rv != RAFT_NOCONNECTION) {
        return rv;
    }

    return 0;
}

void recvBumpCurrentTerm(struct raft *r, raft_term term)
{
    char msg[128];

    assert(r != NULL);
    assert(term > r->current_term);

    sprintf(msg, "remote term is higher (%lld vs %lld) -> bump term", term,
            r->current_term);
    if (r->state != RAFT_FOLLOWER) {
        strcat(msg, ", step down");
    }
    infof("%s", msg);

    /* Mark both the current term and vote as changed. */
    r->update->flags |= RAFT_UPDATE_CURRENT_TERM | RAFT_UPDATE_VOTED_FOR;

    /* Update our cache too. */
    r->current_term = term;
    r->voted_for = 0;

    if (r->state != RAFT_FOLLOWER) {
        /* Also convert to follower. */
        convertToFollower(r);
    }

    /* Reset the match index, because we don't know anything about the leader of
     * this new term yet. */
    r->follower_state.match = 0;
}

int recvCheckMatchingTerms(const struct raft *r, raft_term term)
{
    int match;

    if (term < r->current_term) {
        match = -1;
    } else if (term > r->current_term) {
        match = 1;
    } else {
        match = 0;
    }

    return match;
}

int recvEnsureMatchingTerms(struct raft *r, raft_term term)
{
    int match;

    assert(r != NULL);

    match = recvCheckMatchingTerms(r, term);

    if (match == -1) {
        goto out;
    }

    /* From Figure 3.1:
     *
     *   Rules for Servers: All Servers: If RPC request or response contains
     *   term T > currentTerm: set currentTerm = T, convert to follower.
     *
     * From state diagram in Figure 3.3:
     *
     *   [leader]: discovers server with higher term -> [follower]
     *
     * From Section 3.3:
     *
     *   If a candidate or leader discovers that its term is out of date, it
     *   immediately reverts to follower state.
     */
    if (match == 1) {
        recvBumpCurrentTerm(r, term);
    }

out:
    return match;
}

int recvUpdateLeader(struct raft *r, const raft_id id, const char *address)
{
    assert(r->state == RAFT_FOLLOWER);

    r->follower_state.current_leader.id = id;

    /* If the address of the current leader is the same as the given one, we're
     * done. */
    if (r->follower_state.current_leader.address != NULL &&
        strcmp(address, r->follower_state.current_leader.address) == 0) {
        return 0;
    }

    if (r->follower_state.current_leader.address != NULL) {
        RaftHeapFree(r->follower_state.current_leader.address);
    }
    r->follower_state.current_leader.address =
        RaftHeapMalloc(strlen(address) + 1);
    if (r->follower_state.current_leader.address == NULL) {
        return RAFT_NOMEM;
    }
    strcpy(r->follower_state.current_leader.address, address);

    return 0;
}

#undef infof
