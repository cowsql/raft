#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "membership.h"
#include "message.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "tracing.h"
#include "trail.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* Emit a trace info message summarizing the entries being submmitted. */
static void clientEmitSubmissionMessage(const struct raft *r,
                                        const raft_index index,
                                        const struct raft_entry *entries,
                                        const unsigned n)
{
    if (n == 1) {
        const char *type;
        switch (entries[0].type) {
            case RAFT_COMMAND:
                type = "command";
                break;
            case RAFT_BARRIER:
                type = "barrier";
                break;
            case RAFT_CHANGE:
                type = "configuration";
                break;
            default:
                type = "unknown";
                break;
        }
        infof("replicate 1 new %s entry (%llu^%llu)", type, index,
              entries[0].term);
    } else {
        infof("replicate %u new entries (%llu^%llu..%llu^%llu)", n, index,
              entries[0].term, index + n - 1, entries[n - 1].term);
    }
}

/* Return true if the capacity of the majority of voter servers is within the
 * configured threshold. */
static bool clientCapacityIsWithinThreshold(const struct raft *r)
{
    unsigned reporting = 0; /* N. of voters reporting capacity. */
    unsigned healthy = 0;   /* N. of voters with capacity above threshold. */
    unsigned i;

    /* If a capacity threshold is not set, don't perform any check. */
    if (r->capacity_threshold == 0) {
        return true;
    }

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        unsigned features = progressGetFeatures(r, i);

        if (server->role != RAFT_VOTER) {
            continue;
        }

        if (!(features & MESSAGE__FEATURE_CAPACITY)) {
            continue;
        }

        reporting += 1;

        if (progressGetCapacity(r, i) >= r->capacity_threshold) {
            healthy += 1;
        }
    }

    /* If not enough nodes are actually reporting capacity, don't draw any bad
     * conclusion. */
    if (reporting <= configurationVoterCount(&r->configuration) / 2) {
        return true;
    }

    return healthy > configurationVoterCount(&r->configuration) / 2;
}

int ClientSubmit(struct raft *r, struct raft_entry *entries, unsigned n)
{
    const raft_index index = TrailLastIndex(&r->trail) + 1; /* 1st new entry */
    unsigned i;
    int rv;

    assert(r != NULL);
    assert(entries != NULL);
    assert(n > 0);
    assert(entries[0].batch != NULL);

    if (r->state != RAFT_LEADER || r->leader_state.transferee != 0) {
        rv = RAFT_NOTLEADER;
        ErrMsgFromCode(r->errmsg, rv);
        goto err;
    }

    if (!clientCapacityIsWithinThreshold(r)) {
        rv = RAFT_NOSPACE;
        ErrMsgFromCode(r->errmsg, rv);
        goto err;
    }

    clientEmitSubmissionMessage(r, index, entries, n);

    for (i = 0; i < n; i++) {
        const struct raft_entry *entry = &entries[i];

        if (entry->type == RAFT_CHANGE) {
            rv = membershipCanChangeConfiguration(r);
            if (rv != 0) {
                assert(rv == RAFT_CANTCHANGE);
                goto err;
            }
        }

        rv = TrailAppend(&r->trail, entry->term);
        if (rv != 0) {
            assert(rv == RAFT_NOMEM);
            goto err;
        }

        if (entry->type == RAFT_CHANGE) {
            rv = membershipUncommittedChange(r, index + i, entry);
            if (rv != 0) {
                goto err_after_trail_append;
            }
        }
    }

    rv = replicationTrigger(r, index, entries, n);
    if (rv != 0) {
        /* TODO: assert the possible error values */
        goto err_after_trail_append;
    }

    return 0;

err_after_trail_append:
    TrailTruncate(&r->trail, index);
err:
    assert(rv == RAFT_NOTLEADER || rv == RAFT_MALFORMED || rv == RAFT_NOMEM ||
           rv == RAFT_NOSPACE || rv == RAFT_CANTCHANGE);
    return rv;
}

void ClientCatchUp(struct raft *r, raft_id server_id)
{
    const struct raft_server *server;
    unsigned server_index;
    raft_index last_index;
    int rv;

    server = configurationGet(&r->configuration, server_id);
    assert(server != NULL);

    server_index = configurationIndexOf(&r->configuration, server_id);

    last_index = TrailLastIndex(&r->trail);

    r->leader_state.promotee_id = server->id;

    /* Initialize the first catch-up round. */
    r->leader_state.round_number = 1;
    r->leader_state.round_index = last_index;
    r->leader_state.round_start = r->now;

    progressCatchUpStart(r, server_index);

    /* Immediately initiate an AppendEntries request. */
    rv = replicationProgress(r, server_index);
    if (rv != 0 && rv != RAFT_NOCONNECTION) {
        /* This error is not fatal. */
        tracef("failed to send append entries to server %llu: %s (%d)",
               server->id, raft_strerror(rv), rv);
    }
}

/* Find a suitable voting follower. */
static raft_id clientSelectTransferee(const struct raft *r)
{
    const struct raft_server *transferee = NULL;
    unsigned i;

    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id || server->role != RAFT_VOTER) {
            continue;
        }
        transferee = server;
        if (progressMatchIndex(r, i) == TrailLastIndex(&r->trail)) {
            break;
        }
    }

    if (transferee != NULL) {
        return transferee->id;
    }

    return 0;
}

int ClientTransfer(struct raft *r, raft_id server_id)
{
    const struct raft_server *server;
    unsigned i;
    int rv;

    if (r->state != RAFT_LEADER || r->leader_state.transferee != 0) {
        rv = RAFT_NOTLEADER;
        ErrMsgFromCode(r->errmsg, rv);
        goto err;
    }

    if (server_id == 0) {
        server_id = clientSelectTransferee(r);
        if (server_id == 0) {
            rv = RAFT_NOTFOUND;
            ErrMsgPrintf(r->errmsg, "there's no other voting server");
            goto err;
        }
    }

    server = configurationGet(&r->configuration, server_id);
    if (server == NULL || server->id == r->id || server->role != RAFT_VOTER) {
        rv = RAFT_BADID;
        ErrMsgFromCode(r->errmsg, rv);
        goto err;
    }

    /* If this follower is up-to-date, we can send it the TimeoutNow message
     * right away. */
    i = configurationIndexOf(&r->configuration, server->id);
    assert(i < r->configuration.n);

    r->leader_state.transferee = server_id;
    r->leader_state.transfer_start = r->now;

    if (progressMatchIndex(r, i) == TrailLastIndex(&r->trail)) {
        rv = membershipLeadershipTransferStart(r);
        if (rv != 0) {
            assert(rv == RAFT_NOMEM);
            r->leader_state.transferee = 0;
            goto err;
        }
    } else {
        infof("wait for transferee to catch up");
    }

    return 0;

err:
    assert(rv == RAFT_NOTLEADER || rv == RAFT_NOTFOUND || rv == RAFT_BADID ||
           rv == RAFT_NOMEM);
    return rv;
}

#undef infof
#undef tracef
