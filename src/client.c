#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "tracing.h"
#include "trail.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* This function is called when a new configuration entry is being submitted. It
 * updates the progress array and it switches the current configuration to the
 * new one. */
static int clientSubmitConfiguration(struct raft *r, struct raft_entry *entry)
{
    struct raft_configuration configuration;
    int rv;

    assert(entry->type == RAFT_CHANGE);

    rv = configurationDecode(&entry->buf, &configuration);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM || rv == RAFT_MALFORMED);
        goto err;
    }

    /* Rebuild the progress array if the new configuration has a different
     * number of servers than the old one. */
    if (configuration.n != r->configuration.n) {
        rv = progressRebuildArray(r, &configuration);
        if (rv != 0) {
            assert(rv == RAFT_NOMEM);
            goto err_after_decode;
        }
    }

    /* Update the current configuration. */
    raft_configuration_close(&r->configuration);
    r->configuration = configuration;
    r->configuration_uncommitted_index = TrailLastIndex(&r->trail);

    return 0;

err_after_decode:
    configurationClose(&configuration);
err:
    assert(rv == RAFT_NOMEM || rv == RAFT_MALFORMED);
    return rv;
}

int ClientSubmit(struct raft *r, struct raft_entry *entries, unsigned n)
{
    raft_index index;
    unsigned i;
    int rv;

    assert(r != NULL);
    assert(entries != NULL);
    assert(n > 0);

    if (r->state != RAFT_LEADER || r->leader_state.transferee != 0) {
        rv = RAFT_NOTLEADER;
        ErrMsgFromCode(r->errmsg, rv);
        goto err;
    }

    /* Index of the first entry being appended. */
    index = TrailLastIndex(&r->trail) + 1;

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

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];

        if (entry->type == RAFT_CHANGE) {
            rv = membershipCanChangeConfiguration(r);
            if (rv != 0) {
                return rv;
            }
        }

        rv = logAppend(r->log, entry->term, entry->type, &entry->buf, NULL);
        if (rv != 0) {
            /* This logAppend call can't fail with RAFT_BUSY, because these are
             * brand new entries. */
            assert(rv == RAFT_NOMEM);
            goto err_after_log_append;
        }
        rv = TrailAppend(&r->trail, entry->term);
        if (rv != 0) {
            assert(rv == RAFT_NOMEM);
            goto err_after_log_append;
        }

        if (entry->type == RAFT_CHANGE) {
            rv = clientSubmitConfiguration(r, entry);
            if (rv != 0) {
                goto err_after_log_append;
            }
        }
    }

    rv = replicationTrigger(r, index);
    if (rv != 0) {
        /* TODO: assert the possible error values */
        goto err_after_log_append;
    }

    return 0;

err_after_log_append:
    logDiscard(r->log, index);
err:
    assert(rv == RAFT_NOTLEADER || rv == RAFT_MALFORMED || rv == RAFT_NOMEM);
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
raft_id clientSelectTransferee(struct raft *r)
{
    const struct raft_server *transferee = NULL;
    unsigned i;

    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id || server->role != RAFT_VOTER) {
            continue;
        }
        transferee = server;
        if (progressIsUpToDate(r, i)) {
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

    if (progressIsUpToDate(r, i)) {
        rv = membershipLeadershipTransferStart(r);
        if (rv != 0) {
            r->leader_state.transferee = 0;
            goto err;
        }
    } else {
        infof("wait for transferee to catch up");
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}

#undef infof
#undef tracef
