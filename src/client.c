#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "legacy.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "tracing.h"

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
        goto err;
    }

    /* Rebuild the progress array if the new configuration has a different
     * number of servers than the old one. */
    if (configuration.n != r->configuration.n) {
        rv = progressRebuildArray(r, &configuration);
        if (rv != 0) {
            goto err_after_decode;
        }
    }

    /* Update the current configuration. */
    raft_configuration_close(&r->configuration);
    r->configuration = configuration;
    r->configuration_uncommitted_index = logLastIndex(r->log);

    return 0;

err_after_decode:
    configurationClose(&configuration);
err:
    assert(rv != 0);
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

    if (r->state != RAFT_LEADER || r->transfer != NULL) {
        rv = RAFT_NOTLEADER;
        ErrMsgFromCode(r->errmsg, rv);
        tracef("raft_apply not leader");
        goto err;
    }

    /* Index of the first entry being appended. */
    index = logLastIndex(r->log) + 1;

    if (n == 1) {
        infof("replicate 1 new entry (%llu^%llu)", index, entries[0].term);
    } else {
        infof("replicate %u new entries (%llu^%llu..%llu^%llu)", n, index,
              entries[0].term, index + n - 1, entries[n - 1].term);
    }

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];

        rv = logAppend(r->log, entry->term, entry->type, &entry->buf, NULL);
        if (rv != 0) {
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
        goto err_after_log_append;
    }

    return 0;

err_after_log_append:
    logDiscard(r->log, index);
err:
    assert(rv != 0);
    return rv;
}

int raft_apply(struct raft *r,
               struct raft_apply *req,
               const struct raft_buffer bufs[],
               const unsigned n,
               raft_apply_cb cb)
{
    raft_index index;
    struct raft_event event;
    struct raft_entry entry;
    int rv;

    tracef("raft_apply n %d", n);

    assert(r != NULL);
    assert(bufs != NULL);
    assert(n == 1);

    /* Index of the first entry being appended. */
    index = logLastIndex(r->log) + 1;
    req->type = RAFT_COMMAND;
    req->index = index;
    req->cb = cb;

    entry.type = RAFT_COMMAND;
    entry.term = r->current_term;
    entry.buf = bufs[0];
    entry.batch = NULL;

    event.time = r->io->time(r->io);
    event.type = RAFT_SUBMIT;
    event.submit.entries = &entry;
    event.submit.n = 1;

    rv = LegacyForwardToRaftIo(r, &event);
    if (rv != 0) {
        return rv;
    }

    QUEUE_PUSH(&r->legacy.pending, &req->queue);

    return 0;
}

int raft_barrier(struct raft *r, struct raft_barrier *req, raft_barrier_cb cb)
{
    struct raft_event event;
    struct raft_entry entry;
    raft_index index;
    int rv;

    /* Index of the barrier entry being appended. */
    index = logLastIndex(r->log) + 1;
    req->type = RAFT_BARRIER;
    req->index = index;
    req->cb = cb;

    entry.type = RAFT_BARRIER;
    entry.term = r->current_term;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);

    if (entry.buf.base == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    event.time = r->io->time(r->io);
    event.type = RAFT_SUBMIT;
    event.submit.entries = &entry;
    event.submit.n = 1;

    rv = LegacyForwardToRaftIo(r, &event);
    if (rv != 0) {
        goto err_after_buf_alloc;
    }

    QUEUE_PUSH(&r->legacy.pending, &req->queue);

    return 0;

err_after_buf_alloc:
    raft_free(entry.buf.base);
err:
    assert(rv != 0);
    return rv;
}

static int clientChangeConfiguration(
    struct raft *r,
    const struct raft_configuration *configuration)
{
    struct raft_entry entry;
    struct raft_event event;
    int rv;

    assert(r->state == RAFT_LEADER);

    entry.type = RAFT_CHANGE;
    entry.term = r->current_term;

    /* Encode the configuration. */
    rv = configurationEncode(configuration, &entry.buf);
    if (rv != 0) {
        return rv;
    }

    event.time = r->io->time(r->io);
    event.type = RAFT_SUBMIT;
    event.submit.entries = &entry;
    event.submit.n = 1;

    rv = LegacyForwardToRaftIo(r, &event);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_add(struct raft *r,
             struct raft_change *req,
             raft_id id,
             const char *address,
             raft_change_cb cb)
{
    struct raft_configuration configuration;
    int rv;

    rv = membershipCanChangeConfiguration(r);
    if (rv != 0) {
        return rv;
    }

    tracef("add server: id %llu, address %s", id, address);

    /* Make a copy of the current configuration, and add the new server to
     * it. */
    rv = configurationCopy(&r->configuration, &configuration);
    if (rv != 0) {
        goto err;
    }

    rv = raft_configuration_add(&configuration, id, address, RAFT_SPARE);
    if (rv != 0) {
        goto err_after_configuration_copy;
    }

    req->cb = cb;
    req->catch_up_id = 0;

    rv = clientChangeConfiguration(r, &configuration);
    if (rv != 0) {
        goto err_after_configuration_copy;
    }

    assert(r->legacy.change == NULL);
    r->legacy.change = req;

    raft_configuration_close(&configuration);

    return 0;

err_after_configuration_copy:
    raft_configuration_close(&configuration);
err:
    assert(rv != 0);
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

    last_index = logLastIndex(r->log);

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

int raft_assign(struct raft *r,
                struct raft_change *req,
                raft_id id,
                int role,
                raft_change_cb cb)
{
    const struct raft_server *server;
    struct raft_event event;
    unsigned server_index;
    raft_index last_index;
    int rv;

    r->now = r->io->time(r->io);

    tracef("raft_assign to id:%llu the role:%d", id, role);
    if (role != RAFT_STANDBY && role != RAFT_VOTER && role != RAFT_SPARE) {
        rv = RAFT_BADROLE;
        ErrMsgFromCode(r->errmsg, rv);
        return rv;
    }

    rv = membershipCanChangeConfiguration(r);
    if (rv != 0) {
        return rv;
    }

    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        rv = RAFT_NOTFOUND;
        ErrMsgPrintf(r->errmsg, "no server has ID %llu", id);
        goto err;
    }

    /* Check if we have already the desired role. */
    if (server->role == role) {
        const char *name;
        rv = RAFT_BADROLE;
        switch (role) {
            case RAFT_VOTER:
                name = "voter";
                break;
            case RAFT_STANDBY:
                name = "stand-by";
                break;
            case RAFT_SPARE:
                name = "spare";
                break;
            default:
                name = NULL;
                assert(0);
                break;
        }
        ErrMsgPrintf(r->errmsg, "server is already %s", name);
        goto err;
    }

    server_index = configurationIndexOf(&r->configuration, id);
    assert(server_index < r->configuration.n);

    last_index = logLastIndex(r->log);

    req->cb = cb;
    req->catch_up_id = 0;

    assert(r->legacy.change == NULL);
    r->legacy.change = req;

    /* If we are not promoting to the voter role or if the log of this server is
     * already up-to-date, we can submit the configuration change
     * immediately. */
    if (role != RAFT_VOTER ||
        progressMatchIndex(r, server_index) == last_index) {
        int old_role = r->configuration.servers[server_index].role;
        r->configuration.servers[server_index].role = role;

        rv = clientChangeConfiguration(r, &r->configuration);
        if (rv != 0) {
            tracef("clientChangeConfiguration failed %d", rv);
            r->configuration.servers[server_index].role = old_role;
            return rv;
        }

        return 0;
    }

    event.time = r->now;
    event.type = RAFT_CATCH_UP;
    event.catch_up.server_id = server->id;

    rv = LegacyForwardToRaftIo(r, &event);
    if (rv != 0) {
        return rv;
    }

    req->catch_up_id = server->id;

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int raft_remove(struct raft *r,
                struct raft_change *req,
                raft_id id,
                raft_change_cb cb)
{
    const struct raft_server *server;
    struct raft_configuration configuration;
    int rv;

    rv = membershipCanChangeConfiguration(r);
    if (rv != 0) {
        return rv;
    }

    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        rv = RAFT_BADID;
        goto err;
    }

    tracef("remove server: id %llu", id);

    /* Make a copy of the current configuration, and remove the given server
     * from it. */
    rv = configurationCopy(&r->configuration, &configuration);
    if (rv != 0) {
        goto err;
    }

    rv = configurationRemove(&configuration, id);
    if (rv != 0) {
        goto err_after_configuration_copy;
    }

    req->cb = cb;
    req->catch_up_id = 0;

    rv = clientChangeConfiguration(r, &configuration);
    if (rv != 0) {
        goto err_after_configuration_copy;
    }

    assert(r->legacy.change == NULL);
    r->legacy.change = req;

    raft_configuration_close(&configuration);

    return 0;

err_after_configuration_copy:
    raft_configuration_close(&configuration);

err:
    assert(rv != 0);
    return rv;
}

/* Find a suitable voting follower. */
static raft_id clientSelectTransferee(struct raft *r)
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

    if (progressIsUpToDate(r, i)) {
        rv = membershipLeadershipTransferStart(r);
        if (rv != 0) {
            r->transfer = NULL;
            goto err;
        }
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int raft_transfer(struct raft *r,
                  struct raft_transfer *req,
                  raft_id id,
                  raft_transfer_cb cb)
{
    const struct raft_server *server;
    struct raft_event event;
    unsigned i;
    int rv;

    tracef("transfer to %llu", id);
    if (r->state != RAFT_LEADER || r->transfer != NULL) {
        tracef("transfer error - state:%d", r->state);
        rv = RAFT_NOTLEADER;
        ErrMsgFromCode(r->errmsg, rv);
        goto err;
    }

    if (id == 0) {
        id = clientSelectTransferee(r);
        if (id == 0) {
            rv = RAFT_NOTFOUND;
            ErrMsgPrintf(r->errmsg, "there's no other voting server");
            goto err;
        }
    }

    server = configurationGet(&r->configuration, id);
    if (server == NULL || server->id == r->id || server->role != RAFT_VOTER) {
        rv = RAFT_BADID;
        ErrMsgFromCode(r->errmsg, rv);
        goto err;
    }

    /* If this follower is up-to-date, we can send it the TimeoutNow message
     * right away. */
    i = configurationIndexOf(&r->configuration, server->id);
    assert(i < r->configuration.n);

    membershipLeadershipTransferInit(r, req, id, cb);

    event.time = r->io->time(r->io);
    event.type = RAFT_TRANSFER;
    event.transfer.server_id = id;

    rv = LegacyForwardToRaftIo(r, &event);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}

#undef infof
#undef tracef
