#include "legacy.h"
#include "assert.h"
#include "client.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#include "err.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "snapshot.h"
#include "tracing.h"

#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

struct legacySendMessage
{
    struct raft_io_send send;
    struct raft_io_snapshot_get get;
    struct raft *r;
    struct raft_message message;
};

static void legacySendMessageCb(struct raft_io_send *send, int status)
{
    struct legacySendMessage *req = send->data;
    struct raft *r = req->r;

    (void)status;

    switch (req->message.type) {
        case RAFT_IO_APPEND_ENTRIES:
            logRelease(r->legacy.log,
                       req->message.append_entries.prev_log_index + 1,
                       req->message.append_entries.entries,
                       req->message.append_entries.n_entries);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            configurationClose(&req->message.install_snapshot.conf);
            raft_free(req->message.install_snapshot.data.base);
            break;
    }

    raft_free(req);
}

static int legacyLoadSnapshot(struct legacySendMessage *req);

static int legacyFillAppendEntries(struct raft *r,
                                   struct raft_append_entries *args)
{
    raft_index index = args->prev_log_index + 1;
    unsigned n = args->n_entries;
    int rv;
    rv = logAcquireAtMost(r->legacy.log, index, (int)n, &args->entries,
                          &args->n_entries);
    if (rv != 0) {
        return rv;
    }
    assert(args->n_entries == n);
    return 0;
}

static void legacyAbortAppendEntries(struct raft *r,
                                     struct raft_append_entries *args)
{
    raft_index index = args->prev_log_index + 1;
    logRelease(r->legacy.log, index, args->entries, args->n_entries);
}

static int legacySendMessage(struct raft *r, struct raft_message *message)
{
    struct legacySendMessage *req;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->message = *message;
    req->send.data = req;

    switch (req->message.type) {
        case RAFT_IO_APPEND_ENTRIES:
            rv = legacyFillAppendEntries(r, &req->message.append_entries);
            if (rv != 0) {
                return rv;
            }
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            rv = legacyLoadSnapshot(req);
            if (rv != 0) {
                return rv;
            }
            return 0;
    }

    rv = r->io->send(r->io, &req->send, &req->message, legacySendMessageCb);
    if (rv != 0) {
        switch (req->message.type) {
            case RAFT_IO_APPEND_ENTRIES:
                legacyAbortAppendEntries(r, &req->message.append_entries);
                break;
        }
        raft_free(req);
        ErrMsgTransferf(r->io->errmsg, r->errmsg,
                        "send message of type %d to %llu", message->type,
                        message->server_id);
        return rv;
    }

    return 0;
}

struct legacyPersistEntries
{
    struct raft_io_append append;
    struct raft *r;
    raft_index index;
    struct raft_entry *entries;
    unsigned n;
};

static void legacyPersistEntriesCb(struct raft_io_append *append, int status)
{
    struct legacyPersistEntries *req = append->data;
    struct raft *r = req->r;
    struct raft_event event;

    if (status != 0) {
        assert(r->legacy.closing);
        assert(status == RAFT_CANCELED);
        if (req->index <= logLastIndex(r->legacy.log)) {
            goto out;
        }
    }

    event.type = RAFT_PERSISTED_ENTRIES;
    event.persisted_entries.index = req->index;
    event.persisted_entries.batch = req->entries;
    event.persisted_entries.n = req->n;

    LegacyForwardToRaftIo(r, &event);

out:
    raft_free(req);
    logRelease(r->legacy.log, event.persisted_entries.index,
               event.persisted_entries.batch, event.persisted_entries.n);
}

static int legacyHandleUpdateEntries(struct raft *r,
                                     raft_index index,
                                     struct raft_entry *entries,
                                     unsigned n)
{
    struct legacyPersistEntries *req;
    struct raft_entry *acquired;
    unsigned n_acquired;
    unsigned i;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->index = index;
    req->n = n;
    req->append.data = req;

    if (index <= logLastIndex(r->legacy.log)) {
        logTruncate(r->legacy.log, index);
    }

    for (i = 0; i < n; i++) {
        struct raft_entry entry;
        rv = entryCopy(&entries[i], &entry);
        if (rv != 0) {
            goto err;
        }
        rv = logAppend(r->legacy.log, entry.term, entry.type, &entry.buf, NULL);
        if (rv != 0) {
            goto err;
        }
    }

    if (n > 0) {
        assert(entries[0].batch != NULL);
        raft_free(entries[0].batch);
    }

    rv = r->io->truncate(r->io, index);
    if (rv != 0) {
        goto err;
    }

    rv = logAcquire(r->legacy.log, index, &acquired, &n_acquired);
    assert(n_acquired == n);
    if (rv != 0) {
        goto err;
    }

    req->entries = acquired;

    rv =
        r->io->append(r->io, &req->append, acquired, n, legacyPersistEntriesCb);
    if (rv != 0) {
        goto err_after_acquired;
    }

    return 0;

err_after_acquired:
    logRelease(r->legacy.log, index, acquired, n_acquired);
err:
    logDiscard(r->legacy.log, index);
    raft_free(req);
    ErrMsgTransferf(r->io->errmsg, r->errmsg, "append %u entries", n);
    return rv;
}

struct legacyPersistSnapshot
{
    struct raft_io_snapshot_put put;
    struct raft_snapshot snapshot;
    struct raft *r;
    struct raft_snapshot_metadata metadata;
    size_t offset;
    struct raft_buffer chunk;
    bool last;
};

static void legacyPersistSnapshotCb(struct raft_io_snapshot_put *put,
                                    int status)
{
    struct legacyPersistSnapshot *req = put->data;
    struct raft *r = req->r;
    struct raft_event event;

    event.type = RAFT_PERSISTED_SNAPSHOT;
    event.persisted_snapshot.metadata = req->metadata;
    event.persisted_snapshot.offset = req->offset;
    event.persisted_snapshot.chunk = req->chunk;
    event.persisted_snapshot.last = req->last;
    event.persisted_snapshot.status = status;

    /* If we successfully persisted the snapshot, keep the snapshot data around,
     * since we'll then need it immediately after calling raft_step(), in order
     * to restore the FSM state.
     *
     * Otherwise, discard the snapshot data altogether. */
    if (status == 0) {
        assert(r->legacy.snapshot_index == 0);
        r->legacy.snapshot_index = req->metadata.index;
        r->legacy.snapshot_chunk = req->chunk;
    } else {
        raft_free(req->chunk.base);
    }

    raft_free(req);

    LegacyForwardToRaftIo(r, &event);
}

static int legacyHandleUpdateSnapshot(struct raft *r,
                                      struct raft_snapshot_metadata *metadata,
                                      size_t offset,
                                      struct raft_buffer *chunk,
                                      bool last)
{
    struct legacyPersistSnapshot *req;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->metadata = *metadata;
    req->offset = offset;
    req->chunk = *chunk;
    req->last = last;
    req->put.data = req;

    req->snapshot.index = req->metadata.index;
    req->snapshot.term = req->metadata.term;
    req->snapshot.configuration = req->metadata.configuration;
    req->snapshot.configuration_index = req->metadata.configuration_index;
    req->snapshot.bufs = &req->chunk;
    req->snapshot.n_bufs = 1;

    logRestore(r->legacy.log, req->metadata.index, req->metadata.term);

    rv = r->io->snapshot_put(r->io, 0, &req->put, &req->snapshot,
                             legacyPersistSnapshotCb);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    raft_free(req);
    ErrMsgTransferf(r->io->errmsg, r->errmsg, "put snapshot at %llu",
                    req->metadata.index);
    return rv;
}

static int legacyHandleUpdateMessages(struct raft *r,
                                      struct raft_message *messages,
                                      unsigned n)
{
    unsigned i;
    int rv;
    for (i = 0; i < n; i++) {
        rv = legacySendMessage(r, &messages[i]);
        if (rv != 0) {
            return rv;
        }
    }
    return 0;
}

static void legacyLoadSnapshotCb(struct raft_io_snapshot_get *get,
                                 struct raft_snapshot *snapshot,
                                 int status)
{
    struct legacySendMessage *req = get->data;
    struct raft *r = req->r;
    struct raft_install_snapshot *params = &req->message.install_snapshot;
    int rv;

    if (status != 0) {
        goto abort;
    }

    /* The old raft_io interface makes no guarantee about the index of the
     * loaded snapshot. */
    if (snapshot->index != params->last_index) {
        assert(snapshot->index > params->last_index);
        params->last_index = snapshot->index;
    }

    assert(snapshot->n_bufs == 1);
    params->data = snapshot->bufs[0];
    params->conf = snapshot->configuration;
    params->conf_index = snapshot->configuration_index;

    raft_free(snapshot->bufs);
    raft_free(snapshot);

    rv = r->io->send(r->io, &req->send, &req->message, legacySendMessageCb);
    if (rv != 0) {
        ErrMsgTransferf(r->io->errmsg, r->errmsg,
                        "send message of type %d to %llu", req->message.type,
                        req->message.server_id);
        status = rv;
        goto abort;
    }

    return;

abort:
    configurationClose(&params->conf);
    raft_free(params->data.base);

    raft_free(req);
}

static int legacyLoadSnapshot(struct legacySendMessage *req)
{
    struct raft *r = req->r;
    int rv;

    req->get.data = req;

    rv = r->io->snapshot_get(r->io, &req->get, legacyLoadSnapshotCb);
    if (rv != 0) {
        raft_free(req);
        ErrMsgTransferf(r->io->errmsg, r->errmsg, "load snapshot at %llu",
                        req->message.install_snapshot.last_index);
        return rv;
    }

    return 0;
}

struct legacyTakeSnapshot
{
    struct raft *r;
    struct raft_snapshot_metadata metadata;
    struct raft_snapshot snapshot;
    struct raft_io_snapshot_put put;
};

/*
 * When taking a snapshot, ownership of the snapshot data is with raft if
 * `snapshot_finalize` is NULL.
 */
static void takeSnapshotClose(struct raft *r, struct raft_snapshot *s)
{
    r->snapshot.taking = false;

    if (r->fsm->version == 1 ||
        (r->fsm->version > 1 && r->fsm->snapshot_finalize == NULL)) {
        unsigned i;
        for (i = 0; i < s->n_bufs; i++) {
            raft_free(s->bufs[i].base);
        }
        raft_free(s->bufs);
        return;
    }

    r->fsm->snapshot_finalize(r->fsm, &s->bufs, &s->n_bufs);
}

static void takeSnapshotCb(struct raft_io_snapshot_put *put, int status)
{
    struct legacyTakeSnapshot *req = put->data;
    struct raft *r = req->r;
    struct raft_snapshot_metadata metadata = req->metadata;
    struct raft_snapshot *snapshot = &req->snapshot;
    struct raft_event event;

    r->snapshot.persisting = false;

    takeSnapshotClose(r, snapshot);
    raft_free(req);

    /* If we are shutting down, cancel the snapshot.
     *
     * Or the snapshot's index might not be in the log anymore because the
     * associated entry failed to be persisted and got truncated (TODO: we
     * should retry instead of truncating). */
    assert(metadata.term != 0);
    if (r->legacy.closing ||
        logTermOf(r->legacy.log, metadata.index) != metadata.term) {
        tracef("cancelling snapshot");
        status = RAFT_CANCELED;
    }

    if (status != 0) {
        tracef("snapshot %lld at term %lld: %s", metadata.index, metadata.term,
               raft_strerror(status));
        configurationClose(&metadata.configuration);
        return;
    }

    logSnapshot(r->legacy.log, metadata.index, r->snapshot.trailing);

    event.type = RAFT_SNAPSHOT;
    memset(&event.reserved, 0, sizeof event.reserved);
    event.snapshot.metadata = metadata;
    event.snapshot.trailing = 0;
    LegacyForwardToRaftIo(r, &event);
}

static int putSnapshot(struct legacyTakeSnapshot *req)
{
    struct raft *r = req->r;
    struct raft_snapshot *snapshot = &req->snapshot;
    int rv;
    assert(!r->snapshot.persisting);
    req->put.data = req;
    r->snapshot.persisting = true;
    rv = r->io->snapshot_put(r->io, r->snapshot.trailing, &req->put, snapshot,
                             takeSnapshotCb);
    if (rv != 0) {
        r->snapshot.persisting = false;
    }
    return rv;
}

static bool legacyShouldTakeSnapshot(const struct raft *r)
{
    /* We currently support only synchronous FSMs, where entries are applied
     * synchronously as soon as we advance the commit index, so the two
     * values always match when we get here. */
    if (r->last_applied < r->commit_index) {
        return false;
    }

    /* If we are shutting down, let's not do anything. */
    if (r->legacy.closing) {
        return false;
    }

    /* If a snapshot is already in progress or we're installing a snapshot, we
     * don't want to start another one. */
    if (r->snapshot.taking || r->snapshot.persisting) {
        return false;
    };

    /* If we didn't reach the threshold yet, do nothing. */
    if (r->commit_index - r->legacy.log->snapshot.last_index <
        r->snapshot.threshold) {
        return false;
    }

    /* If the last committed index is not anymore in our log, it means that the
     * log got truncated because we have received an InstallSnapshot
     * message. Don't take a snapshot now.*/
    if (logTermOf(r->legacy.log, r->commit_index) == 0) {
        return false;
    }

    return true;
}

static void legacyTakeSnapshot(struct raft *r)
{
    struct raft_snapshot_metadata metadata;
    struct raft_snapshot *snapshot;
    struct legacyTakeSnapshot *req;
    int rv;

    /* We currently support only synchronous FSMs, where entries are applied
     * synchronously as soon as we advance the commit index, so the two
     * values always match when we get here. */
    assert(r->last_applied == r->commit_index);

    assert(!r->snapshot.persisting);

    tracef("take snapshot at %lld", r->commit_index);

    metadata.index = r->commit_index;
    metadata.term = logTermOf(r->legacy.log, r->commit_index);

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        goto abort;
    }
    req->r = r;

    rv = membershipFetchLastCommittedConfiguration(r, &metadata.configuration);
    if (rv != 0) {
        goto abort_after_req_alloc;
    }
    metadata.configuration_index = r->configuration_committed_index;

    req->metadata = metadata;

    snapshot = &req->snapshot;
    snapshot->index = metadata.index;
    snapshot->term = metadata.term;
    snapshot->configuration = metadata.configuration;
    snapshot->configuration_index = metadata.configuration_index;
    snapshot->bufs = NULL;
    snapshot->n_bufs = 0;

    rv = r->fsm->snapshot(r->fsm, &snapshot->bufs, &snapshot->n_bufs);
    if (rv == 0 && r->fsm->version >= 3 && r->fsm->snapshot_async != NULL) {
        rv = r->fsm->snapshot_async(r->fsm, &snapshot->bufs, &snapshot->n_bufs);
    }
    if (rv != 0) {
        ErrMsgTransferf(r->io->errmsg, r->errmsg, "load snapshot at %llu",
                        metadata.index);
        goto abort_after_conf_fetched;
    }

    /* putSnapshot will clean up config and buffers in case of error */
    rv = putSnapshot(req);
    if (rv != 0) {
        goto abort_after_snapshot;
    }

    r->snapshot.taking = true;

    return;

abort_after_snapshot:
    takeSnapshotClose(r, snapshot);
abort_after_conf_fetched:
    configurationClose(&metadata.configuration);
abort_after_req_alloc:
    raft_free(req);
abort:
    return;
}

static void legacyFailApply(struct raft *r, struct raft_apply *req)
{
    if (req != NULL && req->cb != NULL) {
        req->status = RAFT_LEADERSHIPLOST;
        req->result = NULL;
        QUEUE_PUSH(&r->legacy.requests, &req->queue);
    }
}

static void legacyFailBarrier(struct raft *r, struct raft_barrier *req)
{
    if (req != NULL && req->cb != NULL) {
        req->status = RAFT_LEADERSHIPLOST;
        QUEUE_PUSH(&r->legacy.requests, &req->queue);
    }
}

void LegacyFailPendingRequests(struct raft *r)
{
    /* Fail any promote request that is still outstanding because the server is
     * still catching up and no entry was submitted. */
    if (r->legacy.change != NULL) {
        struct raft_change *req = r->legacy.change;
        if (req != NULL && req->cb != NULL) {
            /* XXX: set the type here, since it's not done in client.c */
            req->type = RAFT_CHANGE;
            req->status = RAFT_LEADERSHIPLOST;
            QUEUE_PUSH(&r->legacy.requests, &req->queue);
        }
        r->legacy.change = NULL;
    }

    /* Fail all outstanding requests */
    while (!QUEUE_IS_EMPTY(&r->legacy.pending)) {
        struct request *req;
        queue *head;
        head = QUEUE_HEAD(&r->legacy.pending);
        QUEUE_REMOVE(head);
        req = QUEUE_DATA(head, struct request, queue);
        assert(req->type == RAFT_COMMAND || req->type == RAFT_BARRIER);
        switch (req->type) {
            case RAFT_COMMAND:
                legacyFailApply(r, (struct raft_apply *)req);
                break;
            case RAFT_BARRIER:
                legacyFailBarrier(r, (struct raft_barrier *)req);
                break;
        };
    }
}

static void legacyFireApply(struct raft_apply *req)
{
    req->cb(req, req->status, req->result);
}

static void legacyFireBarrier(struct raft_barrier *req)
{
    req->cb(req, req->status);
}

static void legacyFireChange(struct raft_change *req)
{
    req->cb(req, req->status);
}

static void legacyFireTransfer(struct raft_transfer *req)
{
    req->cb(req);
}

void LegacyFireCompletedRequests(struct raft *r)
{
    while (!QUEUE_IS_EMPTY(&r->legacy.requests)) {
        struct request *req;
        queue *head;
        head = QUEUE_HEAD(&r->legacy.requests);
        QUEUE_REMOVE(head);
        req = QUEUE_DATA(head, struct request, queue);
        switch (req->type) {
            case RAFT_COMMAND:
                legacyFireApply((struct raft_apply *)req);
                break;
            case RAFT_BARRIER:
                legacyFireBarrier((struct raft_barrier *)req);
                break;
            case RAFT_CHANGE:
                legacyFireChange((struct raft_change *)req);
                break;
            case RAFT_TRANSFER_:
                legacyFireTransfer((struct raft_transfer *)req);
                break;
            default:
                tracef("unknown request type, shutdown.");
                assert(false);
                break;
        };
    }
}

/* Check whether a raft_change request has been completed, and put it in the
 * completed requests queue if so. */
static void legacyCheckChangeRequest(struct raft *r,
                                     struct raft_entry *entry,
                                     struct raft_event **events,
                                     unsigned *n_events)
{
    struct raft_change *change;
    int status;
    int rv;

    if (r->legacy.change == NULL) {
        return;
    }

    if (r->legacy.change->catch_up_id == 0) {
        return;
    }

    change = r->legacy.change;

    /* A raft_catch_up() call can fail only if the server is not the
     * leader or if the given ID is invalid. If the server was not the
     * leader then r->legacy.change would be NULL, and we know that the
     * ID is valid, otherwise the request couldn't have been submitted.
     */
    rv = raft_catch_up(r, r->legacy.change->catch_up_id, &status);
    assert(rv == 0);

    if (status == RAFT_CATCH_UP_ABORTED) {
        r->legacy.change = NULL;
        if (change->cb != NULL) {
            change->type = RAFT_CHANGE;
            change->status = RAFT_NOCONNECTION;
            QUEUE_PUSH(&r->legacy.requests, &change->queue);
        }
    }

    if (status == RAFT_CATCH_UP_FINISHED) {
        struct raft_configuration configuration;
        struct raft_server *server;
        struct raft_event *event;
        unsigned i;

        /* If we're transferring leadership, fail the request. */
        if (raft_transferee(r) != 0) {
            r->legacy.change = NULL;
            if (change->cb != NULL) {
                change->type = RAFT_CHANGE;
                change->status = RAFT_LEADERSHIPLOST;
                QUEUE_PUSH(&r->legacy.requests, &change->queue);
            }
            return;
        }

        i = configurationIndexOf(&r->configuration, change->catch_up_id);
        assert(i < r->configuration.n);

        server = &r->configuration.servers[i];
        assert(server->role != RAFT_VOTER);

        change->catch_up_id = 0;

        /* Update our current configuration. */
        rv = configurationCopy(&r->configuration, &configuration);
        assert(rv == 0);

        configuration.servers[i].role = RAFT_VOTER;

        entry->type = RAFT_CHANGE;
        entry->term = r->current_term;

        /* Encode the configuration. */
        rv = configurationEncode(&configuration, &entry->buf);
        assert(rv == 0);

        entry->batch = entry->buf.base;

        *n_events += 1;
        *events = raft_realloc(*events, *n_events * sizeof **events);
        assert(*events != NULL);

        event = &(*events)[*n_events - 1];
        event->type = RAFT_SUBMIT;
        event->submit.entries = entry;
        event->submit.n = 1;

        configurationClose(&configuration);
    }
}

/* Get the request matching the given @index and @type, if any.
 * The type check is skipped when @type == -1. */
static struct request *legacyGetRequest(struct raft *r,
                                        const raft_index index,
                                        int type)
{
    queue *head;
    struct request *req;

    QUEUE_FOREACH (head, &r->legacy.pending) {
        req = QUEUE_DATA(head, struct request, queue);
        if (req->index == index) {
            if (type != -1) {
                assert(req->type == type);
            }
            QUEUE_REMOVE(&req->queue);
            return req;
        }
    }
    return NULL;
}

/* Apply a RAFT_COMMAND entry that has been committed. */
static int applyCommand(struct raft *r,
                        const raft_index index,
                        const struct raft_buffer *buf)
{
    struct raft_apply *req;
    void *result;
    int rv;
    rv = r->fsm->apply(r->fsm, buf, &result);
    if (rv != 0) {
        return rv;
    }

    r->last_applied = index;

    req = (struct raft_apply *)legacyGetRequest(r, index, RAFT_COMMAND);
    if (req != NULL && req->cb != NULL) {
        req->status = 0;
        req->result = result;
        QUEUE_PUSH(&r->legacy.requests, &req->queue);
    }
    return 0;
}

/* Fire the callback of a barrier request whose entry has been committed. */
static void applyBarrier(struct raft *r, const raft_index index)
{
    r->last_applied = index;

    struct raft_barrier *req;
    req = (struct raft_barrier *)legacyGetRequest(r, index, RAFT_BARRIER);
    if (req != NULL && req->cb != NULL) {
        req->status = 0;
        QUEUE_PUSH(&r->legacy.requests, &req->queue);
    }
}

/* Apply a RAFT_CHANGE entry that has been committed. */
static void applyChange(struct raft *r, const raft_index index)
{
    struct raft_change *req;

    assert(index > 0);

    r->last_applied = index;

    if (r->state == RAFT_LEADER) {
        req = r->legacy.change;
        r->legacy.change = NULL;

        if (req != NULL && req->cb != NULL) {
            /* XXX: set the type here, since it's not done in client.c */
            req->type = RAFT_CHANGE;
            req->status = 0;
            QUEUE_PUSH(&r->legacy.requests, &req->queue);
        }
    }
}

static int legacyApply(struct raft *r,
                       struct raft_event **events,
                       unsigned *n_events)
{
    raft_index index;
    struct raft_event *event;
    int rv = 0;

    assert(r->state == RAFT_LEADER || r->state == RAFT_FOLLOWER);
    assert(r->last_applied <= r->commit_index);

    if (r->last_applied == r->commit_index) {
        /* Nothing to do. */
        return 0;
    }

    for (index = r->last_applied + 1; index <= r->commit_index; index++) {
        const struct raft_entry *entry = logGet(r->legacy.log, index);
        if (entry == NULL) {
            /* This can happen while installing a snapshot */
            tracef("replicationApply - ENTRY NULL");
            return 0;
        }

        assert(entry->type == RAFT_COMMAND || entry->type == RAFT_BARRIER ||
               entry->type == RAFT_CHANGE);

        switch (entry->type) {
            case RAFT_COMMAND:
                rv = applyCommand(r, index, &entry->buf);
                break;
            case RAFT_BARRIER:
                applyBarrier(r, index);
                rv = 0;
                break;
            case RAFT_CHANGE:
                applyChange(r, index);

                *n_events += 1;
                *events = raft_realloc(*events, *n_events * sizeof **events);
                assert(*events != NULL);

                event = &(*events)[*n_events - 1];
                event->type = RAFT_CONFIGURATION;
                event->configuration.index = index;

                rv = configurationDecode(&entry->buf,
                                         &event->configuration.conf);

                break;
            default:
                rv = 0; /* For coverity. This case can't be taken. */
                break;
        }

        if (rv != 0) {
            break;
        }
    }

    return rv;
}

void LegacyLeadershipTransferClose(struct raft *r)
{
    struct raft_transfer *req = r->transfer;

    /* Only assert raft_trasferee() if we're not closing, because the result is
     * effectively undefined in that case. */
    if (!r->legacy.closing) {
        assert(raft_transferee(r) == 0);
    }

    r->transfer = NULL;
    if (req->cb != NULL) {
        req->type = RAFT_TRANSFER_;
        QUEUE_PUSH(&r->legacy.requests, &req->queue);
    }
}

static void legacyHandleStateUpdate(struct raft *r)
{
    assert(r->legacy.prev_state != r->state);

    if (r->legacy.prev_state == RAFT_LEADER) {
        LegacyFailPendingRequests(r);
        assert(QUEUE_IS_EMPTY(&r->legacy.pending));
    }

    if (raft_state(r) == RAFT_LEADER) {
        assert(r->legacy.change == NULL);
    }

    if (r->legacy.closing) {
        if (r->transfer != NULL) {
            LegacyLeadershipTransferClose(r);
        }
        LegacyFailPendingRequests(r);
        LegacyFireCompletedRequests(r);
    }

    r->legacy.prev_state = r->state;
}

/* Whether the state_cb callback should be invoked. */
static bool legacyShouldFireStepCb(struct raft *r)
{
    queue *head;
    struct request *req;

    if (r->legacy.step_cb == NULL) {
        return false;
    }

    /* Check if there's a client request in the completion queue which has
     * failed due to a RAFT_NOSPACE error. In that case we will not call the
     * step_cb just yet, because otherwise cowsql/dqlite would notice that
     * the leader has stepped down and immediately close all connections,
     * without a chance of properly returning the error to the client. */
    QUEUE_FOREACH (head, &r->legacy.requests) {
        req = QUEUE_DATA(head, struct request, queue);
        if (req->type == RAFT_COMMAND) {
            if (((struct raft_apply *)req)->status == RAFT_NOSPACE) {
                return false;
            }
        }
    }

    return true;
}

static int legacyHandleUpdateCommitIndex(struct raft *r,
                                         struct raft_event **events,
                                         unsigned *n_events)
{
    raft_index commit_index = raft_commit_index(r);
    int rv;

    /* If the new commit index matches the index of a snapshot we have just
     * persisted, then restore the FSM state using its cached data. */
    if (commit_index != 0 && commit_index == r->legacy.snapshot_index) {
        /* From Figure 5.3:
         *
         *   8. Reset state machine using snapshot contents.
         */
        r->legacy.snapshot_index = 0;
        rv = r->fsm->restore(r->fsm, &r->legacy.snapshot_chunk);
        if (rv != 0) {
            tracef("restore snapshot: %s", errCodeToString(rv));
            return rv;
        }
        r->last_applied = commit_index;
    }

    rv = legacyApply(r, events, n_events);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

/* Handle a single event, possibly adding more events. */
static int legacyHandleEvent(struct raft *r,
                             struct raft_entry *entry,
                             struct raft_event **events,
                             unsigned *n_events,
                             unsigned i)
{
    struct raft_event *event;
    struct raft_update update;
    int rv;

    event = &(*events)[i];
    event->time = r->io->time(r->io);
    event->capacity = r->io->capacity;

    rv = raft_step(r, event, &update);
    if (rv != 0) {
        return rv;
    }

    if (update.flags & RAFT_UPDATE_STATE) {
        legacyHandleStateUpdate(r);
    }

    /* Check whether a raft_change request has been completed. */
    legacyCheckChangeRequest(r, entry, events, n_events);

    if (legacyShouldFireStepCb(r)) {
        r->legacy.step_cb(r);
    }

    if (legacyShouldTakeSnapshot(r)) {
        legacyTakeSnapshot(r);
    }

    /* If the current term was updated, persist it. */
    if (update.flags & RAFT_UPDATE_CURRENT_TERM) {
        rv = r->io->set_term(r->io, raft_current_term(r));
        if (rv != 0) {
            return rv;
        }
    }

    /* If the current vote was updated, persist it. */
    if (update.flags & RAFT_UPDATE_VOTED_FOR) {
        rv = r->io->set_vote(r->io, raft_voted_for(r));
        if (rv != 0) {
            return rv;
        }
    }

    if (update.flags & RAFT_UPDATE_ENTRIES) {
        rv = legacyHandleUpdateEntries(r, update.entries.index,
                                       update.entries.batch, update.entries.n);
        if (rv != 0) {
            return rv;
        }
    }

    if (update.flags & RAFT_UPDATE_SNAPSHOT) {
        rv = legacyHandleUpdateSnapshot(
            r, &update.snapshot.metadata, update.snapshot.offset,
            &update.snapshot.chunk, update.snapshot.last);
        if (rv != 0) {
            return rv;
        }
    }

    if (update.flags & RAFT_UPDATE_MESSAGES) {
        rv = legacyHandleUpdateMessages(r, update.messages.batch,
                                        update.messages.n);
        if (rv != 0) {
            return rv;
        }
    }

    if (update.flags & RAFT_UPDATE_COMMIT_INDEX) {
        rv = legacyHandleUpdateCommitIndex(r, events, n_events);
        if (rv != 0) {
            return rv;
        }
    }

    /* If there's a pending leadership transfer request, and no leadership
     * transfer is in progress, check if it has completed. */
    if (r->transfer != NULL && raft_transferee(r) == 0) {
        /* If we are leader it means that the request was aborted. If we are
         * follower we wait until we find a new leader. */
        if (raft_state(r) == RAFT_LEADER) {
            LegacyLeadershipTransferClose(r);
        } else if (raft_state(r) == RAFT_FOLLOWER) {
            raft_id leader_id;
            const char *leader_address;
            raft_leader(r, &leader_id, &leader_address);
            if (leader_id != 0) {
                LegacyLeadershipTransferClose(r);
            }
        }
    }

    return 0;
}

int LegacyForwardToRaftIo(struct raft *r, struct raft_event *event)
{
    struct raft_event *events;
    unsigned n_events;
    unsigned i;
    struct raft_entry entry; /* Used for actual promotion of RAFT_CHANGE reqs */
    int rv;

    assert(r->io != NULL);

    /* Initially the set of events contains only the event passed as
     * argument, but might grow if some further events get generated by the
     * handling code. */
    events = raft_malloc(sizeof *events);
    if (events == NULL) {
        return RAFT_NOMEM;
    }
    events[0] = *event;
    n_events = 1;

    for (i = 0; i < n_events; i++) {
        if (r->legacy.closing) {
            break;
        }
        rv = legacyHandleEvent(r, &entry, &events, &n_events, i);
        if (rv != 0) {
            break;
        }
    }

    raft_free(events);

    if (rv != 0) {
        return rv;
    }

    return 0;
}

void LegacyLeadershipTransferInit(struct raft *r,
                                  struct raft_transfer *req,
                                  raft_id id,
                                  raft_transfer_cb cb)
{
    assert(r->state == RAFT_LEADER);
    assert(r->leader_state.transferee == 0);
    assert(!r->leader_state.transferring);

    req->cb = cb;
    req->id = id;

    r->transfer = req;
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

    assert(r != NULL);
    assert(bufs != NULL);
    assert(n == 1);

    /* Index of the first entry being appended. */
    index = logLastIndex(r->legacy.log) + 1;
    req->type = RAFT_COMMAND;
    req->index = index;
    req->cb = cb;

    entry.type = RAFT_COMMAND;
    entry.term = r->current_term;
    entry.buf = bufs[0];
    entry.batch = entry.buf.base;

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
    index = logLastIndex(r->legacy.log) + 1;
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

    entry.batch = entry.buf.base;

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

    entry.batch = entry.buf.base;

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

    if (r->state != RAFT_LEADER || r->leader_state.transferee != 0) {
        rv = RAFT_NOTLEADER;
        goto err;
    }

    rv = membershipCanChangeConfiguration(r);
    if (rv != 0) {
        return rv;
    }

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

    if (r->state != RAFT_LEADER || r->leader_state.transferee != 0) {
        rv = RAFT_NOTLEADER;
        goto err;
    }

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

    last_index = logLastIndex(r->legacy.log);

    req->cb = cb;
    req->catch_up_id = 0;

    assert(r->legacy.change == NULL);
    r->legacy.change = req;

    /* If we are not promoting to the voter role or if the log of this
     * server is already up-to-date, we can submit the configuration change
     * immediately. */
    if (role != RAFT_VOTER ||
        progressMatchIndex(r, server_index) == last_index) {
        int old_role = r->configuration.servers[server_index].role;
        r->configuration.servers[server_index].role = role;

        rv = clientChangeConfiguration(r, &r->configuration);
        if (rv != 0) {
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

    if (r->state != RAFT_LEADER || r->leader_state.transferee != 0) {
        rv = RAFT_NOTLEADER;
        goto err;
    }

    rv = membershipCanChangeConfiguration(r);
    if (rv != 0) {
        return rv;
    }

    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        rv = RAFT_BADID;
        goto err;
    }

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

int raft_transfer(struct raft *r,
                  struct raft_transfer *req,
                  raft_id id,
                  raft_transfer_cb cb)
{
    const struct raft_server *server;
    struct raft_event event;
    unsigned i;
    int rv;

    if (r->state != RAFT_LEADER || r->leader_state.transferee != 0) {
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

    LegacyLeadershipTransferInit(r, req, id, cb);

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

int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    rv = r->io->bootstrap(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_recover(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    rv = r->io->recover(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static void tickCb(struct raft_io *io)
{
    struct raft *r;
    struct raft_event event;
    int rv;

    r = io->data;

    event.type = RAFT_TIMEOUT;
    event.time = r->io->time(io);

    rv = LegacyForwardToRaftIo(r, &event);
    assert(rv == 0); /* TODO: just log warning? */
}

static void recvCb(struct raft_io *io, struct raft_message *message)
{
    struct raft *r = io->data;
    struct raft_event event;
    int rv;

    r->now = r->io->time(r->io);
    if (r->legacy.closing) {
        switch (message->type) {
            case RAFT_IO_APPEND_ENTRIES:
                entryBatchesDestroy(message->append_entries.entries,
                                    message->append_entries.n_entries);
                break;
            case RAFT_IO_INSTALL_SNAPSHOT:
                raft_configuration_close(&message->install_snapshot.conf);
                raft_free(message->install_snapshot.data.base);
                break;
        }
        return;
    }

    event.type = RAFT_RECEIVE;
    event.time = r->now;
    event.receive.message = message;

    rv = LegacyForwardToRaftIo(r, &event);

    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            if (message->append_entries.n_entries > 0) {
                if (rv != 0) {
                    raft_free(message->append_entries.entries[0].batch);
                }
                raft_free(message->append_entries.entries);
            }
            break;
    }

    assert(rv == 0); /* TODO: just log warning? */
}

int raft_start(struct raft *r)
{
    struct raft_snapshot *snapshot;
    struct raft_snapshot_metadata metadata;
    raft_term term;
    raft_id voted_for;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;
    struct raft_event event;
    raft_index snapshot_index = 0;
    raft_term snapshot_term = 0;
    unsigned i;
    int rv;

    assert(r != NULL);
    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);
    assert(r->install_snapshot_timeout != 0);
    assert(logNumEntries(r->legacy.log) == 0);
    assert(logSnapshotIndex(r->legacy.log) == 0);
    assert(r->last_stored == 0);

    tracef("starting");
    rv = r->io->load(r->io, &term, &voted_for, &snapshot, &start_index,
                     &entries, &n_entries);
    if (rv != 0) {
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        return rv;
    }
    assert(start_index >= 1);
    tracef("current_term:%llu voted_for:%llu start_index:%llu n_entries:%zu",
           term, voted_for, start_index, n_entries);

    /* If we have a snapshot, let's restore it. */
    if (snapshot != NULL) {
        tracef("restore snapshot with last index %llu and last term %llu",
               snapshot->index, snapshot->term);

        /* Save the snapshot data in the cache, it will be used by legacy
         * compat code to avoid loading the snapshot asynchronously. */
        rv = r->fsm->restore(r->fsm, &snapshot->bufs[0]);
        if (rv != 0) {
            tracef("restore snapshot %llu: %s", snapshot->index,
                   errCodeToString(rv));
            snapshotDestroy(snapshot);
            entryBatchesDestroy(entries, n_entries);
            return rv;
        }
        r->last_applied = snapshot->index;

        snapshot_index = snapshot->index;
        snapshot_term = snapshot->term;

    } else if (n_entries > 1) {
        r->last_applied = 1;
    }

    logStart(r->legacy.log, snapshot_index, snapshot_term, start_index);
    for (i = 0; i < n_entries; i++) {
        struct raft_entry *entry = &entries[i];
        rv = logAppend(r->legacy.log, entry->term, entry->type, &entry->buf,
                       entry->batch);
        if (rv != 0) {
            return rv;
        }
    }

    event.time = r->now;
    event.type = RAFT_START;
    event.start.term = term;
    event.start.voted_for = voted_for;
    event.start.metadata = NULL;
    if (snapshot != NULL) {
        metadata.index = snapshot->index;
        metadata.term = snapshot->term;
        metadata.configuration = snapshot->configuration;
        metadata.configuration_index = snapshot->configuration_index;
        event.start.metadata = &metadata;
    }
    event.start.start_index = start_index;
    event.start.entries = entries;
    event.start.n_entries = (unsigned)n_entries;

    LegacyForwardToRaftIo(r, &event);

    if (entries != NULL) {
        raft_free(entries);
    }

    /* Start the I/O backend. The tickCb function is expected to fire every
     * r->heartbeat_timeout milliseconds and recvCb whenever an RPC is
     * received. */
    rv = r->io->start(r->io, r->heartbeat_timeout, tickCb, recvCb);
    if (rv != 0) {
        tracef("io start failed %d", rv);
        goto out;
    }

out:
    if (snapshot != NULL) {
        raft_free(snapshot->bufs);
        raft_free(snapshot);
    }
    if (rv != 0) {
        return rv;
    }

    return 0;
}

raft_index raft_last_applied(struct raft *r)
{
    return r->last_applied;
}

#undef tracef
