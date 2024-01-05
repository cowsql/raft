#include "legacy.h"
#include "assert.h"
#include "client.h"
#include "configuration.h"
#include "err.h"
#include "log.h"
#include "membership.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
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
    struct raft_event event;

    if (req->message.type == RAFT_IO_INSTALL_SNAPSHOT) {
        raft_free(req->message.install_snapshot.data.base);
    }

    event.type = RAFT_SENT;
    event.sent.message = req->message;
    event.sent.status = status;

    raft_free(req);

    LegacyForwardToRaftIo(r, &event);
}

static int legacyLoadSnapshot(struct legacySendMessage *req);

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

    if (req->message.type == RAFT_IO_INSTALL_SNAPSHOT) {
        rv = legacyLoadSnapshot(req);
        if (rv != 0) {
            return rv;
        }
        return 0;
    }

    rv = r->io->send(r->io, &req->send, &req->message, legacySendMessageCb);
    if (rv != 0) {
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

    event.type = RAFT_PERSISTED_ENTRIES;
    event.persisted_entries.index = req->index;
    event.persisted_entries.batch = req->entries;
    event.persisted_entries.n = req->n;
    event.persisted_entries.status = status;

    raft_free(req);

    LegacyForwardToRaftIo(r, &event);
}

static int legacyHandleUpdateEntries(struct raft *r,
                                     raft_index index,
                                     struct raft_entry *entries,
                                     unsigned n)
{
    struct legacyPersistEntries *req;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->index = index;
    req->entries = entries;
    req->n = n;
    req->append.data = req;

    rv = r->io->truncate(r->io, index);
    if (rv != 0) {
        goto err;
    }

    rv = r->io->append(r->io, &req->append, entries, n, legacyPersistEntriesCb);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
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
    struct raft_event event;
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

    configurationClose(&snapshot->configuration);
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
    event.type = RAFT_SENT;
    event.sent.message = req->message;
    event.sent.status = status;

    raft_free(req);

    LegacyForwardToRaftIo(r, &event);
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

    /* We might have converted to RAFT_UNAVAILBLE if we are shutting down.
     *
     * Or the snapshot's index might not be in the log anymore because the
     * associated entry failed to be persisted and got truncated (TODO: we
     * should retry instead of truncating). */
    assert(metadata.term != 0);
    if (r->state == RAFT_UNAVAILABLE ||
        logTermOf(r->log, metadata.index) != metadata.term) {
        tracef("cancelling snapshot");
        status = RAFT_CANCELED;
    }

    if (status != 0) {
        tracef("snapshot %lld at term %lld: %s", metadata.index, metadata.term,
               raft_strerror(status));
        configurationClose(&metadata.configuration);
        return;
    }

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

static bool legacyShouldTakeSnapshot(struct raft *r)
{
    /* We currently support only synchronous FSMs, where entries are applied
     * synchronously as soon as we advance the commit index, so the two
     * values always match when we get here. */
    if (r->last_applied < r->commit_index) {
        return false;
    }

    /* If we are shutting down, let's not do anything. */
    if (r->state == RAFT_UNAVAILABLE) {
        return false;
    }

    /* If a snapshot is already in progress or we're installing a snapshot, we
     * don't want to start another one. */
    if (r->snapshot.taking || r->snapshot.persisting) {
        return false;
    };

    /* If we didn't reach the threshold yet, do nothing. */
    if (r->commit_index - r->log->snapshot.last_index < r->snapshot.threshold) {
        return false;
    }

    /* If the last committed index is not anymore in our log, it means that the
     * log got truncated because we have received an InstallSnapshot
     * message. Don't take a snapshot now.*/
    if (logTermOf(r->log, r->commit_index) == 0) {
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

    tracef("take snapshot at %lld", r->commit_index);

    metadata.index = r->commit_index;
    metadata.term = logTermOf(r->log, r->commit_index);

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
        const struct raft_entry *entry = logGet(r->log, index);
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

                rv = 0;
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

static void legacyPersistedEntriesFailure(struct raft *r,
                                          struct raft_event *event)
{
    raft_index index = event->persisted_entries.index;
    unsigned n = event->persisted_entries.n;
    int status = event->persisted_entries.status;

    for (unsigned i = 0; i < n; i++) {
        struct request *req = legacyGetRequest(r, index + i, -1);
        if (!req) {
            tracef("no request found at index %llu", index + i);
            continue;
        }
        switch (req->type) {
            case RAFT_COMMAND: {
                struct raft_apply *apply = (struct raft_apply *)req;
                if (apply->cb) {
                    apply->status = status;
                    apply->result = NULL;
                    QUEUE_PUSH(&r->legacy.requests, &apply->queue);
                }
                break;
            }
            case RAFT_BARRIER: {
                struct raft_barrier *barrier = (struct raft_barrier *)req;
                if (barrier->cb) {
                    barrier->status = status;
                    QUEUE_PUSH(&r->legacy.requests, &barrier->queue);
                }
                break;
            }
            case RAFT_CHANGE: {
                struct raft_change *change = (struct raft_change *)req;
                if (change->cb) {
                    change->status = status;
                    QUEUE_PUSH(&r->legacy.requests, &change->queue);
                }
                break;
            }
            default:
                tracef("unknown request type, shutdown.");
                assert(false);
                break;
        }
    }
}

static void legacyHandleStateUpdate(struct raft *r, struct raft_event *event)
{
    assert(r->legacy.prev_state != r->state);

    if (r->legacy.prev_state == RAFT_LEADER) {
        /* If we're stepping down because of disk write failure, fail
         * requests using the same status code as the write failure.*/
        if (event->type == RAFT_PERSISTED_ENTRIES &&
            event->persisted_entries.status != 0) {
            legacyPersistedEntriesFailure(r, event);
        }

        LegacyFailPendingRequests(r);
        assert(QUEUE_IS_EMPTY(&r->legacy.pending));
    }

    if (raft_state(r) == RAFT_LEADER) {
        assert(r->legacy.change == NULL);
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

    rv = raft_step(r, event, &update);
    if (rv != 0) {
        return rv;
    }

    if (update.flags & RAFT_UPDATE_STATE) {
        legacyHandleStateUpdate(r, event);
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

    /* Initially the set of events contains only the event passed as argument,
     * but might grow if some further events get generated by the handling
     * code. */
    events = raft_malloc(sizeof *events);
    if (events == NULL) {
        return RAFT_NOMEM;
    }
    events[0] = *event;
    n_events = 1;

    for (i = 0; i < n_events; i++) {
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
