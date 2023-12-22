#include "legacy.h"
#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "log.h"
#include "membership.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "tracing.h"

#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* Call LegacyForwardToRaftIo() after an asynchronous task has been
 * completed. */
static void ioTaskDone(struct raft *r, struct raft_task *task, int status)
{
    struct raft_event event;
    event.type = RAFT_DONE;
    event.time = r->io->time(r->io);
    event.done.task = *task;
    event.done.status = status;
    LegacyForwardToRaftIo(r, &event);
}

struct ioForwardSendMessage
{
    struct raft_io_send send;
    struct raft *r;
    struct raft_message message;
};

static void ioSendMessageCb(struct raft_io_send *send, int status)
{
    struct ioForwardSendMessage *req = send->data;
    struct raft *r = req->r;
    struct raft_message message = req->message;
    struct raft_event event;

    raft_free(req);

    event.type = RAFT_SENT;
    event.time = r->io->time(r->io);
    event.sent.message = message;
    event.sent.status = status;
    LegacyForwardToRaftIo(r, &event);
}

static int ioSendMessage(struct raft *r, struct raft_message *message)
{
    struct ioForwardSendMessage *req;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->message = *message;
    req->send.data = req;

    message = &req->message;

    rv = r->io->send(r->io, &req->send, message, ioSendMessageCb);
    if (rv != 0) {
        raft_free(req);
        ErrMsgTransferf(r->io->errmsg, r->errmsg,
                        "send message of type %d to %llu", message->type,
                        message->server_id);
        return rv;
    }

    return 0;
}

struct ioForwardPersistEntries
{
    struct raft_io_append append;
    struct raft *r;
    raft_index index;
    struct raft_entry *entries;
    unsigned n;
};

static void ioForwardPersistEntriesCb(struct raft_io_append *append, int status)
{
    struct ioForwardPersistEntries *req = append->data;
    struct raft *r = req->r;
    struct raft_event event;

    event.time = r->io->time(r->io);
    event.type = RAFT_PERSISTED_ENTRIES;
    event.persisted_entries.index = req->index;
    event.persisted_entries.batch = req->entries;
    event.persisted_entries.n = req->n;
    event.persisted_entries.status = status;

    raft_free(req);

    LegacyForwardToRaftIo(r, &event);
}

static int ioForwardPersistEntries(struct raft *r,
                                   raft_index index,
                                   struct raft_entry *entries,
                                   unsigned n)
{
    struct ioForwardPersistEntries *req;
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

    rv = r->io->append(r->io, &req->append, entries, n,
                       ioForwardPersistEntriesCb);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    raft_free(req);
    ErrMsgTransferf(r->io->errmsg, r->errmsg, "append %u entries", n);
    return rv;
}

struct ioForwardPersistSnapshot
{
    struct raft_io_snapshot_put put;
    struct raft_snapshot snapshot;
    struct raft *r;
    struct raft_snapshot_metadata metadata;
    size_t offset;
    struct raft_buffer chunk;
    bool last;
};

static void ioForwardPersistSnapshotCb(struct raft_io_snapshot_put *put,
                                       int status)
{
    struct ioForwardPersistSnapshot *req = put->data;
    struct raft *r = req->r;
    struct raft_event event;

    event.time = r->io->time(r->io);
    event.type = RAFT_PERSISTED_SNAPSHOT;
    event.persisted_snapshot.metadata = req->metadata;
    event.persisted_snapshot.offset = req->offset;
    event.persisted_snapshot.chunk = req->chunk;
    event.persisted_snapshot.last = req->last;
    event.persisted_snapshot.status = status;

    raft_free(req);

    LegacyForwardToRaftIo(r, &event);
}

static int ioForwardPersistSnapshot(struct raft *r,
                                    struct raft_snapshot_metadata *metadata,
                                    size_t offset,
                                    struct raft_buffer *chunk,
                                    bool last)
{
    struct ioForwardPersistSnapshot *req;
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
                             ioForwardPersistSnapshotCb);
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

struct ioForwardLoadSnapshot
{
    struct raft_io_snapshot_get get;
    struct raft *r;
    struct raft_task task;
};

static void ioForwardLoadSnapshotCb(struct raft_io_snapshot_get *get,
                                    struct raft_snapshot *snapshot,
                                    int status)
{
    struct ioForwardLoadSnapshot *req = get->data;
    struct raft *r = req->r;
    struct raft_task task = req->task;
    struct raft_load_snapshot *params = &task.load_snapshot;

    if (status == 0) {
        /* The old raft_io interface makes no guarantee about the index of the
         * loaded snapshot. */
        if (snapshot->index != params->index) {
            assert(snapshot->index > params->index);
            params->index = snapshot->index;
        }

        assert(snapshot->n_bufs == 1);
        params->chunk = snapshot->bufs[0];
        configurationClose(&snapshot->configuration);
        raft_free(snapshot->bufs);
        raft_free(snapshot);
    }

    raft_free(req);
    ioTaskDone(r, &task, status);
}

static int ioForwardLoadSnapshot(struct raft *r, struct raft_task *task)
{
    struct raft_load_snapshot *params = &task->load_snapshot;
    struct ioForwardLoadSnapshot *req;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->task = *task;
    req->get.data = req;

    rv = r->io->snapshot_get(r->io, &req->get, ioForwardLoadSnapshotCb);
    if (rv != 0) {
        raft_free(req);
        ErrMsgTransferf(r->io->errmsg, r->errmsg, "load snapshot at %llu",
                        params->index);
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
    event.time = r->io->time(r->io);
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

int LegacyForwardToRaftIo(struct raft *r, struct raft_event *event)
{
    struct raft_update update;
    raft_index commit_index;
    raft_time timeout;
    struct raft_task *tasks;
    unsigned n_tasks;
    unsigned j;
    queue *head;
    struct request *req;
    bool has_pending_no_space_failure = false;

    int rv;

    if (r->io == NULL) {
        /* No legacy raft_io implementation, just do nothing. */
        return 0;
    }

    rv =
        raft_step(r, event, &update, &commit_index, &timeout, &tasks, &n_tasks);
    if (rv != 0) {
        goto err;
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
                has_pending_no_space_failure = true;
                break;
            }
        }
    }

    if (!has_pending_no_space_failure && r->legacy.step_cb != NULL) {
        r->legacy.step_cb(r);
    }

    if (legacyShouldTakeSnapshot(r)) {
        legacyTakeSnapshot(r);
    }

    /* If the current term was updated, persist it. */
    if (update.flags & RAFT_UPDATE_CURRENT_TERM) {
        rv = r->io->set_term(r->io, raft_current_term(r));
        if (rv != 0) {
            goto err;
        }
    }

    /* If the current vote was updated, persist it. */
    if (update.flags & RAFT_UPDATE_VOTED_FOR) {
        rv = r->io->set_vote(r->io, raft_voted_for(r));
        if (rv != 0) {
            goto err;
        }
    }

    if (update.flags & RAFT_UPDATE_ENTRIES) {
        rv = ioForwardPersistEntries(r, update.entries.index,
                                     update.entries.batch, update.entries.n);
        if (rv != 0) {
            goto err;
        }
    }

    if (update.flags & RAFT_UPDATE_SNAPSHOT) {
        rv = ioForwardPersistSnapshot(
            r, &update.snapshot.metadata, update.snapshot.offset,
            &update.snapshot.chunk, update.snapshot.last);
        if (rv != 0) {
            goto err;
        }
    }

    if (update.flags & RAFT_UPDATE_MESSAGES) {
        for (j = 0; j < update.messages.n; j++) {
            rv = ioSendMessage(r, &update.messages.batch[j]);
            if (rv != 0) {
                goto err;
            }
        }
    }

    for (j = 0; j < n_tasks; j++) {
        struct raft_task *task = &tasks[j];

        /* Don't execute any further task if we're shutting down. */
        if (r->close_cb != NULL) {
            ioTaskDone(r, task, RAFT_CANCELED);
            continue;
        }

        switch (task->type) {
            case RAFT_LOAD_SNAPSHOT:
                rv = ioForwardLoadSnapshot(r, task);
                break;
            default:
                rv = RAFT_INVALID;
                assert(0);
                break;
        };

        if (rv != 0) {
            goto err;
        }
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}
