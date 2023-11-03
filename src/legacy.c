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

static struct raft_event *eventAppend(struct raft_event *events[],
                                      unsigned *n_events)
{
    struct raft_event *array;

    array = raft_realloc(*events, sizeof **events * (*n_events + 1));
    if (array == NULL) {
        return NULL;
    }

    *events = array;
    *n_events += 1;

    return &(*events)[*n_events - 1];
}

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
    struct raft_task task;
};

static void ioSendMessageCb(struct raft_io_send *send, int status)
{
    struct ioForwardSendMessage *req = send->data;
    struct raft *r = req->r;
    struct raft_task task = req->task;
    raft_free(req);
    ioTaskDone(r, &task, status);
}

static int ioSendMessage(struct raft *r, struct raft_task *task)
{
    struct raft_send_message *params = &task->send_message;
    struct ioForwardSendMessage *req;
    struct raft_message message;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->task = *task;
    req->send.data = req;

    message = params->message;
    message.server_id = params->id;
    message.server_address = params->address;

    rv = r->io->send(r->io, &req->send, &message, ioSendMessageCb);
    if (rv != 0) {
        raft_free(req);
        ErrMsgTransferf(r->io->errmsg, r->errmsg,
                        "send message of type %d to %llu", params->message.type,
                        params->id);
        return rv;
    }

    return 0;
}

struct ioForwardPersistEntries
{
    struct raft_io_append append;
    struct raft *r;
    struct raft_task task;
};

static void ioForwardPersistEntriesCb(struct raft_io_append *append, int status)
{
    struct ioForwardPersistEntries *req = append->data;
    struct raft *r = req->r;
    struct raft_task task = req->task;
    raft_free(req);
    ioTaskDone(r, &task, status);
}

static int ioForwardPersistEntries(struct raft *r, struct raft_task *task)
{
    struct raft_persist_entries *params = &task->persist_entries;
    struct ioForwardPersistEntries *req;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->task = *task;
    req->append.data = req;

    rv = r->io->truncate(r->io, params->index);
    if (rv != 0) {
        goto err;
    }

    rv = r->io->append(r->io, &req->append, params->entries, params->n,
                       ioForwardPersistEntriesCb);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    raft_free(req);
    ErrMsgTransferf(r->io->errmsg, r->errmsg, "append %u entries", params->n);
    return rv;
}

static int ioPersistTermAndVote(struct raft *r,
                                struct raft_task *task,
                                struct raft_event *events[],
                                unsigned *n_events)
{
    struct raft_persist_term_and_vote *params = &task->persist_term_and_vote;
    struct raft_event *event;
    int rv;

    rv = r->io->set_term(r->io, params->term);
    if (rv != 0) {
        goto err;
    }

    rv = r->io->set_vote(r->io, params->voted_for);
    if (rv != 0) {
        goto err;
    }

    /* Add a completion event immediately, since set_term() and set_vote() are
     * required to be synchronous */
    event = eventAppend(events, n_events);
    if (event == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    event->type = RAFT_DONE;
    event->time = r->io->time(r->io);
    event->done.task = *task;
    event->done.status = 0;

    return 0;

err:
    return rv;
}

struct ioForwardPersistSnapshot
{
    struct raft_io_snapshot_put put;
    struct raft_snapshot snapshot;
    struct raft *r;
    struct raft_task task;
};

static void ioForwardPersistSnapshotCb(struct raft_io_snapshot_put *put,
                                       int status)
{
    struct ioForwardPersistSnapshot *req = put->data;
    struct raft *r = req->r;
    struct raft_task task = req->task;
    raft_free(req);
    ioTaskDone(r, &task, status);
}

static int ioForwardPersistSnapshot(struct raft *r, struct raft_task *task)
{
    struct ioForwardPersistSnapshot *req;
    struct raft_persist_snapshot *params;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->task = *task;
    req->put.data = req;

    params = &req->task.persist_snapshot;

    req->snapshot.index = params->metadata.index;
    req->snapshot.term = params->metadata.term;
    req->snapshot.configuration = params->metadata.configuration;
    req->snapshot.configuration_index = params->metadata.configuration_index;
    req->snapshot.bufs = &params->chunk;
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
                    params->metadata.index);
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
        tracef("snapshot %lld at term %lld: %s", snapshot->index,
               snapshot->term, raft_strerror(status));
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
        };
    }
}

int LegacyForwardToRaftIo(struct raft *r, struct raft_event *event)
{
    struct raft_event *events;
    unsigned n_events;
    unsigned i;
    int rv;

    if (r->io == NULL) {
        /* No legacy raft_io implementation, just do nothing. */
        return 0;
    }

    /* Initially the set of events contains only the event passed as argument,
     * but might grow if some of the tasks get completed synchronously. */
    events = raft_malloc(sizeof *events);
    if (events == NULL) {
        return RAFT_NOMEM;
    }
    events[0] = *event;
    n_events = 1;

    for (i = 0; i < n_events; i++) {
        raft_index commit_index;
        raft_time timeout;
        struct raft_task *tasks;
        unsigned n_tasks;
        unsigned j;

        event = &events[i];

        rv = raft_step(r, event, &commit_index, &timeout, &tasks, &n_tasks);
        if (rv != 0) {
            goto err;
        }

        LegacyFireCompletedRequests(r);

        if (r->legacy.step_cb != NULL) {
            r->legacy.step_cb(r);
        }

        if (legacyShouldTakeSnapshot(r)) {
            legacyTakeSnapshot(r);
        }

        for (j = 0; j < n_tasks; j++) {
            struct raft_task *task = &tasks[j];

            /* Don't execute any further task if we're shutting down. */
            if (r->close_cb != NULL) {
                ioTaskDone(r, task, RAFT_CANCELED);
                continue;
            }

            switch (task->type) {
                case RAFT_SEND_MESSAGE:
                    rv = ioSendMessage(r, task);
                    break;
                case RAFT_PERSIST_ENTRIES:
                    rv = ioForwardPersistEntries(r, task);
                    break;
                case RAFT_PERSIST_TERM_AND_VOTE:
                    rv = ioPersistTermAndVote(r, task, &events, &n_events);
                    break;
                case RAFT_PERSIST_SNAPSHOT:
                    rv = ioForwardPersistSnapshot(r, task);
                    break;
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
    }

    raft_free(events);
    return 0;
err:
    raft_free(events);

    return rv;
}
