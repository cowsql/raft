#include "legacy.h"
#include "assert.h"
#include "configuration.h"
#include "err.h"

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
    event->done.task.type = RAFT_PERSIST_TERM_AND_VOTE;
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
    struct raft_persist_snapshot *params = &task->persist_snapshot;
    struct ioForwardPersistSnapshot *req;
    int rv;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->r = r;
    req->task = *task;
    req->put.data = req;

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
        assert(snapshot->index == params->index);
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
        struct raft_task *tasks;
        unsigned n_tasks;
        raft_time timeout;
        unsigned j;

        event = &events[i];

        rv = raft_step(r, event, &timeout, &tasks, &n_tasks);
        if (rv != 0) {
            goto err;
        }

        for (j = 0; j < n_tasks; j++) {
            struct raft_task *task = &tasks[j];

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
