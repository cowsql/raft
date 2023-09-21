#include "legacy.h"
#include "assert.h"

static struct raft_event *eventAppend(struct raft_event *events[],
                                      unsigned *n_events)
{
    struct raft_event *array;

    array = raft_realloc(*events, sizeof **events * (*n_events + 1));
    if (array == NULL) {
        return NULL;
    }

    *n_events += 1;

    return &(*events)[*n_events - 1];
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
    events = event;
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
                case RAFT_PERSIST_TERM_AND_VOTE:
                    rv = ioPersistTermAndVote(r, task, &events, &n_events);
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

    return 0;
err:
    if (n_events > 1) {
        raft_free(events);
    }

    return rv;
}
