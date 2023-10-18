#include "task.h"
#include "assert.h"
#include "heap.h"
#include "queue.h"

/* Append a new task to the r->tasks queue and return a pointer to it.
 *
 * Return RAFT_NOMEM if no-memory is available. */
static struct raft_task *taskAppend(struct raft *r)
{
    struct raft_task *tasks;
    unsigned n_tasks = r->n_tasks + 1;

    if (n_tasks > r->n_tasks_cap) {
        unsigned n_tasks_cap = r->n_tasks_cap;
        if (n_tasks_cap == 0) {
            n_tasks_cap = 16; /* Initial cap */
        } else {
            n_tasks_cap *= 2;
        }
        tasks = raft_realloc(r->tasks, sizeof *r->tasks * n_tasks_cap);
        if (tasks == NULL) {
            return NULL;
        }
        r->tasks = tasks;
        r->n_tasks_cap = n_tasks_cap;
    }

    r->n_tasks = n_tasks;

    return &r->tasks[r->n_tasks - 1];
}

int TaskSendMessage(struct raft *r,
                    raft_id id,
                    const char *address,
                    struct raft_message *message)
{
    struct raft_task *task;
    struct raft_send_message *params;
    int rv;

    task = taskAppend(r);
    if (task == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    task->type = RAFT_SEND_MESSAGE;

    params = &task->send_message;
    params->id = id;
    params->address = address;
    params->message = *message;

    return 0;

err:
    assert(rv == RAFT_NOMEM);
    return rv;
}

int TaskPersistEntries(struct raft *r,
                       raft_index index,
                       struct raft_entry entries[],
                       unsigned n)
{
    struct raft_task *task;
    struct raft_persist_entries *params;
    int rv;

    task = taskAppend(r);
    if (task == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    task->type = RAFT_PERSIST_ENTRIES;

    params = &task->persist_entries;
    params->index = index;
    params->entries = entries;
    params->n = n;

    return 0;

err:
    assert(rv == RAFT_NOMEM);
    return rv;
}

int TaskPersistTermAndVote(struct raft *r, raft_term term, raft_id voted_for)
{
    struct raft_task *task;
    struct raft_persist_term_and_vote *params;
    int rv;

    task = taskAppend(r);
    if (task == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    task->type = RAFT_PERSIST_TERM_AND_VOTE;

    params = &task->persist_term_and_vote;
    params->term = term;
    params->voted_for = voted_for;

    return 0;

err:
    assert(rv == RAFT_NOMEM);
    return rv;
}

int TaskPersistSnapshot(struct raft *r,
                        struct raft_snapshot_metadata metadata,
                        size_t offset,
                        struct raft_buffer chunk,
                        bool last)
{
    struct raft_task *task;
    struct raft_persist_snapshot *params;
    int rv;

    task = taskAppend(r);
    if (task == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    task->type = RAFT_PERSIST_SNAPSHOT;

    params = &task->persist_snapshot;
    params->metadata = metadata;
    params->offset = offset;
    params->chunk = chunk;
    params->last = last;

    return 0;

err:
    assert(rv == RAFT_NOMEM);
    return rv;
}

int TaskLoadSnapshot(struct raft *r, raft_index index, size_t offset)
{
    struct raft_task *task;
    struct raft_load_snapshot *params;
    int rv;

    task = taskAppend(r);
    if (task == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    task->type = RAFT_LOAD_SNAPSHOT;

    params = &task->load_snapshot;
    params->index = index;
    params->offset = offset;

    return 0;

err:
    assert(rv == RAFT_NOMEM);
    return rv;
}

int TaskApplyCommand(struct raft *r,
                     raft_index index,
                     const struct raft_buffer *command)
{
    struct raft_task *task;
    struct raft_apply_command *params;
    int rv;

    task = taskAppend(r);
    if (task == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    task->type = RAFT_APPLY_COMMAND;

    params = &task->apply_command;
    params->index = index;
    params->command = command;

    return 0;

err:
    assert(rv == RAFT_NOMEM);
    return rv;
}
