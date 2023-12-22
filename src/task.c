#include "task.h"
#include "assert.h"
#include "heap.h"
#include "queue.h"

/* Append a new message to the r->messages queue and return a pointer to it.
 *
 * Return NULL if no-memory is available. */
static struct raft_message *messageAppend(struct raft *r)
{
    struct raft_message *messages;
    unsigned n_messages = r->n_messages + 1;

    if (n_messages > r->n_messages_cap) {
        unsigned n_messages_cap = r->n_messages_cap;
        if (n_messages_cap == 0) {
            n_messages_cap = 16; /* Initial cap */
        } else {
            n_messages_cap *= 2;
        }
        messages =
            raft_realloc(r->messages, sizeof *r->messages * n_messages_cap);
        if (messages == NULL) {
            return NULL;
        }
        r->messages = messages;
        r->n_messages_cap = n_messages_cap;
    }

    r->n_messages = n_messages;

    return &r->messages[r->n_messages - 1];
}

int TaskSendMessage(struct raft *r, struct raft_message *message)
{
    struct raft_message *next;
    int rv;

    r->updates |= RAFT_UPDATE_MESSAGES;

    next = messageAppend(r);
    if (next == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    *next = *message;

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
    assert(!(r->updates & RAFT_UPDATE_ENTRIES));

    r->updates |= RAFT_UPDATE_ENTRIES;

    r->entries_index = index;
    r->entries = entries;
    r->n_entries = n;

    return 0;
}

int TaskPersistSnapshot(struct raft *r,
                        struct raft_snapshot_metadata metadata,
                        size_t offset,
                        struct raft_buffer chunk,
                        bool last)
{
    assert(!(r->updates & RAFT_UPDATE_SNAPSHOT));

    r->updates |= RAFT_UPDATE_SNAPSHOT;

    r->snapshot_metadata = metadata;
    r->snapshot_offset = offset;
    r->snapshot_chunk = chunk;
    r->snapshot_last = last;

    return 0;
}
