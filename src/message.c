#include "message.h"
#include "assert.h"
#include "heap.h"
#include "queue.h"

/* Append a new message to the r->messages queue and return a pointer to it.
 *
 * Return NULL if no-memory is available. */
static struct raft_message *messageAppend(struct raft *r)
{
    struct raft_message *messages;
    unsigned n_messages = r->update->messages.n + 1;

    assert(r->update->messages.batch == r->messages);

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
        r->update->messages.batch = r->messages;
    }

    r->update->messages.n = n_messages;

    return &r->update->messages.batch[r->update->messages.n - 1];
}

int MessageEnqueue(struct raft *r, struct raft_message *message)
{
    struct raft_message *next;
    int rv;

    r->update->flags |= RAFT_UPDATE_MESSAGES;

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
