#include "message.h"
#include "assert.h"
#include "heap.h"
#include "queue.h"

/* Ensure that the r->messages array as at least n_messages slots, and expand it
 * if needed.
 *
 * Return NULL if no-memory is available. */
static int messageEnsureQueueCapacity(struct raft *r, const unsigned n_messages)
{
    unsigned n_messages_cap = r->n_messages_cap;
    struct raft_message *messages;

    if (n_messages <= n_messages_cap) {
        return 0;
    }

    if (n_messages_cap == 0) {
        n_messages_cap = 16; /* Initial cap */
    } else {
        n_messages_cap *= 2;
    }

    messages = raft_realloc(r->messages, sizeof *r->messages * n_messages_cap);
    if (messages == NULL) {
        return RAFT_NOMEM;
    }

    r->messages = messages;
    r->n_messages_cap = n_messages_cap;
    r->update->messages.batch = r->messages;

    return 0;
}

int MessageEnqueue(struct raft *r, struct raft_message *message)
{
    unsigned n_messages = r->update->messages.n + 1;
    int rv;

    assert(r->update->messages.batch == r->messages);

    rv = messageEnsureQueueCapacity(r, n_messages);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        return rv;
    }

    r->update->messages.n = n_messages;
    r->update->messages.batch[r->update->messages.n - 1] = *message;
    r->update->flags |= RAFT_UPDATE_MESSAGES;

    return 0;
}
