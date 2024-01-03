#define TEST_CLUSTER_V1

#include "../../src/queue.h"

#include "cluster.h"

/* Defaults */
#define DEFAULT_ELECTION_TIMEOUT 100
#define DEFAULT_HEARTBEAT_TIMEOUT 50
#define DEFAULT_NETWORK_LATENCY 10
#define DEFAULT_DISK_LATENCY 10

enum operation_type {
    OPERATION_ENTRIES = 0,
    OPERATION_SNAPSHOT,
    OPERATION_SEND,
    OPERATION_TRANSMIT
};

/* Track pending async operations. */
struct operation
{
    enum operation_type type;
    raft_id id;           /* Target server ID. */
    raft_time completion; /* When the operation should complete */
    union {
        struct
        {
            struct raft_message message;
        } send;
        struct
        {
            raft_index index;
            struct raft_entry *batch;
            unsigned n;
        } entries;
        struct
        {
            struct raft_snapshot_metadata metadata;
            size_t offset;
            struct raft_buffer chunk;
            bool last;
        } snapshot;
        struct
        {
            raft_id from;
            struct raft_message message;
        } transmit;
    };
    queue queue;
};

/* Mark @id1 as disconnected from @id2. */
struct disconnect
{
    raft_id id1;
    raft_id id2;
    queue queue;
};

/* Initialize an empty disk with no persisted data. */
static void diskInit(struct test_disk *d)
{
    d->term = 0;
    d->voted_for = 0;
    d->snapshot = NULL;
    d->start_index = 1;
    d->entries = NULL;
    d->n_entries = 0;
}

/* Release all memory used by the disk snapshot, if present. */
static void diskDestroySnapshotIfPresent(struct test_disk *d)
{
    if (d->snapshot == NULL) {
        return;
    }
    raft_configuration_close(&d->snapshot->metadata.configuration);
    free(d->snapshot->data.base);
    free(d->snapshot);
    d->snapshot = NULL;
}

/* Release all memory used by the disk. */
static void diskClose(struct test_disk *d)
{
    unsigned i;

    for (i = 0; i < d->n_entries; i++) {
        free(d->entries[i].buf.base);
    }
    free(d->entries);
    diskDestroySnapshotIfPresent(d);
}

/* Set the persisted term. */
static void diskSetTerm(struct test_disk *d, raft_term term)
{
    d->term = term;
}

/* Set the persisted vote. */
static void diskSetVote(struct test_disk *d, raft_id vote)
{
    d->voted_for = vote;
}

/* Set the persisted snapshot. */
static void diskSetSnapshot(struct test_disk *d, struct test_snapshot *snapshot)
{
    diskDestroySnapshotIfPresent(d);
    d->snapshot = snapshot;

    /* If there are no entries, set the start index to the snapshot's last
     * index. */
    if (d->n_entries == 0) {
        d->start_index = snapshot->metadata.index + 1;
    }
}

static void entryCopy(const struct raft_entry *src, struct raft_entry *dst)
{
    dst->term = src->term;
    dst->type = src->type;
    dst->buf.len = src->buf.len;
    dst->buf.base = munit_malloc(dst->buf.len);
    memcpy(dst->buf.base, src->buf.base, dst->buf.len);
    dst->batch = NULL;
}

/* Append a new entry to the log. */
static void diskAddEntry(struct test_disk *d, const struct raft_entry *entry)
{
    d->n_entries++;
    d->entries = realloc(d->entries, d->n_entries * sizeof *d->entries);
    munit_assert_ptr_not_null(d->entries);
    entryCopy(entry, &d->entries[d->n_entries - 1]);
}

/* Deep copy configuration object @src to @dst. */
static void confCopy(const struct raft_configuration *src,
                     struct raft_configuration *dst)
{
    unsigned i;
    int rv;

    raft_configuration_init(dst);
    for (i = 0; i < src->n; i++) {
        struct raft_server *server = &src->servers[i];
        rv = raft_configuration_add(dst, server->id, server->address,
                                    server->role);
        munit_assert_int(rv, ==, 0);
    }
}

/* Copy snapshot metadata @src to @dst. */
static void snapshotCopy(const struct raft_snapshot_metadata *src,
                         struct raft_snapshot_metadata *dst)
{
    dst->index = src->index;
    dst->term = src->term;

    confCopy(&src->configuration, &dst->configuration);
    dst->configuration_index = src->configuration_index;
}

/* Load the metadata of latest snapshot. */
static void diskLoadSnapshotMetadata(struct test_disk *d,
                                     struct raft_snapshot_metadata *metadata)
{
    munit_assert_ptr_not_null(d->snapshot);
    snapshotCopy(&d->snapshot->metadata, metadata);
}

/* Load all data persisted on the disk. */
static void diskLoad(struct test_disk *d,
                     raft_term *term,
                     raft_id *voted_for,
                     struct raft_snapshot_metadata **metadata,
                     raft_index *start_index,
                     struct raft_entry **entries,
                     unsigned *n_entries)
{
    size_t size = 0;
    void *batch;
    uint8_t *cursor;
    unsigned i;

    *term = d->term;
    *voted_for = d->voted_for;
    if (d->snapshot != NULL) {
        *metadata = munit_malloc(sizeof **metadata);
        diskLoadSnapshotMetadata(d, *metadata);
    } else {
        *metadata = NULL;
    }
    *start_index = d->start_index;
    *n_entries = d->n_entries;

    if (*n_entries == 0) {
        *entries = NULL;
        return;
    }

    /* Calculate the total size of the entries content and allocate the
     * batch. */
    for (i = 0; i < d->n_entries; i++) {
        size += d->entries[i].buf.len;
    }

    batch = raft_malloc(size);
    munit_assert_ptr_not_null(batch);

    /* Copy the entries. */
    *entries = raft_malloc(d->n_entries * sizeof **entries);
    munit_assert_ptr_not_null(*entries);

    cursor = batch;

    for (i = 0; i < d->n_entries; i++) {
        (*entries)[i].term = d->entries[i].term;
        (*entries)[i].type = d->entries[i].type;
        (*entries)[i].buf.base = cursor;
        (*entries)[i].buf.len = d->entries[i].buf.len;
        (*entries)[i].batch = batch;
        memcpy((*entries)[i].buf.base, d->entries[i].buf.base,
               d->entries[i].buf.len);
        cursor += d->entries[i].buf.len;
    }
}

/* Truncate all entries from the given index onwards. If there are no entries at
 * the given index, this is a no-op. */
static void diskTruncateEntries(struct test_disk *d, raft_index index)
{
    unsigned i;

    if (index == d->start_index + d->n_entries) {
        return;
    }

    munit_assert_ulong(index, >=, d->start_index);
    munit_assert_ulong(index, <=, d->start_index + d->n_entries);

    for (i = (unsigned)(index - d->start_index); i < d->n_entries; i++) {
        free(d->entries[i].buf.base);
        d->n_entries--;
    }
}

/* Append a new entry to the log. */
static void diskPersistEntry(struct test_disk *d, struct raft_entry *entry)
{
    d->n_entries++;
    d->entries = realloc(d->entries, d->n_entries * sizeof *d->entries);
    munit_assert_ptr_not_null(d->entries);
    entryCopy(entry, &d->entries[d->n_entries - 1]);
}

/* Custom emit tracer function which includes the server ID. */
static void serverTrace(struct raft_tracer *t, int type, const void *data)
{
    struct test_server *server;
    struct test_cluster *cluster;
    const struct raft_tracer_info *info = data;
    char trace[1024];

    server = t->impl;
    cluster = server->cluster;

    if (cluster->in_tear_down) {
        return;
    }

    if (type != RAFT_TRACER_DIAGNOSTIC) {
        return;
    }

    if (info->diagnostic.level > 3) {
        fprintf(stderr, "TRACE: %llu > %s\n", server->raft.id,
                info->diagnostic.message);
        return;
    }

    if (info->diagnostic.message[0] == '>') {
        snprintf(trace, sizeof trace, "[%4lld] %llu %s", cluster->time,
                 server->raft.id, info->diagnostic.message);
    } else {
        snprintf(trace, sizeof trace, "         %s", info->diagnostic.message);
    }
    strcat(cluster->trace, trace);
    strcat(cluster->trace, "\n");
    fprintf(stderr, "%s\n", trace);
}

static void serverSeed(struct test_server *s);

/* Set the election timeout and the randomized election timeout (timeout +
 * delta). */
static void serverSetElectionTimeout(struct test_server *s,
                                     unsigned timeout,
                                     unsigned delta)
{
    munit_assert_uint(delta, <=, timeout);

    s->randomized_election_timeout = timeout;
    s->randomized_election_timeout += delta;

    s->raft.election_timeout = timeout;
    serverSeed(s);

    raft_set_election_timeout(&s->raft, timeout);

    /* The current timeout might have changed now. */
    s->timeout = raft_timeout(&s->raft);
}

/* Initialize a new server object. */
static void serverInit(struct test_server *s,
                       raft_id id,
                       struct test_cluster *cluster)
{
    char address[64];
    unsigned delta;
    int rv;

    s->tracer.impl = s;
    s->tracer.version = 2;
    s->tracer.trace = serverTrace;
    s->randomized_election_timeout_prev = 0;

    sprintf(address, "%llu", id);

    rv = raft_init(&s->raft, NULL, NULL, id, address);
    munit_assert_int(rv, ==, 0);

    s->raft.tracer = &s->tracer;

    /* By default servers have their randomized timeout increasing
     * progressively, so they timeout in order. */
    switch (id) {
        case 1:
            delta = 0;
            break;
        case 2:
            delta = 30;
            break;
        case 3:
            delta = 60;
            break;
        case 4:
            delta = 80;
            break;
        case 5:
            delta = 90;
            break;
        default:
            delta = 90 + (unsigned)id;
            break;
    }

    serverSetElectionTimeout(s, DEFAULT_ELECTION_TIMEOUT, delta);
    raft_set_heartbeat_timeout(&s->raft, DEFAULT_HEARTBEAT_TIMEOUT);

    s->network_latency = DEFAULT_NETWORK_LATENCY;
    s->cluster = cluster;
    s->disk_latency = DEFAULT_DISK_LATENCY;
    s->running = false;
}

static void serverStep(struct test_server *s, struct raft_event *event);

static void serverCancelSend(struct test_server *s, struct operation *operation)
{
    struct raft_event event;

    event.time = s->cluster->time;
    event.type = RAFT_SENT;
    event.sent.message = operation->send.message;
    event.sent.status = RAFT_CANCELED;

    serverStep(s, &event);
}

static void serverCancelEntries(struct test_server *s,
                                struct operation *operation)
{
    struct raft_event event;

    event.time = s->cluster->time;
    event.type = RAFT_PERSISTED_ENTRIES;
    event.persisted_entries.index = operation->entries.index;
    event.persisted_entries.n = operation->entries.n;
    event.persisted_entries.batch = operation->entries.batch;
    event.persisted_entries.status = RAFT_CANCELED;

    serverStep(s, &event);
}

static void serverCancelSnapshot(struct test_server *s,
                                 struct operation *operation)
{
    struct raft_event event;

    event.time = s->cluster->time;
    event.type = RAFT_PERSISTED_SNAPSHOT;
    event.persisted_snapshot.metadata = operation->snapshot.metadata;
    event.persisted_snapshot.offset = operation->snapshot.offset;
    event.persisted_snapshot.chunk = operation->snapshot.chunk;
    event.persisted_snapshot.last = operation->snapshot.last;
    event.persisted_snapshot.status = RAFT_CANCELED;

    /* XXX: this should probably be done by raft core */
    raft_free(operation->snapshot.chunk.base);

    serverStep(s, &event);
}

/* Release the memory used by a transmit operation. */
static void clearTransmitOperation(struct operation *o)
{
    switch (o->transmit.message.type) {
        case RAFT_IO_APPEND_ENTRIES:
            if (o->transmit.message.append_entries.n_entries > 0) {
                struct raft_entry *entries =
                    o->transmit.message.append_entries.entries;
                raft_free(entries[0].buf.base);
                raft_free(entries);
            }
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            raft_configuration_close(
                &o->transmit.message.install_snapshot.conf);
            raft_free(o->transmit.message.install_snapshot.data.base);
            break;
    }
}

/* Cancel all pending operations generated by the given server */
static void serverCancelPendingOperations(struct test_server *s)
{
    while (1) {
        struct operation *operation = NULL;
        queue *head;
        QUEUE_FOREACH (head, &s->cluster->operations) {
            struct operation *current;
            current = QUEUE_DATA(head, struct operation, queue);
            if (current->id == s->raft.id) {
                operation = current;
                break;
            }
        }
        if (operation == NULL) {
            break;
        }

        switch (operation->type) {
            case OPERATION_SEND:
                serverCancelSend(s, operation);
                break;
            case OPERATION_ENTRIES:
                serverCancelEntries(s, operation);
                break;
            case OPERATION_SNAPSHOT:
                serverCancelSnapshot(s, operation);
                break;
            case OPERATION_TRANSMIT:
                clearTransmitOperation(operation);
                break;
            default:
                break;
        }
        QUEUE_REMOVE(&operation->queue);
        free(operation);
    }
}

static void serverStop(struct test_server *s)
{
    struct raft_event event;
    struct raft_update update;
    int rv;

    event.time = s->cluster->time;
    event.type = RAFT_STOP;

    rv = raft_step(&s->raft, &event, &update);
    munit_assert_int(rv, ==, 0);

    s->running = false;

    /* Re-initialized the raft object. */
    raft_close(&s->raft, NULL);
    serverInit(s, s->raft.id, s->cluster);
}

/* Release all resources used by a server object. */
static void serverClose(struct test_server *s)
{
    if (s->running) {
        serverStop(s);
    }
    raft_close(&s->raft, NULL);
    diskClose(&s->disk);
}

/* Seed raft's internal pseudo random number generator so that the
 * next time RandomWithinRange() is run it will return
 * exactly the value stored in s->randomized_election_timeout. */
static void serverSeed(struct test_server *s)
{
    unsigned timeout = s->raft.election_timeout;

    if (s->randomized_election_timeout == s->randomized_election_timeout_prev) {
        goto done;
    }

    s->seed = s->raft.random;
    while (1) {
        unsigned random = s->seed;
        unsigned n = raft_random(&random, timeout, timeout * 2);
        if (n == s->randomized_election_timeout) {
            goto done;
        }
        s->seed = random;
    }

done:
    raft_seed(&s->raft, s->seed);
    s->randomized_election_timeout_prev = s->randomized_election_timeout;
}

static void serverProcessEntries(struct test_server *s,
                                 raft_index first_index,
                                 struct raft_entry *entries,
                                 unsigned n)
{
    struct operation *operation = munit_malloc(sizeof *operation);

    operation->type = OPERATION_ENTRIES;
    operation->id = s->raft.id;

    operation->completion = s->cluster->time + s->disk_latency;

    operation->entries.index = first_index;
    operation->entries.batch = entries;
    operation->entries.n = n;

    QUEUE_PUSH(&s->cluster->operations, &operation->queue);
}

static void serverProcessSnapshot(struct test_server *s,
                                  struct raft_snapshot_metadata *metadata,
                                  size_t offset,
                                  struct raft_buffer *chunk,
                                  bool last)
{
    struct operation *operation = munit_malloc(sizeof *operation);

    operation->type = OPERATION_SNAPSHOT;
    operation->id = s->raft.id;

    operation->completion = s->cluster->time + s->disk_latency;

    operation->snapshot.metadata = *metadata;
    operation->snapshot.offset = offset;
    operation->snapshot.chunk = *chunk;
    operation->snapshot.last = last;

    QUEUE_PUSH(&s->cluster->operations, &operation->queue);
}

static void serverProcessMessages(struct test_server *s,
                                  struct raft_message *messages,
                                  unsigned n)
{
    unsigned i;

    for (i = 0; i < n; i++) {
        struct operation *operation = munit_malloc(sizeof *operation);

        operation->type = OPERATION_SEND;
        operation->id = s->raft.id;

        /* TODO: simulate the presence of an OS send buffer, whose available
         * size might delay the completion of send requests */
        operation->completion = s->cluster->time;

        operation->send.message = messages[i];

        QUEUE_PUSH(&s->cluster->operations, &operation->queue);
    }
}

static void serverProcessCommitIndex(struct test_server *s)
{
    struct raft *r = &s->raft;
    struct raft_event event;
    struct raft_update update;
    int rv;

    event.time = s->cluster->time;
    event.type = RAFT_CONFIGURATION;

    /* XXX: we should check if the given index is actually associated with a
     * configuration */
    event.configuration.index = raft_commit_index(r);

    rv = raft_step(r, &event, &update);
    munit_assert_int(rv, ==, 0);
}

/* Fire the given event using raft_step() and process the resulting struct
 * raft_update object. */
static void serverStep(struct test_server *s, struct raft_event *event)
{
    struct raft *r = &s->raft;
    struct raft_update update;
    int rv;

    munit_assert_true(s->running);

    rv = raft_step(r, event, &update);
    munit_assert_int(rv, ==, 0);

    if (update.flags & RAFT_UPDATE_CURRENT_TERM) {
        diskSetTerm(&s->disk, raft_current_term(r));
    }

    if (update.flags & RAFT_UPDATE_VOTED_FOR) {
        diskSetVote(&s->disk, raft_voted_for(r));
    }

    if (update.flags & RAFT_UPDATE_ENTRIES) {
        serverProcessEntries(s, update.entries.index, update.entries.batch,
                             update.entries.n);
    }

    if (update.flags & RAFT_UPDATE_SNAPSHOT) {
        serverProcessSnapshot(s, &update.snapshot.metadata,
                              update.snapshot.offset, &update.snapshot.chunk,
                              update.snapshot.last);
    }

    if (update.flags & RAFT_UPDATE_MESSAGES) {
        serverProcessMessages(s, update.messages.batch, update.messages.n);
    }

    if (update.flags & RAFT_UPDATE_TIMEOUT) {
        s->timeout = raft_timeout(&s->raft);
    }

    if (update.flags & RAFT_UPDATE_COMMIT_INDEX) {
        serverProcessCommitIndex(s);
    }
}

/* Start the server by passing to raft_step() a RAFT_START event with the
 * current disk state. */
static void serverStart(struct test_server *s)
{
    struct raft_event event;

    s->running = true;

    serverSeed(s);

    event.time = s->cluster->time;
    event.type = RAFT_START;

    diskLoad(&s->disk, &event.start.term, &event.start.voted_for,
             &event.start.metadata, &event.start.start_index,
             &event.start.entries, &event.start.n_entries);

    serverStep(s, &event);

    if (event.start.metadata != NULL) {
        free(event.start.metadata);
    }
}

/* Fire a RAFT_TIMEOUT event. */
static void serverTimeout(struct test_server *s)
{
    struct raft_event event;

    s->cluster->time = s->timeout;

    event.time = s->cluster->time;
    event.type = RAFT_TIMEOUT;

    serverStep(s, &event);
}

/* Create a single batch of entries containing a copy of the given entries,
 * including their data. Use raft_malloc() since memory ownership is going to be
 * handed over to raft via raft_recv(). */
static void copyEntries(const struct raft_entry *src,
                        struct raft_entry **dst,
                        const size_t n)
{
    size_t size = 0;
    void *batch;
    uint8_t *cursor;
    unsigned i;

    if (n == 0) {
        *dst = NULL;
        return;
    }

    /* Calculate the total size of the entries content and allocate the
     * batch. */
    for (i = 0; i < n; i++) {
        size += src[i].buf.len;
    }

    batch = raft_malloc(size);
    munit_assert_ptr_not_null(batch);

    /* Copy the entries. */
    *dst = raft_malloc(n * sizeof **dst);
    munit_assert_ptr_not_null(*dst);

    cursor = batch;

    for (i = 0; i < n; i++) {
        (*dst)[i].term = src[i].term;
        (*dst)[i].type = src[i].type;
        (*dst)[i].buf.base = cursor;
        (*dst)[i].buf.len = src[i].buf.len;
        (*dst)[i].batch = batch;
        memcpy((*dst)[i].buf.base, src[i].buf.base, src[i].buf.len);
        cursor += src[i].buf.len;
    }
}

static void serverEnqueueTransmit(struct test_server *s,
                                  struct raft_message *message)
{
    struct operation *operation = munit_malloc(sizeof *operation);

    operation->type = OPERATION_TRANSMIT;
    operation->id = message->server_id;

    operation->completion = s->cluster->time + s->network_latency;
    operation->transmit.from = s->raft.id;
    operation->transmit.message = *message;

    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            /* Create a copy of the entries being sent, so the memory of the
             * original message can be released when raft_done() is called. */
            copyEntries(message->append_entries.entries,
                        &operation->transmit.message.append_entries.entries,
                        message->append_entries.n_entries);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            /* Create a copy of the snapshot being sent, so the memory of the
             * original message can be released when raft_done() is called. */
            {
                struct raft_install_snapshot *src = &message->install_snapshot;
                struct raft_install_snapshot *dst =
                    &operation->transmit.message.install_snapshot;
                dst->last_index = src->last_index;
                dst->last_term = src->last_term;
                confCopy(&src->conf, &dst->conf);
                dst->conf_index = src->conf_index;
                dst->data.len = src->data.len;
                dst->data.base = raft_malloc(dst->data.len);
                munit_assert_ptr_not_null(dst->data.base);
                memcpy(dst->data.base, src->data.base, src->data.len);
            }
            break;
    }

    QUEUE_PUSH(&s->cluster->operations, &operation->queue);
}

static void serverCompleteEntries(struct test_server *s,
                                  struct operation *operation)
{
    struct raft_event event;
    unsigned i;

    /* Possibly truncate stale entries. */
    diskTruncateEntries(&s->disk, operation->entries.index);

    for (i = 0; i < operation->entries.n; i++) {
        diskPersistEntry(&s->disk, &operation->entries.batch[i]);
    }

    event.time = s->cluster->time;
    event.type = RAFT_PERSISTED_ENTRIES;
    event.persisted_entries.index = operation->entries.index;
    event.persisted_entries.n = operation->entries.n;
    event.persisted_entries.batch = operation->entries.batch;
    event.persisted_entries.status = 0;

    serverStep(s, &event);
}

static void serverCompleteSnapshot(struct test_server *s,
                                   struct operation *operation)
{
    struct test_snapshot *snapshot = munit_malloc(sizeof *snapshot);
    struct raft_event event;

    snapshot->metadata.index = operation->snapshot.metadata.index;
    snapshot->metadata.term = operation->snapshot.metadata.term;
    confCopy(&operation->snapshot.metadata.configuration,
             &snapshot->metadata.configuration);
    snapshot->metadata.configuration_index =
        operation->snapshot.metadata.configuration_index;

    snapshot->data = operation->snapshot.chunk;

    diskSetSnapshot(&s->disk, snapshot);

    event.time = s->cluster->time;
    event.type = RAFT_PERSISTED_SNAPSHOT;
    event.persisted_snapshot.metadata = operation->snapshot.metadata;
    event.persisted_snapshot.offset = operation->snapshot.offset;
    event.persisted_snapshot.chunk = operation->snapshot.chunk;
    event.persisted_snapshot.last = operation->snapshot.last;
    event.persisted_snapshot.status = 0;

    serverStep(s, &event);
}

static void serverCompleteSend(struct test_server *s,
                               struct operation *operation)
{
    struct raft_event event;
    queue *head;
    int status = 0;

    /* Check if there's a disconnection. */
    QUEUE_FOREACH (head, &s->cluster->disconnect) {
        struct disconnect *d = QUEUE_DATA(head, struct disconnect, queue);
        if (d->id1 == s->raft.id &&
            d->id2 == operation->send.message.server_id) {
            status = RAFT_NOCONNECTION;
            break;
        }
    }

    if (status == 0) {
        serverEnqueueTransmit(s, &operation->send.message);
    }

    event.time = s->cluster->time;
    event.type = RAFT_SENT;
    event.sent.message = operation->send.message;
    event.sent.status = status;

    serverStep(s, &event);
}

static struct test_server *clusterGetServer(struct test_cluster *c, raft_id id);

static void serverCompleteTransmit(struct test_server *s,
                                   struct operation *operation)
{
    struct test_server *sender;
    struct raft_event event;
    queue *head;

    if (!s->running) {
        clearTransmitOperation(operation);
        return;
    }

    /* Check if there's a disconnection. */
    QUEUE_FOREACH (head, &s->cluster->disconnect) {
        struct disconnect *d = QUEUE_DATA(head, struct disconnect, queue);
        if (d->id1 == operation->transmit.from && d->id2 == s->raft.id) {
            clearTransmitOperation(operation);
            return;
        }
    }

    sender = clusterGetServer(s->cluster, operation->transmit.from);

    event.time = s->cluster->time;
    event.type = RAFT_RECEIVE;
    event.receive.id = operation->transmit.from;
    event.receive.address = sender->raft.address;
    event.receive.message = &operation->transmit.message;

    serverStep(s, &event);
}

/* Complete either a @raft_io or transmit operation. */
static void serverCompleteOperation(struct test_server *s,
                                    struct operation *operation)
{
    munit_assert_ulong(s->raft.id, ==, operation->id);

    s->cluster->time = operation->completion;

    QUEUE_REMOVE(&operation->queue);
    switch (operation->type) {
        case OPERATION_ENTRIES:
            serverCompleteEntries(s, operation);
            break;
        case OPERATION_SNAPSHOT:
            serverCompleteSnapshot(s, operation);
            break;
        case OPERATION_SEND:
            serverCompleteSend(s, operation);
            break;
        case OPERATION_TRANSMIT:
            serverCompleteTransmit(s, operation);
            break;
        default:
            munit_errorf("unexpected operation type %d", operation->type);
            break;
    }
    free(operation);
}

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c)
{
    unsigned i;

    (void)params;

    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        diskInit(&c->servers[i].disk);
        serverInit(&c->servers[i], i + 1, c);
    }

    c->time = 0;
    c->in_tear_down = false;
    QUEUE_INIT(&c->operations);
    QUEUE_INIT(&c->disconnect);
}

/* Return the server with the given @id. */
static struct test_server *clusterGetServer(struct test_cluster *c, raft_id id)
{
    munit_assert_ulong(id, <=, TEST_CLUSTER_N_SERVERS);
    return &c->servers[id - 1];
}

void test_cluster_tear_down(struct test_cluster *c)
{
    unsigned i;

    c->in_tear_down = true;

    /* Drop pending operations */
    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        struct test_server *server = &c->servers[i];
        serverCancelPendingOperations(server);
    }
    munit_assert_true(QUEUE_IS_EMPTY(&c->operations));

    /* Drop outstanding disconnections */
    while (!QUEUE_IS_EMPTY(&c->disconnect)) {
        struct disconnect *disconnect;
        queue *head;
        head = QUEUE_HEAD(&c->disconnect);
        disconnect = QUEUE_DATA(head, struct disconnect, queue);
        QUEUE_REMOVE(&disconnect->queue);
        free(disconnect);
    }

    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        serverClose(&c->servers[i]);
    }
}

struct raft *test_cluster_raft(struct test_cluster *c, raft_id id)
{
    struct test_server *server = clusterGetServer(c, id);
    return &server->raft;
}

void test_cluster_set_term(struct test_cluster *c, raft_id id, raft_term term)
{
    struct test_server *server = clusterGetServer(c, id);
    munit_assert_false(server->running);
    diskSetTerm(&server->disk, term);
}

void test_cluster_set_vote(struct test_cluster *c, raft_id id, raft_id vote)
{
    struct test_server *server = clusterGetServer(c, id);
    munit_assert_false(server->running);
    diskSetVote(&server->disk, vote);
}

void test_cluster_set_snapshot(struct test_cluster *c,
                               raft_id id,
                               struct test_snapshot *snapshot)
{
    struct test_server *server = clusterGetServer(c, id);
    munit_assert_false(server->running);
    diskSetSnapshot(&server->disk, snapshot);
}

void test_cluster_add_entry(struct test_cluster *c,
                            raft_id id,
                            const struct raft_entry *entry)
{
    struct test_server *server = clusterGetServer(c, id);
    munit_assert_false(server->running);
    diskAddEntry(&server->disk, entry);
}

void test_cluster_set_election_timeout(struct test_cluster *c,
                                       raft_id id,
                                       unsigned timeout,
                                       unsigned delta)
{
    struct test_server *server = clusterGetServer(c, id);
    serverSetElectionTimeout(server, timeout, delta);
}

void test_cluster_set_network_latency(struct test_cluster *c,
                                      raft_id id,
                                      unsigned latency)
{
    struct test_server *server = clusterGetServer(c, id);
    server->network_latency = latency;
}

void test_cluster_set_disk_latency(struct test_cluster *c,
                                   raft_id id,
                                   unsigned latency)
{
    struct test_server *server = clusterGetServer(c, id);
    server->disk_latency = latency;
}

void test_cluster_start(struct test_cluster *c, raft_id id)
{
    struct test_server *server = clusterGetServer(c, id);
    serverStart(server);
}

void test_cluster_stop(struct test_cluster *c, raft_id id)
{
    struct test_server *server = clusterGetServer(c, id);
    serverCancelPendingOperations(server);
    serverStop(server);
}

void test_cluster_submit(struct test_cluster *c,
                         raft_id id,
                         struct raft_entry *entry)
{
    struct test_server *server = clusterGetServer(c, id);
    struct raft_event event;

    event.time = c->time;
    event.type = RAFT_SUBMIT;
    event.submit.n = 1;
    event.submit.entries = entry;

    serverStep(server, &event);
}

/* Update the PNRG seed of each server, to match the expected randomized
 * election timeout. */
static void clusterSeed(struct test_cluster *c)
{
    unsigned i;
    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        struct test_server *server = &c->servers[i];
        serverSeed(server);
    }
}

/* Return the operation with the lowest completion time. */
static struct operation *clusterGetOperationWithEarliestCompletion(
    struct test_cluster *c)
{
    struct operation *operation = NULL;
    queue *head;
    QUEUE_FOREACH (head, &c->operations) {
        struct operation *other = QUEUE_DATA(head, struct operation, queue);
        if (operation == NULL || other->completion < operation->completion) {
            operation = other;
        }
    }

    return operation;
}

/* Return the server with the earliest raft_timeout() value. */
static struct test_server *clusterGetServerWithEarliestTimeout(
    struct test_cluster *c)
{
    struct test_server *server = NULL;
    unsigned i;
    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        struct test_server *other = &c->servers[i];
        if (!other->running) {
            continue;
        }
        if (server == NULL || other->timeout < server->timeout) {
            server = other;
        }
    }
    return server;
}

void test_cluster_step(struct test_cluster *c)
{
    struct test_server *server;
    struct operation *operation;

    clusterSeed(c);

    server = clusterGetServerWithEarliestTimeout(c);
    operation = clusterGetOperationWithEarliestCompletion(c);

    if (operation == NULL || server->timeout < operation->completion) {
        serverTimeout(server);
    } else {
        server = clusterGetServer(c, operation->id);
        serverCompleteOperation(server, operation);
    }
}

void test_cluster_elapse(struct test_cluster *c, unsigned msecs)
{
    struct test_server *server;
    struct operation *operation;
    raft_time time = c->time + msecs;

    server = clusterGetServerWithEarliestTimeout(c);
    munit_assert_ullong(time, <, server->timeout);

    operation = clusterGetOperationWithEarliestCompletion(c);
    if (operation != NULL) {
        munit_assert_ullong(time, <=, operation->completion);
    }

    c->time = time;
}

void test_cluster_disconnect(struct test_cluster *c, raft_id id1, raft_id id2)
{
    struct disconnect *disconnect = munit_malloc(sizeof *disconnect);
    disconnect->id1 = id1;
    disconnect->id2 = id2;
    QUEUE_PUSH(&c->disconnect, &disconnect->queue);
}

void test_cluster_reconnect(struct test_cluster *c, raft_id id1, raft_id id2)
{
    queue *head;
    QUEUE_FOREACH (head, &c->disconnect) {
        struct disconnect *d = QUEUE_DATA(head, struct disconnect, queue);
        if (d->id1 == id1 && d->id2 == id2) {
            QUEUE_REMOVE(&d->queue);
            free(d);
            return;
        }
    }
}

void test_cluster_kill(struct test_cluster *c, raft_id id)
{
    struct test_server *server = clusterGetServer(c, id);
    server->running = false;
}

bool test_cluster_trace(struct test_cluster *c, const char *expected)
{
    size_t n1;
    size_t n2;
    size_t i;
    unsigned max_steps = 100;

consume:
    if (max_steps == 0) {
        goto mismatch;
    }
    max_steps -= 1;

    n1 = strlen(c->trace);
    n2 = strlen(expected);

    for (i = 0; i < n1 && i < n2; i++) {
        if (c->trace[i] != expected[i]) {
            break;
        }
    }

    /* Check if we produced more output than the expected one. */
    if (n1 > n2) {
        goto mismatch;
    }

    /* If there's more expected output, check that so far we're good, then
     * step and repeat. */
    if (n1 < n2) {
        if (i != n1) {
            goto mismatch;
        }
        c->trace[0] = 0;
        expected += i;
        test_cluster_step(c);
        goto consume;
    }

    munit_assert_ulong(n1, ==, n2);
    if (i != n1) {
        goto mismatch;
    }

    c->trace[0] = 0;

    return true;

mismatch:
    fprintf(stderr, "==> Expected:\n");
    fprintf(stderr, "%s\n", expected);

    fprintf(stderr, "==> Actual:\n");
    fprintf(stderr, "%s\n", c->trace);

    return false;
}

static void randomize(struct raft_fixture *f, unsigned i, int what)
{
    struct raft *raft = raft_fixture_get(f, i);
    switch (what) {
        case RAFT_FIXTURE_TICK:
            /* TODO: provide an API to inspect how much time has elapsed since
             * the last election timer reset */
            if (raft->election_timer_start == raft->io->time(raft->io)) {
                raft_fixture_set_randomized_election_timeout(
                    f, i,
                    munit_rand_int_range(raft->election_timeout,
                                         raft->election_timeout * 2));
            }
            break;
        case RAFT_FIXTURE_DISK:
            raft_fixture_set_disk_latency(f, i, munit_rand_int_range(10, 25));
            break;
        case RAFT_FIXTURE_NETWORK:
            raft_fixture_set_network_latency(f, i,
                                             munit_rand_int_range(25, 50));
            break;
        default:
            munit_assert(0);
            break;
    }
}

void cluster_randomize_init(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < raft_fixture_n(f); i++) {
        randomize(f, i, RAFT_FIXTURE_TICK);
        randomize(f, i, RAFT_FIXTURE_DISK);
        randomize(f, i, RAFT_FIXTURE_NETWORK);
    }
}

void cluster_randomize(struct raft_fixture *f, struct raft_fixture_event *event)
{
    munit_assert(!v1);
    unsigned index = raft_fixture_event_server_index(event);
    int type = raft_fixture_event_type(event);
    randomize(f, index, type);
}
