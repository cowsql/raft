#define TEST_CLUSTER_V1

#include "../../src/queue.h"

#include "cluster.h"

/* Defaults */
#define DEFAULT_ELECTION_TIMEOUT 100
#define DEFAULT_HEARTBEAT_TIMEOUT 50
#define DEFAULT_NETWORK_LATENCY 10
#define DEFAULT_SNAPSHOT_THRESHOLD 64
#define DEFAULT_SNAPSHOT_TRAILING 32
#define DEFAULT_DISK_LATENCY 10
#define DEFAULT_DISK_SIZE 256 /* In bytes */

/* Maximum number of log entries. */
#define MAX_LOG_ENTRIES 15

/* Track the event to to fire in a cluster step. */
struct step
{
    raft_id id;              /* Target server ID. */
    struct raft_event event; /* Event to fire. */
    union {
        struct
        {
            struct raft_entry *batch;
            unsigned n;
        } entries;
        struct
        {
            struct raft_buffer chunk;
        } snapshot;
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
    d->size = DEFAULT_DISK_SIZE;
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

/* Return the remaining disk capacity. */
static unsigned short diskCapacity(struct test_disk *d)
{
    unsigned short capacity = d->size;
    unsigned i;

    if (d->snapshot != NULL) {
        munit_assert_ullong(d->snapshot->data.len, >, 0);
        munit_assert_ullong(capacity, >=, d->snapshot->data.len);
        capacity -= d->snapshot->data.len;
    }

    for (i = 0; i < d->n_entries; i++) {
        struct raft_entry *entry = &d->entries[i];
        munit_assert_ullong(entry->buf.len, >, 0);
        munit_assert_ullong(capacity, >=, entry->buf.len);
        capacity -= entry->buf.len;
    }

    return capacity;
}

/* Set the persisted snapshot. */
static void diskSetSnapshot(struct test_disk *d, struct test_snapshot *snapshot)
{
    diskDestroySnapshotIfPresent(d);

    munit_assert_ptr_not_null(snapshot->data.base);
    munit_assert_ullong(snapshot->data.len, >, 0);

    munit_assert_ullong(diskCapacity(d), >=, snapshot->data.len);

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
    munit_assert_ullong(diskCapacity(d), >=, entry->buf.len);
    d->n_entries++;
    d->entries = realloc(d->entries, d->n_entries * sizeof *d->entries);
    munit_assert_ptr_not_null(d->entries);
    entryCopy(entry, &d->entries[d->n_entries - 1]);
}

/* Get the entry at the given index. */
static const struct raft_entry *diskGetEntry(struct test_disk *d,
                                             raft_index index)
{
    unsigned i;

    if (index < d->start_index) {
        return NULL;
    }

    i = (unsigned)(index - d->start_index);
    if (i > d->n_entries - 1) {
        return NULL;
    }

    return &d->entries[i];
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

/* Load snapshot data into the given buffer, asserting that index and term match
 * the given metadata. */
static void diskLoadSnapshotData(struct test_disk *d,
                                 raft_index index,
                                 raft_term term,
                                 struct raft_buffer *data)
{
    munit_assert_ptr_not_null(d->snapshot);
    munit_assert_ullong(d->snapshot->metadata.index, ==, index);
    munit_assert_ullong(d->snapshot->metadata.term, ==, term);

    data->len = d->snapshot->data.len;
    data->base = raft_malloc(data->len);
    munit_assert_ptr_not_null(data->base);

    memcpy(data->base, d->snapshot->data.base, data->len);
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
/* Custom emit tracer function which includes the server ID. */
static void serverEmit(struct raft_tracer *t, int type, const void *data)
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
    unsigned delta;
    int rv;

    s->tracer.impl = s;
    s->tracer.version = 2;
    s->tracer.emit = serverEmit;
    s->randomized_election_timeout_prev = 0;

    sprintf(s->address, "%llu", id);

    rv = raft_init(&s->raft, NULL, NULL, id, s->address);
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
    raft_set_install_snapshot_timeout(&s->raft, 50);

    raft_set_capacity_threshold(&s->raft, 64);

    s->log.start = 1;
    s->log.entries = munit_malloc(MAX_LOG_ENTRIES * sizeof *s->log.entries);
    s->log.n = 0;

    s->last_applied = 0;
    s->cluster = cluster;
    s->network_latency = DEFAULT_NETWORK_LATENCY;
    s->disk_latency = DEFAULT_DISK_LATENCY;
    s->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    s->snapshot.trailing = DEFAULT_SNAPSHOT_TRAILING;
    s->snapshot.installing = false;
    s->running = false;
}

static int serverStep(struct test_server *s, struct raft_event *event);

static void serverCancelEntries(struct test_server *s, struct step *step)
{
    (void)s;

    raft_free(step->entries.batch[0].batch);
    raft_free(step->entries.batch);
}

static void serverCancelSnapshot(struct test_server *s, struct step *step)
{
    struct raft_event *event = &step->event;
    (void)s;

    raft_free(step->snapshot.chunk.base);
    raft_configuration_close(&event->persisted_snapshot.metadata.configuration);
}

/* Release the memory used by a RAFT_RECEIVE event. */
static void dropReceiveEvent(struct step *step)
{
    struct raft_event *event = &step->event;
    switch (event->receive.message->type) {
        case RAFT_APPEND_ENTRIES:
            if (event->receive.message->append_entries.n_entries > 0) {
                struct raft_entry *entries =
                    event->receive.message->append_entries.entries;
                raft_free(entries[0].buf.base);
                raft_free(entries);
            }
            break;
        case RAFT_INSTALL_SNAPSHOT:
            raft_configuration_close(
                &event->receive.message->install_snapshot.conf);
            raft_free(event->receive.message->install_snapshot.data.base);
            break;
        default:
            break;
    }

    free(event->receive.message);
}

/* Cancel all pending steps targeted to the given server */
static void serverCancelPending(struct test_server *s)
{
    while (1) {
        struct step *step = NULL;
        queue *head;
        QUEUE_FOREACH (head, &s->cluster->steps) {
            struct step *current;
            current = QUEUE_DATA(head, struct step, queue);
            if (current->id == s->raft.id) {
                step = current;
                break;
            }
        }
        if (step == NULL) {
            break;
        }

        switch (step->event.type) {
            case RAFT_PERSISTED_ENTRIES:
                serverCancelEntries(s, step);
                break;
            case RAFT_PERSISTED_SNAPSHOT:
                serverCancelSnapshot(s, step);
                break;
            case RAFT_RECEIVE:
                dropReceiveEvent(step);
                break;
            case RAFT_CONFIGURATION:
                raft_configuration_close(&step->event.configuration.conf);
                break;
            default:
                break;
        }
        QUEUE_REMOVE(&step->queue);

        free(step);
    }

    while (1) {
        struct step *step = NULL;
        queue *head;
        QUEUE_FOREACH (head, &s->cluster->send) {
            struct step *current;
            struct raft_message *message;
            current = QUEUE_DATA(head, struct step, queue);
            munit_assert_uint(current->event.type, ==, RAFT_RECEIVE);
            message = current->event.receive.message;
            if (message->server_id /* sender */ == s->raft.id) {
                step = current;
                break;
            }
        }
        if (step == NULL) {
            break;
        }

        QUEUE_REMOVE(&step->queue);

        dropReceiveEvent(step);
        free(step);
    }
}

static void serverStop(struct test_server *s)
{
    unsigned i;

    s->running = false;

    for (i = 0; i < s->log.n; i++) {
        free(s->log.entries[i].buf.base);
    }
    free(s->log.entries);

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
    free(s->log.entries);
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

/* Truncate all in-memory entries from the given index onwards. If there are no
 * entries at the given index, this is a no-op. */
static void serverTruncateEntries(struct test_server *s, raft_index index)
{
    unsigned i;
    unsigned n = s->log.n;

    if (index == s->log.start + n) {
        return;
    }

    munit_assert_ulong(index, >=, s->log.start);
    munit_assert_ulong(index, <=, s->log.start + n);

    for (i = (unsigned)(index - s->log.start); i < n; i++) {
        free(s->log.entries[i].buf.base);
        s->log.n--;
    }
}

static void serverAddEntry(struct test_server *s,
                           const struct raft_entry *entry)
{
    s->log.n++;
    munit_assert_uint(s->log.n, <=, MAX_LOG_ENTRIES);
    entryCopy(entry, &s->log.entries[s->log.n - 1]);
}

static void copyEntries(const struct raft_entry *src,
                        struct raft_entry **dst,
                        const size_t n);

static void serverProcessEntries(struct test_server *s,
                                 raft_index first_index,
                                 struct raft_entry *entries,
                                 unsigned n)
{
    struct step *step = munit_malloc(sizeof *step);
    struct raft_event *event = &step->event;
    unsigned i;

    serverTruncateEntries(s, first_index);

    for (i = 0; i < n; i++) {
        serverAddEntry(s, &entries[i]);
    }

    copyEntries(entries, &step->entries.batch, n);
    step->entries.n = n;

    if (n > 0) {
        munit_assert_ptr_not_null(entries[0].batch);
        raft_free(entries[0].batch);
    }

    step->id = s->raft.id;

    event->time = s->cluster->time + s->disk_latency;
    event->type = RAFT_PERSISTED_ENTRIES;

    event->persisted_entries.index = first_index + n - 1;

    QUEUE_PUSH(&s->cluster->steps, &step->queue);
}

static void serverProcessSnapshot(struct test_server *s,
                                  struct raft_snapshot_metadata *metadata,
                                  size_t offset,
                                  struct raft_buffer *chunk,
                                  bool last)
{
    struct step *step = munit_malloc(sizeof *step);

    munit_assert_false(s->snapshot.installing);

    s->snapshot.installing = true;

    step->id = s->raft.id;

    /* Update the in-memory log */
    serverTruncateEntries(s, s->log.start);
    munit_assert_uint(s->log.n, ==, 0);
    s->log.start = metadata->index + 1;

    step->snapshot.chunk = *chunk;

    step->event.time = s->cluster->time + s->disk_latency;
    step->event.type = RAFT_PERSISTED_SNAPSHOT;

    step->event.persisted_snapshot.metadata = *metadata;
    step->event.persisted_snapshot.offset = offset;
    step->event.persisted_snapshot.last = last;

    QUEUE_PUSH(&s->cluster->steps, &step->queue);
}

static void serverFillAppendEntries(struct test_server *s,
                                    struct raft_append_entries *args);

static void serverFillInstallSnapshot(struct test_server *s,
                                      struct raft_install_snapshot *args);

static void serverProcessMessages(struct test_server *s,
                                  struct raft_message *messages,
                                  unsigned n)
{
    unsigned i;

    for (i = 0; i < n; i++) {
        struct step *step = munit_malloc(sizeof *step);
        struct raft_event *event = &step->event;
        struct raft_message *message = &messages[i];

        step->id = message->server_id;

        event->time = s->cluster->time + s->network_latency;
        event->type = RAFT_RECEIVE;
        event->receive.message = munit_malloc(sizeof *event->receive.message);
        *event->receive.message = *message;
        event->receive.message->server_id = s->raft.id;
        event->receive.message->server_address = s->address;

        switch (message->type) {
            case RAFT_APPEND_ENTRIES:
                serverFillAppendEntries(
                    s, &event->receive.message->append_entries);
                break;
            case RAFT_INSTALL_SNAPSHOT:
                serverFillInstallSnapshot(
                    s, &event->receive.message->install_snapshot);
                break;
            default:
                break;
        }

        QUEUE_PUSH(&s->cluster->send, &step->queue);
    }
}

static void serverMaybeTakeSnapshot(struct test_server *s)
{
    struct step *step;
    struct raft *r = &s->raft;
    struct raft_event *event;
    raft_index last_snapshot_index = 0;

    if (s->disk.snapshot != NULL) {
        last_snapshot_index = s->disk.snapshot->metadata.index;
    }

    if (raft_commit_index(r) - last_snapshot_index < s->snapshot.threshold) {
        return;
    }

    step = munit_malloc(sizeof *step);
    step->id = s->raft.id;

    event = &step->event;
    event->time = s->cluster->time;
    event->type = RAFT_SNAPSHOT;

    event->snapshot.metadata.index = raft_commit_index(r);
    event->snapshot.metadata.term = raft_current_term(r);

    /* XXX: assume there is no uncommitted configuration. */
    munit_assert_ullong(r->configuration_uncommitted_index, ==, 0);
    munit_assert_ullong(r->configuration_committed_index, >, 0);

    confCopy(&r->configuration, &event->snapshot.metadata.configuration);
    event->snapshot.metadata.configuration_index =
        r->configuration_committed_index;

    event->snapshot.trailing = s->snapshot.trailing;

    QUEUE_PUSH(&s->cluster->steps, &step->queue);
}

static void serverProcessCommitIndex(struct test_server *s)
{
    struct step *step;
    const struct raft_entry *entry;
    raft_index commit_index = raft_commit_index(&s->raft);
    raft_index index;
    int rv;

    for (index = s->last_applied + 1; index <= commit_index; index++) {
        entry = diskGetEntry(&s->disk, index);

        if (entry == NULL || entry->type != RAFT_CHANGE) {
            continue;
        }

        step = munit_malloc(sizeof *step);
        step->id = s->raft.id;
        step->event.time = s->cluster->time;
        step->event.type = RAFT_CONFIGURATION;
        step->event.configuration.index = index;

        rv = raft_configuration_decode(&entry->buf,
                                       &step->event.configuration.conf);
        munit_assert_int(rv, ==, 0);

        QUEUE_PUSH(&s->cluster->steps, &step->queue);
    }

    s->last_applied = commit_index;

    serverMaybeTakeSnapshot(s);
}

/* Fire the given event using raft_step() and process the resulting struct
 * raft_update object. */
static int serverStep(struct test_server *s, struct raft_event *event)
{
    struct raft *r = &s->raft;
    struct raft_update update;
    int rv;

    event->capacity = diskCapacity(&s->disk);

    munit_assert_true(s->running);

    rv = raft_step(r, event, &update);
    if (rv != 0) {
        return rv;
    }

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

    return 0;
}

/* Start the server by passing to raft_step() a RAFT_START event with the
 * current disk state. */
static void serverStart(struct test_server *s)
{
    struct raft_event event;
    unsigned i;
    int rv;

    s->running = true;

    serverSeed(s);

    event.time = s->cluster->time;
    event.type = RAFT_START;

    diskLoad(&s->disk, &event.start.term, &event.start.voted_for,
             &event.start.metadata, &event.start.start_index,
             &event.start.entries, &event.start.n_entries);

    s->log.start = event.start.start_index;
    s->log.n = event.start.n_entries;
    munit_assert_uint(s->log.n, <=, MAX_LOG_ENTRIES);
    for (i = 0; i < s->log.n; i++) {
        entryCopy(&event.start.entries[i], &s->log.entries[i]);
    }

    rv = serverStep(s, &event);
    munit_assert_int(rv, ==, 0);

    if (event.start.metadata != NULL) {
        free(event.start.metadata);
    }

    if (event.start.entries != NULL) {
        raft_free(event.start.entries[0].batch);
        raft_free(event.start.entries);
    }
}

/* Fire a RAFT_TIMEOUT event. */
static void serverTimeout(struct test_server *s)
{
    struct raft_event event;
    int rv;

    s->cluster->time = s->timeout;

    event.time = s->cluster->time;
    event.type = RAFT_TIMEOUT;

    rv = serverStep(s, &event);
    munit_assert_int(rv, ==, 0);
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

/* Use the log cache to populate the given AppendEntries message. */
static void serverFillAppendEntries(struct test_server *s,
                                    struct raft_append_entries *args)
{
    raft_index index = args->prev_log_index + 1;
    unsigned i;

    munit_assert_ullong(index, >=, s->log.start);
    munit_assert_ullong(index + args->n_entries, <=, s->log.start + s->log.n);

    if (args->n_entries == 0) {
        args->entries = NULL;
        return;
    }

    i = (unsigned)(index - s->log.start);
    copyEntries(&s->log.entries[i], &args->entries, args->n_entries);
}

/* Load from disk the data of the snapshot being sent. */
static void serverFillInstallSnapshot(struct test_server *s,
                                      struct raft_install_snapshot *args)
{
    struct raft_snapshot_metadata metadata;
    diskLoadSnapshotData(&s->disk, args->last_index, args->last_term,
                         &args->data);
    diskLoadSnapshotMetadata(&s->disk, &metadata);
    args->conf = metadata.configuration;
    args->conf_index = metadata.configuration_index;
}

static void serverCompleteEntries(struct test_server *s, struct step *step)
{
    struct raft_event *event = &step->event;
    struct raft_entry *entries = step->entries.batch;
    raft_index index = event->persisted_entries.index;
    unsigned n = step->entries.n;
    unsigned i;
    int rv;

    /* Possibly truncate stale entries. */
    diskTruncateEntries(&s->disk, index - n + 1);

    for (i = 0; i < n; i++) {
        diskAddEntry(&s->disk, &entries[i]);
    }

    if (!s->snapshot.installing) {
        rv = serverStep(s, event);
        munit_assert_int(rv, ==, 0);
    }

    if (n > 0) {
        raft_free(step->entries.batch[0].batch);
        raft_free(step->entries.batch);
    }
}

static void serverCompleteSnapshot(struct test_server *s, struct step *step)
{
    struct test_snapshot *snapshot = munit_malloc(sizeof *snapshot);
    struct raft_event *event = &step->event;
    int rv;

    s->snapshot.installing = false;

    snapshot->metadata.index = event->persisted_snapshot.metadata.index;
    snapshot->metadata.term = event->persisted_snapshot.metadata.term;
    confCopy(&event->persisted_snapshot.metadata.configuration,
             &snapshot->metadata.configuration);
    snapshot->metadata.configuration_index =
        event->persisted_snapshot.metadata.configuration_index;

    snapshot->data = step->snapshot.chunk;

    diskSetSnapshot(&s->disk, snapshot);

    rv = serverStep(s, event);
    munit_assert_int(rv, ==, 0);
}

/* Return true if the server with id1 is connected with the server with id2 */
static bool clusterAreConnected(struct test_cluster *c,
                                raft_id id1,
                                raft_id id2)
{
    bool connected = true;
    queue *head;

    /* Check if there's a disconnection. */
    QUEUE_FOREACH (head, &c->disconnect) {
        struct disconnect *d = QUEUE_DATA(head, struct disconnect, queue);
        if (d->id1 == id1 && d->id2 == id2) {
            connected = false;
            break;
        }
    }

    return connected;
}

static void serverCompleteReceive(struct test_server *s, struct step *step)
{
    struct raft_event *event = &step->event;
    int rv;

    if (!s->running) {
        dropReceiveEvent(step);
        return;
    }

    /* Check if there's a disconnection. */
    if (!clusterAreConnected(s->cluster,
                             event->receive.message->server_id /* sender */,
                             s->raft.id /* receiver */)) {
        dropReceiveEvent(step);
        return;
    }

    rv = serverStep(s, event);
    munit_assert_int(rv, ==, 0);

    if (event->receive.message->type == RAFT_APPEND_ENTRIES) {
        raft_free(event->receive.message->append_entries.entries);
    }

    free(event->receive.message);
}

static void serverCompleteConfiguration(struct test_server *s,
                                        struct step *step)
{
    struct raft *r = &s->raft;
    struct raft_event *event = &step->event;
    raft_index commit_index = raft_commit_index(r);
    int rv;

    rv = serverStep(s, event);
    munit_assert_int(rv, ==, 0);

    /* The last call to raft_step() did not change the commit index. */
    munit_assert_ullong(raft_commit_index(r), ==, commit_index);
}

static void serverCompleteTakeSnapshot(struct test_server *s, struct step *step)
{
    struct test_snapshot *snapshot = munit_malloc(sizeof *snapshot);
    struct raft_event *event = &step->event;
    int rv;

    /* XXX: this assumes that the current term is the term of the last committed
     * entry. */
    snapshot->metadata.index = event->snapshot.metadata.index;
    snapshot->metadata.term = event->snapshot.metadata.term;

    confCopy(&event->snapshot.metadata.configuration,
             &snapshot->metadata.configuration);

    snapshot->data.len = 8;
    snapshot->data.base = munit_malloc(snapshot->data.len);

    diskSetSnapshot(&s->disk, snapshot);

    rv = serverStep(s, event);
    munit_assert_int(rv, ==, 0);
}

/* Complete some event involving I/O or user actions. */
static void serverComplete(struct test_server *s, struct step *step)
{
    munit_assert_ulong(s->raft.id, ==, step->id);

    s->cluster->time = step->event.time;

    QUEUE_REMOVE(&step->queue);
    switch (step->event.type) {
        case RAFT_PERSISTED_ENTRIES:
            serverCompleteEntries(s, step);
            break;
        case RAFT_PERSISTED_SNAPSHOT:
            serverCompleteSnapshot(s, step);
            break;
        case RAFT_RECEIVE:
            serverCompleteReceive(s, step);
            break;
        case RAFT_CONFIGURATION:
            serverCompleteConfiguration(s, step);
            break;
        case RAFT_SNAPSHOT:
            serverCompleteTakeSnapshot(s, step);
            break;
        default:
            munit_errorf("unexpected step type %d", step->event.type);
            break;
    }
    free(step);
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
    QUEUE_INIT(&c->steps);
    QUEUE_INIT(&c->send);
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

    /* Cancel pending steps */
    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        struct test_server *server = &c->servers[i];
        serverCancelPending(server);
    }
    munit_assert_true(QUEUE_IS_EMPTY(&c->steps));
    munit_assert_true(QUEUE_IS_EMPTY(&c->send));

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

void test_cluster_set_snapshot_threshold(struct test_cluster *c,
                                         raft_id id,
                                         unsigned threshold)
{
    struct test_server *server = clusterGetServer(c, id);
    server->snapshot.threshold = threshold;
}

void test_cluster_set_snapshot_trailing(struct test_cluster *c,
                                        raft_id id,
                                        unsigned trailing)
{
    struct test_server *server = clusterGetServer(c, id);
    server->snapshot.trailing = trailing;
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
    serverCancelPending(server);
    serverStop(server);
}

int test_cluster_submit(struct test_cluster *c,
                        raft_id id,
                        struct raft_entry *entry)
{
    struct test_server *server = clusterGetServer(c, id);
    struct raft_event event;

    event.time = c->time;
    event.type = RAFT_SUBMIT;
    event.submit.n = 1;
    event.submit.entries = entry;

    return serverStep(server, &event);
}

void test_cluster_catch_up(struct test_cluster *c,
                           raft_id id,
                           raft_id catch_up_id)
{
    struct test_server *server = clusterGetServer(c, id);
    struct raft_event event;
    int rv;

    event.time = c->time;
    event.type = RAFT_CATCH_UP;
    event.catch_up.server_id = catch_up_id;

    rv = serverStep(server, &event);
    munit_assert_int(rv, ==, 0);
}

void test_cluster_transfer(struct test_cluster *c,
                           raft_id id,
                           raft_id transferee)
{
    struct test_server *server = clusterGetServer(c, id);
    struct raft_event event;
    int rv;

    event.time = c->time;
    event.type = RAFT_TRANSFER;
    event.transfer.server_id = transferee;

    rv = serverStep(server, &event);
    munit_assert_int(rv, ==, 0);
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

/* Return the schedule step with the lowest execution time. */
static struct step *clusterGetStepWithEarliestExecution(struct test_cluster *c)
{
    struct step *step = NULL;
    queue *head;
    QUEUE_FOREACH (head, &c->steps) {
        struct step *other = QUEUE_DATA(head, struct step, queue);
        if (step == NULL || other->event.time < step->event.time) {
            step = other;
        }
    }

    return step;
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

/* Consume the queue of pending messages to be sent, and enqueue them as regular
 * RAFT_RECEIVE events in the steps queue . */
static void clusterEnqueueReceives(struct test_cluster *c)
{
    while (!QUEUE_IS_EMPTY(&c->send)) {
        struct step *step;
        queue *head;
        struct raft_message *message;
        head = QUEUE_HEAD(&c->send);
        step = QUEUE_DATA(head, struct step, queue);
        munit_assert_uint(step->event.type, ==, RAFT_RECEIVE);
        QUEUE_REMOVE(&step->queue);
        message = step->event.receive.message;

        if (!clusterAreConnected(c, message->server_id /* sender */,
                                 step->id /* receiver */)) {
            dropReceiveEvent(step);
            free(step);
            continue;
        }

        QUEUE_PUSH(&c->steps, &step->queue);
    }
}

void test_cluster_step(struct test_cluster *c)
{
    struct test_server *server;
    struct step *step;

    clusterEnqueueReceives(c);
    clusterSeed(c);

    server = clusterGetServerWithEarliestTimeout(c);
    step = clusterGetStepWithEarliestExecution(c);

    if (step == NULL || server->timeout < step->event.time) {
        serverTimeout(server);
    } else {
        server = clusterGetServer(c, step->id);
        serverComplete(server, step);
    }
}

void test_cluster_elapse(struct test_cluster *c, unsigned msecs)
{
    raft_time time = c->time + msecs;

    while (1) {
        struct test_server *server;
        struct step *step;

        clusterEnqueueReceives(c);

        server = clusterGetServerWithEarliestTimeout(c);
        step = clusterGetStepWithEarliestExecution(c);

        /* If no server and no step is due to timeout/complete before the
         * target time, then we can jump directly to that time. */
        if (time <= server->timeout &&
            (step == NULL || time <= step->event.time)) {
            break;
        }

        /* Otherwise, process those events first. */
        test_cluster_step(c);
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
