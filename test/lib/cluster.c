#define TEST_CLUSTER_V1

#include "../../src/queue.h"

#include "cluster.h"

/* Defaults */
#define DEFAULT_ELECTION_TIMEOUT 100
#define DEFAULT_HEARTBEAT_TIMEOUT 40
#define DEFAULT_NETWORK_LATENCY 10
#define DEFAULT_DISK_LATENCY 10

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
        *metadata = raft_malloc(sizeof **metadata);
        munit_assert_ptr_not_null(*metadata);
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

/* Custom emit tracer function which includes the server ID. */
static void serverTrace(struct raft_tracer *t, int type, const void *data)
{
    struct test_server *server;
    struct test_cluster *cluster;
    const struct raft_tracer_info *info = data;
    char trace[1024];

    if (type != RAFT_TRACER_DIAGNOSTIC) {
        return;
    }

    server = t->impl;
    cluster = server->cluster;
    if (info->diagnostic.level > 3) {
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

/* Initialize a new server object. */
static void serverInit(struct test_server *s,
                       raft_id id,
                       struct test_cluster *cluster)
{
    char address[64];
    int rv;

    diskInit(&s->disk);

    s->tracer.impl = s;
    s->tracer.version = 2;
    s->tracer.trace = serverTrace;

    sprintf(address, "%llu", id);

    rv = raft_init(&s->raft, NULL, NULL, id, address);
    munit_assert_int(rv, ==, 0);

    s->raft.tracer = &s->tracer;

    raft_set_election_timeout(&s->raft, DEFAULT_ELECTION_TIMEOUT);
    raft_set_heartbeat_timeout(&s->raft, DEFAULT_HEARTBEAT_TIMEOUT);

    s->cluster = cluster;
    s->network_latency = DEFAULT_NETWORK_LATENCY;
    s->disk_latency = DEFAULT_DISK_LATENCY;
    s->running = false;
}

/* Release all resources used by a server object. */
static void serverClose(struct test_server *s)
{
    raft_close(&s->raft, NULL);
    diskClose(&s->disk);
}

/* Start the server by passing to raft_step() a RAFT_START event with the
 * current disk state. */
static void serverStart(struct test_server *s)
{
    struct raft_event event;
    struct raft_update update;
    struct raft *r = &s->raft;
    int rv;

    event.time = s->cluster->time;
    event.type = RAFT_START;

    diskLoad(&s->disk, &event.start.term, &event.start.voted_for,
             &event.start.metadata, &event.start.start_index,
             &event.start.entries, &event.start.n_entries);

    rv = raft_step(r, &event, &update);
    munit_assert_int(rv, ==, 0);

    /* Upon startup we don't expect any new state to be persisted or messages
     * being sent. */
    munit_assert_false(update.flags & RAFT_UPDATE_CURRENT_TERM);
    munit_assert_false(update.flags & RAFT_UPDATE_VOTED_FOR);
    munit_assert_false(update.flags & RAFT_UPDATE_ENTRIES);
    munit_assert_false(update.flags & RAFT_UPDATE_SNAPSHOT);
    munit_assert_false(update.flags & RAFT_UPDATE_MESSAGES);

    /* The state must have transitioned either to follower or leader (when
     * self-electing). */
    munit_assert_true(update.flags & RAFT_UPDATE_STATE);
    munit_assert_true(raft_state(r) == RAFT_FOLLOWER ||
                      raft_state(r) == RAFT_LEADER);

    s->running = true;
    s->timeout = raft_timeout(&s->raft);
}

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c)
{
    unsigned i;

    (void)params;

    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        serverInit(&c->servers[i], i + 1, c);
    }

    c->time = 0;
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

    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        serverClose(&c->servers[i]);
    }
}

void test_cluster_set_term(struct test_cluster *c, raft_id id, raft_term term)
{
    struct test_server *server = clusterGetServer(c, id);
    munit_assert_false(server->running);
    diskSetTerm(&server->disk, term);
}

void test_cluster_start(struct test_cluster *c, raft_id id)
{
    struct test_server *server = clusterGetServer(c, id);
    serverStart(server);
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

    /* If there's more expected output, check that so far we're good, then step
     * and repeat. */
    if (n1 < n2) {
        if (i != n1) {
            goto mismatch;
        }
        c->trace[0] = 0;
        expected += i;
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
