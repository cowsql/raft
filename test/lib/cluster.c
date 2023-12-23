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

/* Initialize a new server object. */
static void serverInit(struct test_server *s,
                       raft_id id,
                       struct test_cluster *cluster)
{
    char address[64];
    int rv;

    diskInit(&s->disk);

    sprintf(address, "%llu", id);

    rv = raft_init(&s->raft, NULL, NULL, id, address);
    munit_assert_int(rv, ==, 0);

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

void test_cluster_tear_down(struct test_cluster *c)
{
    unsigned i;

    for (i = 0; i < TEST_CLUSTER_N_SERVERS; i++) {
        serverClose(&c->servers[i]);
    }
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
