#include "../lib/cluster.h"
#include "../lib/runner.h"

struct fixture
{
    FIXTURE_CLUSTER;
};

SUITE(legacy)

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(3);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START();
    CLUSTER_ELECT(0);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* Set the snapshot threshold on all servers of the cluster */
#define SET_SNAPSHOT_THRESHOLD(VALUE)                            \
    {                                                            \
        unsigned i;                                              \
        for (i = 0; i < CLUSTER_N; i++) {                        \
            raft_set_snapshot_threshold(CLUSTER_RAFT(i), VALUE); \
        }                                                        \
    }

/* Set the snapshot trailing logs number on all servers of the cluster */
#define SET_SNAPSHOT_TRAILING(VALUE)                            \
    {                                                           \
        unsigned i;                                             \
        for (i = 0; i < CLUSTER_N; i++) {                       \
            raft_set_snapshot_trailing(CLUSTER_RAFT(i), VALUE); \
        }                                                       \
    }

static int ioMethodSnapshotPutFail(struct raft_io *raft_io,
                                   unsigned trailing,
                                   struct raft_io_snapshot_put *req,
                                   const struct raft_snapshot *snapshot,
                                   raft_io_snapshot_put_cb cb)
{
    (void)raft_io;
    (void)trailing;
    (void)req;
    (void)snapshot;
    (void)cb;
    return -1;
}

#define SET_FAULTY_SNAPSHOT_PUT()                                        \
    {                                                                    \
        unsigned i;                                                      \
        for (i = 0; i < CLUSTER_N; i++) {                                \
            CLUSTER_RAFT(i)->io->snapshot_put = ioMethodSnapshotPutFail; \
        }                                                                \
    }

static char *fsm_version[] = {"1", "2", NULL};
static MunitParameterEnum fsm_snapshot_async_params[] = {
    {CLUSTER_FSM_VERSION_PARAM, fsm_version},
    {NULL, NULL},
};

TEST(legacy,
     takeSnapshotSnapshotPutFail,
     setUp,
     tearDown,
     0,
     fsm_snapshot_async_params)
{
    struct fixture *f = data;
    (void)params;

    SET_FAULTY_SNAPSHOT_PUT();

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* No crash or leaks have occurred */
    return MUNIT_OK;
}

/* A follower doesn't convert to candidate state while it's installing a
 * snapshot. */
TEST(legacy, snapshotBlocksCandidate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);

    /* Apply a few of entries, to force a snapshot to be taken. Drop all
     * network traffic between servers 0 and 2 in order for AppendEntries
     * RPCs to not be replicated */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect both servers and set a high disk latency on server 2 */
    CLUSTER_SET_DISK_LATENCY(2, 5000);
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);

    /* Wait a while and check that the leader has sent a snapshot */
    CLUSTER_STEP_UNTIL_ELAPSED(500);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);

    /* Disconnect the servers again so that heartbeats, etc. won't arrive */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_FOLLOWER);
    munit_assert_true(CLUSTER_RAFT(2)->snapshot.persisting);
    CLUSTER_STEP_UNTIL_ELAPSED(4000);
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_FOLLOWER);
    return MUNIT_OK;
}
