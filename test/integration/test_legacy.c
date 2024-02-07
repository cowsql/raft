#include "../lib/legacy.h"
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
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_INSTALL_SNAPSHOT), ==, 1);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_INSTALL_SNAPSHOT), ==, 1);

    /* Disconnect the servers again so that heartbeats, etc. won't arrive */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_FOLLOWER);
    munit_assert_true(CLUSTER_RAFT(2)->snapshot.installing);
    CLUSTER_STEP_UNTIL_ELAPSED(4000);
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_FOLLOWER);
    return MUNIT_OK;
}

static void *setUpReplication(const MunitParameter params[],
                              MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(2);
    return f;
}

static void tearDownReplication(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

SUITE(replication)

/* If any of the new entry has the same index of an existing entry in our log,
 * but different term, and that entry index is already committed, we bail out
 * with an error. */
TEST(replication,
     recvPrevIndexConflict,
     setUpReplication,
     tearDownReplication,
     0,
     NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    CLUSTER_BOOTSTRAP;

    /* The servers have an entry with a conflicting term. */
    entry1.type = RAFT_COMMAND;
    entry1.term = 2;
    FsmEncodeSetX(1, &entry1.buf);
    CLUSTER_ADD_ENTRY(0, &entry1);

    entry2.type = RAFT_COMMAND;
    entry2.term = 1;
    FsmEncodeSetX(2, &entry2.buf);
    CLUSTER_ADD_ENTRY(1, &entry2);

    CLUSTER_START();
    CLUSTER_ELECT(0);

    /* Artificially bump the commit index on the second server */
    CLUSTER_RAFT(1)->commit_index = 2;
    CLUSTER_STEP;
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* Assert that the fixture time matches the given value */
#define ASSERT_TIME(TIME) munit_assert_int(CLUSTER_TIME, ==, TIME)

/* Standard startup sequence, bootstrapping the cluster and electing server 0 */
#define BOOTSTRAP_START_AND_ELECT \
    CLUSTER_BOOTSTRAP;            \
    CLUSTER_START();              \
    CLUSTER_ELECT(0);             \
    ASSERT_TIME(1045)

/* A leader with slow disk commits an entry that it hasn't persisted yet,
 * because enough followers to have a majority have aknowledged that they have
 * appended the entry. The leader's last_stored field hence lags behind its
 * commit_index. A new leader gets elected, with a higher commit index and sends
 * first a new entry than a heartbeat to the old leader, that needs to update
 * its commit_index taking into account its lagging last_stored.
 *
 * XXX: this test duplicates the one above, but it's kept because the change it
 * is associated with was fixing an assertion in the legacy compat layer. */
TEST(replication,
     lastStoredLaggingBehindCommitIndex,
     setUpReplication,
     tearDownReplication,
     0,
     NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;

    /* Server 0 takes a long time to persist entry 2 (the barrier) */
    CLUSTER_SET_DISK_LATENCY(0, 10000);

    /* Server 0 gets elected. */
    BOOTSTRAP_START_AND_ELECT;

    /* Create an entry at index 2. Server 0 commits and applies it even if
     * it not persist it yet. */
    CLUSTER_MAKE_PROGRESS;

    munit_assert_int(CLUSTER_RAFT(0)->last_stored, ==, 1);
    munit_assert_int(CLUSTER_RAFT(0)->commit_index, ==, 2);
    munit_assert_int(CLUSTER_RAFT(0)->last_applied, ==, 2);

    /* Server 1 stored barrier entry 2, but did not yet receive a
     * notification from server 0 about the new commit index. */
    munit_assert_int(CLUSTER_RAFT(1)->last_stored, ==, 2);
    munit_assert_int(CLUSTER_RAFT(1)->commit_index, ==, 1);
    munit_assert_int(CLUSTER_RAFT(1)->last_applied, ==, 1);

    /* Disconnect server 0 from server 1 and 2. */
    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_DISCONNECT(0, 2);

    /* Set a very high election timeout on server 0, so it won't step down
     * for a while, even if disconnected. */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 10000);
    raft_set_election_timeout(CLUSTER_RAFT(0), 10000);

    /* Server 1 and 2 eventually timeout and start an election, server 1
     * wins. */
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(4000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(2000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);

    /* Server 1 commits the barrier entry at index 3 that it created at the
     * start of its term. */
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 2000);

    /* Reconnect server 0 to server 1, which will start replicating entry 3
     * to it. */
    CLUSTER_RECONNECT(0, 1);
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 20000);

    return MUNIT_OK;
}

/* A leader with faulty disk fails to persist the barrier entry upon election.
 */
TEST(replication,
     failPersistBarrier,
     setUpReplication,
     tearDownReplication,
     0,
     NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;

    /* Server 0 will fail to persist entry 2, a barrier */
    CLUSTER_IO_FAULT(0, 10, 1);

    /* Server 0 gets elected and creates a barrier entry at index 2 */
    CLUSTER_BOOTSTRAP;
    CLUSTER_START();
    CLUSTER_START_ELECT(0);

    /* Cluster recovers. */
    CLUSTER_STEP_UNTIL_HAS_LEADER(20000);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_assign
 *
 *****************************************************************************/

static void *setUpAssign(const MunitParameter params[],
                         MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START();
    CLUSTER_ELECT(0);
    return f;
}

static void tearDownAssign(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

struct result
{
    int status;
    bool done;
};

static void changeCbAssertResult(struct raft_change *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

static bool changeCbHasFired(struct raft_fixture *f, void *arg)
{
    struct result *result = arg;
    (void)f;
    return result->done;
}

/* Invoke raft_assign() against the I'th server and assert it the given error
 * code. */
#define ASSIGN_ERROR(I, ID, ROLE, RV, ERRMSG)                        \
    {                                                                \
        struct raft_change __req;                                    \
        int __rv;                                                    \
        __rv = raft_assign(CLUSTER_RAFT(I), &__req, ID, ROLE, NULL); \
        munit_assert_int(__rv, ==, RV);                              \
        munit_assert_string_equal(ERRMSG, CLUSTER_ERRMSG(I));        \
    }

/* Add a an empty server to the cluster and start it. */
#define GROW                                \
    {                                       \
        int rv__;                           \
        CLUSTER_GROW;                       \
        rv__ = raft_start(CLUSTER_RAFT(2)); \
        munit_assert_int(rv__, ==, 0);      \
    }

/* Submit an add request. */
#define ADD_SUBMIT(I, ID)                                                     \
    struct raft_change _req;                                                  \
    char _address[16];                                                        \
    struct result _result = {0, false};                                       \
    int _rv;                                                                  \
    _req.data = &_result;                                                     \
    sprintf(_address, "%d", ID);                                              \
    _rv =                                                                     \
        raft_add(CLUSTER_RAFT(I), &_req, ID, _address, changeCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

#define ADD(I, ID)                                            \
    do {                                                      \
        ADD_SUBMIT(I, ID);                                    \
        CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 2000); \
    } while (0)

/* Submit an assign role request. */
#define ASSIGN_SUBMIT(I, ID, ROLE)                                             \
    struct raft_change _req;                                                   \
    struct result _result = {0, false};                                        \
    int _rv;                                                                   \
    _req.data = &_result;                                                      \
    _rv = raft_assign(CLUSTER_RAFT(I), &_req, ID, ROLE, changeCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

/* Expect the request callback to fire with the given status. */
#define ASSIGN_EXPECT(STATUS) _result.status = STATUS;

/* Wait until a promote request completes. */
#define ASSIGN_WAIT CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 10000)

SUITE(raft_assign)

/* Trying to change the role of a server whose ID is unknown results in an
 * error. */
TEST(raft_assign, unknownId, setUpAssign, tearDownAssign, 0, NULL)
{
    struct fixture *f = data;
    ASSIGN_ERROR(0, 3, RAFT_VOTER, RAFT_NOTFOUND, "no server has ID 3");
    return MUNIT_OK;
}

/* Trying to promote a server to an unknown role in an. */
TEST(raft_assign, badRole, setUpAssign, tearDownAssign, 0, NULL)
{
    struct fixture *f = data;
    ASSIGN_ERROR(0, 3, 999, RAFT_BADROLE, "server role is not valid");
    return MUNIT_OK;
}

/* Trying to assign the voter role to a server which has already it results in
 * an error. */
TEST(raft_assign, alreadyHasRole, setUpAssign, tearDownAssign, 0, NULL)
{
    struct fixture *f = data;
    ASSIGN_ERROR(0, 1, RAFT_VOTER, RAFT_BADROLE, "server is already voter");
    return MUNIT_OK;
}

/* Trying to assign a new role to a server while a configuration change is in
 * progress results in an error. */
TEST(raft_assign, alreadyInProgressAssign, setUpAssign, tearDown, 0, NULL)
{
    struct fixture *f = data;
    GROW;
    ADD(0, 3);
    ASSIGN_SUBMIT(0, 3, RAFT_VOTER);
    ASSIGN_ERROR(0, 3, RAFT_VOTER, RAFT_CANTCHANGE,
                 "a configuration change is already in progress");
    ASSIGN_WAIT;
    return MUNIT_OK;
}

SUITE(raft_init)

TEST(raft_init, ioVersionNotSet, NULL, NULL, 0, NULL)
{
    struct raft r = {0};
    struct raft_io io = {0};
    struct raft_fsm fsm = {0};
    io.version = 0;
    fsm.version = 3;

    int rc;
    rc = raft_init(&r, &io, &fsm, 1, "1");
    munit_assert_int(rc, ==, -1);
    munit_assert_string_equal(r.errmsg, "io->version must be set");
    return MUNIT_OK;
}

TEST(raft_init, fsmVersionNotSet, NULL, NULL, 0, NULL)
{
    struct raft r = {0};
    struct raft_io io = {0};
    struct raft_fsm fsm = {0};
    io.version = 2;
    fsm.version = 0;

    int rc;
    rc = raft_init(&r, &io, &fsm, 1, "1");
    munit_assert_int(rc, ==, -1);
    munit_assert_string_equal(r.errmsg, "fsm->version must be set");
    return MUNIT_OK;
}
