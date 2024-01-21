#include "../lib/cluster.h"
#include "../lib/runner.h"

struct fixture
{
    FIXTURE_CLUSTER;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER();
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER();
    free(f);
}

SUITE(start)

/* Start a server that has no persisted state whatsoever. */
TEST(start, NoState, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_START(1);
    CLUSTER_TRACE("[   0] 1 > no state\n");
    munit_assert_ullong(raft_timeout(CLUSTER_RAFT(1)), ==, 100);
    return MUNIT_OK;
}

/* Start a server that has a persisted its term. */
TEST(start, PersistedTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_SET_TERM(1 /* ID */, 1 /* term */);
    CLUSTER_START(1 /* ID */);
    CLUSTER_TRACE("[   0] 1 > term 1\n");
    return MUNIT_OK;
}

/* Start a server that has a persisted its term and has a snapshot. */
TEST(start, PersistedTermAndSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_SET_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_SET_SNAPSHOT(1, /* ID                                        */
                         6, /* last index                                */
                         2, /* last term                                 */
                         2, /* N servers                                 */
                         2, /* N voting                                  */
                         1 /* conf index                                 */);
    CLUSTER_START(1 /* ID */);
    CLUSTER_TRACE("[   0] 1 > term 2, 1 snapshot (6^2)\n");
    return MUNIT_OK;
}

/* Start a server that has a persisted its term and has the initial bootstrap
 * log entry. */
TEST(start, PersistedTermAndEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_SET_TERM(1 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_START(1 /* ID */);
    CLUSTER_TRACE("[   0] 1 > term 1, 1 entry (1^1)\n");
    return MUNIT_OK;
}

/* There are two servers. The first has a snapshot present and no other
 * entries. */
TEST(start, OneSnapshotAndNoEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    CLUSTER_SET_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_SET_SNAPSHOT(1, /* ID                                        */
                         6, /* last index                                */
                         2, /* last term                                 */
                         2, /* N servers                                 */
                         2, /* N voting                                  */
                         1 /* conf index                                 */);

    CLUSTER_SET_TERM(2 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 2, 1 snapshot (6^2)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log is more recent (6^2 vs 1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    /* It eventually replicates the snapshot. */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           missing previous entry (6^2) -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (6^2) to server 2\n"
        "[ 150] 2 > recv install snapshot from server 1\n"
        "           start persisting snapshot (6^2)\n"
        "[ 160] 2 > persisted snapshot (6^2)\n"
        "           send success result to 1\n");

    /* When the server 1 receives the result it immediately transition server 2
     * to pipeline mode. */
    CLUSTER_TRACE(
        "[ 170] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}

/* There are two servers. The first has a snapshot along with some follow-up
 * entries. */
TEST(start, OneSnapshotAndSomeFollowUpEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    CLUSTER_SET_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_SET_SNAPSHOT(1, /* ID                                        */
                         6, /* last index                                */
                         2, /* last term                                 */
                         2, /* N servers                                 */
                         2, /* N voting                                  */
                         1 /* conf index                                 */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_COMMAND, 1 /* term */, 0 /* payload */);

    CLUSTER_SET_TERM(2 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 2, 1 snapshot (6^2), 2 entries (7^1..8^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log is longer (8^1 vs 1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (9^3)\n"
        "           probe server 2 sending 1 entry (9^3)\n"
        "[ 130] 1 > persisted 1 entry (9^3)\n"
        "           next uncommitted entry (8^1) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           missing previous entry (8^1) -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (6^2) to server 2\n");

    test_cluster_step(&f->cluster_);
    test_cluster_step(&f->cluster_);
    test_cluster_step(&f->cluster_);
    test_cluster_step(&f->cluster_);

    return MUNIT_OK;
}

/* There is a single voting server in the cluster, which immediately elects
 * itself when starting. */
TEST(start, SingleVotingSelfElect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    CLUSTER_SET_TERM(1 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_CHANGE, 1 /* servers */, 1 /* voters */);
    CLUSTER_START(1 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "           self elect and convert to leader\n");

    munit_assert_int(raft_state(CLUSTER_RAFT(1)), ==, RAFT_LEADER);

    /* The server can make progress alone. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[   0] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^1)\n"
        "[  10] 1 > persisted 1 entry (2^1)\n"
        "           commit 1 new entry (2^1)\n");

    return MUNIT_OK;
}

/* There are two servers in the cluster, one is voting and the other is
 * not. When started, the non-voting server does not elects itself. */
TEST(start, SingleVotingNotUs, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    CLUSTER_SET_TERM(2 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_CHANGE, 2 /* servers */, 1 /* voters */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE("[   0] 2 > term 1, 1 entry (1^1)\n");

    munit_assert_int(raft_state(CLUSTER_RAFT(2)), ==, RAFT_FOLLOWER);

    return MUNIT_OK;
}
