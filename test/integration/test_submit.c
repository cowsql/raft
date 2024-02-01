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

SUITE(submit)

/* If there isn't a majority of voting servers with enough capacity, an error is
 * returned. */
TEST(submit, CapacityBelowThreshold, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;
    struct raft_entry entry;
    char buf[8];
    int rv;

    /* Set a capacity threshold close to the disk capacity. */
    raft_set_capacity_threshold(CLUSTER_RAFT(1), 240);

    /* Bootstrap and start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    raft_set_capacity_threshold(CLUSTER_RAFT(1), 240);

    /* Submitting an entry fails because there's not enough capacity. */
    entry.type = RAFT_COMMAND;
    entry.term = raft_current_term(CLUSTER_RAFT(1));
    entry.buf.len = 8;
    entry.buf.base = buf;
    munit_assert_not_null(entry.buf.base);
    entry.batch = entry.buf.base;

    rv = test_cluster_submit(&f->cluster_, 1 /* ID */, &entry);
    munit_assert_int(rv, ==, RAFT_NOSPACE);

    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    /* Trying to submit again after the first round of heartbeat still fails,
     * because the follower is still reporting the same capacity in the
     * AppendEntries result. */
    rv = test_cluster_submit(&f->cluster_, 1 /* ID */, &entry);
    munit_assert_int(rv, ==, RAFT_NOSPACE);

    return MUNIT_OK;
}
