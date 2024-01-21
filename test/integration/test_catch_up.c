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

SUITE(catch_up)

/* Trying to catch-up an unresponsive server eventually fails. */
TEST(catch_up, Unresponsive, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    int status;
    int rv;

    raft_set_max_catch_up_round_duration(CLUSTER_RAFT(1), 90);

    /* Bootstrap a cluster with 1 voter and 1 spare. Server 1 has an
     * additional entry. Only start server 1 */
    CLUSTER_SET_TERM(1 /* ID */, 1 /* term */);

    CLUSTER_FILL_CONFIGURATION(&configuration, 2, 1, 0 /* stand-by */);
    entry.type = RAFT_CHANGE;
    entry.term = 1;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);
    test_cluster_add_entry(&f->cluster_, 1 /* ID */, &entry);
    raft_free(entry.buf.base);

    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_COMMAND, 1 /* term */, 0 /* payload */);

    CLUSTER_START(1 /* ID */);

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "           self elect and convert to leader\n");

    /* Start to catch-up server 2. */
    test_cluster_catch_up(&f->cluster_, 1 /* ID */, 2 /* Catch-up ID */);
    CLUSTER_TRACE(
        "[   0] 1 > catch-up server 2\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    /* Server is now catching up server 2. */
    rv = raft_catch_up(CLUSTER_RAFT(1), 2 /* ID */, &status);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(status, ==, RAFT_CATCH_UP_RUNNING);

    CLUSTER_TRACE(
        "[  50] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 100] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           server 2 is unresponsive -> abort catch-up\n");

    rv = raft_catch_up(CLUSTER_RAFT(1), 2 /* ID */, &status);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(status, ==, RAFT_CATCH_UP_ABORTED);

    return MUNIT_OK;
}
