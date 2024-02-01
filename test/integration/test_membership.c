#include "../../src/configuration.h"
#include "../../src/progress.h"
#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

/* Set up a cluster of 2 servers, with the first as leader. */
static void *setup(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER();
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER();
    free(f);
}

/* Assert the values of the committed and uncommitted configuration indexes on
 * the raft instance with the given index. */
#define ASSERT_CONFIGURATION_INDEXES(I, COMMITTED, UNCOMMITTED)         \
    {                                                                   \
        struct raft *raft_ = CLUSTER_RAFT(I);                           \
        munit_assert_ullong(raft_->configuration_committed_index, ==,   \
                            COMMITTED);                                 \
        munit_assert_ullong(raft_->configuration_uncommitted_index, ==, \
                            UNCOMMITTED);                               \
    }

/******************************************************************************
 *
 * raft_add
 *
 *****************************************************************************/

SUITE(raft_add)

/* After a request to add a new non-voting server is committed, the new
 * configuration is not marked as uncommitted anymore */
TEST(raft_add, Committed, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(1);
    const struct raft_server *server;
    unsigned id;

    /* Start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }
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

    CLUSTER_SUBMIT(1 /* ID */, CHANGE, 3 /* n */, 2 /* V */, 0 /* S */);
    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new configuration entry (2^2)\n");

    /* The new configuration is already effective. */
    munit_assert_uint(raft->configuration.n, ==, 3);
    server = &raft->configuration.servers[2];
    munit_assert_ullong(server->id, ==, 3);
    munit_assert_string_equal(server->address, "3");
    munit_assert_int(server->role, ==, RAFT_SPARE);

    /* The new configuration is marked as uncommitted. */
    ASSERT_CONFIGURATION_INDEXES(1, 1 /* committed */, 2 /* uncomitted */);

    /* The next/match indexes now include an entry for the new server. */
    munit_assert_ullong(raft->leader_state.progress[2].next_index, ==, 3);
    munit_assert_ullong(raft->leader_state.progress[2].match_index, ==, 0);

    CLUSTER_TRACE(
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 160] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           commit 1 new entry (2^2)\n");

    test_cluster_step(&f->cluster_);

    /* The new configuration is marked as committed. */
    ASSERT_CONFIGURATION_INDEXES(1, 2 /* committed */, 0 /* uncommitted */);

    return MUNIT_OK;
}

/* Trying to add a server on a node which is not the leader results in an
 * error. */
TEST(raft_add, NotLeader, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    struct raft_event event;
    struct raft_update update;
    unsigned id;
    int rv;

    /* Start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }
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

    CLUSTER_FILL_CONFIGURATION(&configuration, 3, 2 /* V */, 0 /* S */);
    entry.type = RAFT_CHANGE;
    entry.term = 2;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);
    entry.batch = entry.buf.base;

    event.time = f->cluster_.time;
    event.type = RAFT_SUBMIT;
    event.submit.n = 1;
    event.submit.entries = &entry;

    rv = raft_step(CLUSTER_RAFT(2), &event, &update);
    munit_assert_int(rv, ==, RAFT_NOTLEADER);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* Trying to add a server while a configuration change is already in progress
 * results in an error. */
TEST(raft_add, Busy, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    int rv;

    /* Start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }
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

    CLUSTER_SUBMIT(1 /* ID */, CHANGE, 3 /* n */, 2 /* V */, 0 /* S */);

    CLUSTER_FILL_CONFIGURATION(&configuration, 3, 2 /* V */, 1 /* S */);
    entry.type = RAFT_CHANGE;
    entry.term = 2;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);
    entry.batch = entry.buf.base;

    rv = test_cluster_submit(&f->cluster_, 1 /* ID */, &entry);
    munit_assert_int(rv, ==, RAFT_CANTCHANGE);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* Trying to add a server with an ID which is already in use results in an
 * error. */
TEST(raft_add, DuplicateId, setup, tear_down, 0, NULL)
{
    struct raft_configuration configuration;
    int rv;

    CLUSTER_FILL_CONFIGURATION(&configuration, 2, 2 /* V */, 0 /* S */);

    rv = raft_configuration_add(&configuration, 2, "3", RAFT_SPARE);
    munit_assert_int(rv, ==, RAFT_DUPLICATEID);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_remove
 *
 *****************************************************************************/

SUITE(raft_remove)

/* After a request to remove server is committed, the new configuration is not
 * marked as uncommitted anymore */
TEST(raft_remove, Committed, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(1);
    const struct raft_server *server;
    unsigned id;

    /* Start a cluster with 3 voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n");

    CLUSTER_SUBMIT(1 /* ID */, CHANGE, 2 /* n */, 2 /* V */, 0 /* S */);
    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new configuration entry (2^2)\n");

    /* The new configuration is already effective. */
    munit_assert_uint(raft->configuration.n, ==, 2);
    server = &raft->configuration.servers[1];
    munit_assert_ullong(server->id, ==, 2);

    /* The new configuration is marked as uncommitted. */
    ASSERT_CONFIGURATION_INDEXES(1, 1 /* committed */, 2 /* uncomitted */);

    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           unknown server -> ignore\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 160] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           commit 1 new entry (2^2)\n");

    test_cluster_step(&f->cluster_);

    /* The new configuration is marked as committed. */
    ASSERT_CONFIGURATION_INDEXES(1, 2 /* committed */, 0 /* uncommitted */);

    return MUNIT_OK;
}

/* A leader gets a request to remove itself. */
TEST(raft_remove, Self, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    int rv;

    /* Start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }
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

    raft_configuration_init(&configuration);
    rv = raft_configuration_add(&configuration, 2, "2", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);

    entry.type = RAFT_CHANGE;
    entry.term = 2;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);
    entry.batch = entry.buf.base;
    CLUSTER_SUBMIT(1 /* ID */, &entry);

    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new configuration entry (2^2)\n"
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 0 votes out of 1\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 160] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           commit 1 new entry (2^2)\n"
        "           leader removed from config -> step down\n"
        "[ 270] 1 > timeout as follower\n"
        "           server not in current configuration -> stay follower\n"
        "[ 280] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "           self elect and convert to leader\n");

    return MUNIT_OK;
}

/* A leader gets a request to remove itself from a 3-node cluster */
TEST(raft_remove, SelfThreeNodeCluster, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    raft_id leader_id;
    const char *leader_address;
    int rv;

    /* Start a cluster with 3 voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n");

    raft_configuration_init(&configuration);
    rv = raft_configuration_add(&configuration, 2, "2", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);
    rv = raft_configuration_add(&configuration, 3, "3", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);

    entry.type = RAFT_CHANGE;
    entry.term = 2;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);
    entry.batch = entry.buf.base;
    CLUSTER_SUBMIT(1 /* ID */, &entry);

    /* The removed- leader should still replicate entries.
     *
     * Raft dissertation 4.2.2:
     *
     * `First, there will be a period of time (while it is committing Cnew) when
     * a leader can manage a cluster that does not include itself; it replicates
     * log entries but does not count itself in majorities.`
     */
    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new configuration entry (2^2)\n");

    /* Verify node with id 1 is the leader */
    raft_leader(CLUSTER_RAFT(1), &leader_id, &leader_address);
    munit_assert_ulong(leader_id, ==, 1);
    munit_assert_string_equal(leader_address, "1");

    /* The removed leader eventually steps down */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 0 votes out of 2\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           pipeline server 3 sending 1 entry (2^2)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 160] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 160] 3 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 170] 1 > recv append entries result from server 3\n"
        "           commit 1 new entry (2^2)\n"
        "           leader removed from config -> step down\n");

    raft_leader(CLUSTER_RAFT(1), &leader_id, &leader_address);
    munit_assert_ulong(leader_id, ==, 0);
    munit_assert_ptr_null(leader_address);

    return MUNIT_OK;
}

SUITE(raft_assign)

/* Trying to promote a server on a raft instance which is not the leader results
 * in an error. */
TEST(raft_assign, NotLeader, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    struct raft_event event;
    struct raft_update update;
    int rv;

    /* Start the non-voter server of 2-server cluster with a signle voter. */
    CLUSTER_SET_TERM(2 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_CHANGE, 2 /* servers */, 1 /* voters */);
    CLUSTER_START(2 /* ID */);

    CLUSTER_FILL_CONFIGURATION(&configuration, 2, 2 /* V */, 0 /* S */);
    entry.type = RAFT_CHANGE;
    entry.term = 2;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);
    entry.batch = entry.buf.base;

    event.time = f->cluster_.time;
    event.type = RAFT_SUBMIT;
    event.submit.n = 1;
    event.submit.entries = &entry;

    rv = raft_step(CLUSTER_RAFT(2), &event, &update);
    munit_assert_int(rv, ==, RAFT_NOTLEADER);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}
