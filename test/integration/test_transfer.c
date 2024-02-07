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

/******************************************************************************
 *
 * raft_transfer
 *
 *****************************************************************************/

SUITE(raft_transfer)

/* The follower we ask to transfer leadership to is up-to-date. */
TEST(raft_transfer, UpToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters. */
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
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    test_cluster_transfer(&f->cluster_, 1, 2);
    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 2);

    CLUSTER_TRACE(
        "[ 140] 1 > transfer leadership to 2\n"
        "           send timeout to 2\n"
        "[ 150] 2 > recv timeout now from server 1\n"
        "           convert to candidate, start election for term 3\n"
        "[ 160] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 170] 2 > recv request vote result from server 1\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 1 sending a heartbeat (no entries)\n");

    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 0);

    return MUNIT_OK;
}

/* The follower we ask to transfer leadership to needs to catch up. */
TEST(raft_transfer, CatchUp, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters. Server 1 has an additioanl
     * entry. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        if (id == 1) {
            CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        }
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is longer (2^1 vs 1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^2)\n"
        "           probe server 2 sending 1 entry (3^2)\n");

    /* Fire a transfer event. Since server 2 is not up-to-date, server 1 will
     * not send it a TimeoutNow message immediately. */
    test_cluster_transfer(&f->cluster_, 1, 2);

    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 2);

    CLUSTER_TRACE(
        "[ 120] 1 > transfer leadership to 2\n"
        "           wait for transferee to catch up\n"
        "[ 130] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^1) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           probe server 2 sending 2 entries (2^1..3^2)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 2 new entries (2^1..3^2)\n"
        "[ 160] 2 > persisted 2 entry (2^1..3^2)\n"
        "           send success result to 1\n");

    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 2);

    /* Server 2 is now up-to-date, so server 1 sends it a timeout. */
    CLUSTER_TRACE(
        "[ 170] 1 > recv append entries result from server 2\n"
        "           send timeout to 2\n"
        "           commit 2 new entries (2^1..3^2)\n"
        "[ 180] 2 > recv timeout now from server 1\n"
        "           convert to candidate, start election for term 3\n"
        "[ 190] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log is equal (3^2) -> grant vote\n"
        "[ 200] 2 > recv request vote result from server 1\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (4^3)\n"
        "           probe server 1 sending 1 entry (4^3)\n");

    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 0);

    return MUNIT_OK;
}

/* The follower we ask to transfer leadership to is down and the leadership
 * transfer does not succeed. */
TEST(raft_transfer, Expire, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 3 voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader. */
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
        "           local server is leader -> ignore\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "[ 140] 1 > recv append entries result from server 3\n");

    /* Stop server 2 and try to transfer leadership to it. */
    CLUSTER_STOP(2 /* ID */);
    test_cluster_transfer(&f->cluster_, 1, 2);

    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 2);

    /* Eventually server 1 stops trying to transfer its leadership. */
    CLUSTER_TRACE(
        "[ 140] 1 > transfer leadership to 2\n"
        "           send timeout to 2\n"
        "[ 170] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n"
        "[ 180] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 190] 1 > recv append entries result from server 3\n"
        "[ 220] 1 > timeout as leader\n"
        "           server 2 is unreachable -> abort pipeline\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n"
        "[ 230] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 240] 1 > recv append entries result from server 3\n"
        "[ 270] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n"
        "           server 2 not replicating fast enough -> abort transfer\n");

    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 0);

    return MUNIT_OK;
}

/* The given ID doesn't match any server in the current configuration. */
TEST(raft_transfer, UnknownServer, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_event event;
    struct raft_update update;
    int rv;

    /* Start a single-node cluster. */
    CLUSTER_SET_TERM(1 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_CHANGE, 1 /* servers */, 1 /* voters */);
    CLUSTER_START(1 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "           self elect and convert to leader\n");

    event.time = f->cluster_.time;
    event.type = RAFT_TRANSFER;
    event.transfer.server_id = 2;

    rv = raft_step(CLUSTER_RAFT(1), &event, &update);
    munit_assert_int(rv, ==, RAFT_BADID);

    return MUNIT_OK;
}

/* Submitting a transfer request twice is an error. */
TEST(raft_transfer, Twice, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_event event;
    struct raft_update update;
    unsigned id;
    int rv;

    /* Bootstrap and start a cluster with 2 voters. */
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

    test_cluster_transfer(&f->cluster_, 1, 2);

    event.time = f->cluster_.time;
    event.type = RAFT_TRANSFER;
    event.transfer.server_id = 2;

    rv = raft_step(CLUSTER_RAFT(1), &event, &update);
    munit_assert_int(rv, ==, RAFT_NOTLEADER);

    return MUNIT_OK;
}

/* If the given ID is zero, the target is selected automatically. */
TEST(raft_transfer, AutoSelect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 3 voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader. */
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
        "           local server is leader -> ignore\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    test_cluster_transfer(&f->cluster_, 1, 0);

    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 2);

    return MUNIT_OK;
}

/* If the given ID is zero, the target is selected automatically. Followers that
 * are up-to-date are preferred. */
TEST(raft_transfer, AutoSelectUpToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 3 voters. Server 1 has an additional
     * entry. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        if (id == 1) {
            CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        }
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader and starts replicating the additional entry. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is longer (2^1 vs 1^1) -> grant vote\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is longer (2^1 vs 1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^2)\n"
        "           probe server 2 sending 1 entry (3^2)\n"
        "           probe server 3 sending 1 entry (3^2)\n");

    /* Stop server 2, so it won't get the extra entry, while server 3 does. */
    CLUSTER_STOP(2 /* ID */);
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n"
        "[ 130] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^1) has 1 vote out of 3\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           log mismatch -> send old entries\n"
        "           probe server 3 sending 2 entries (2^1..3^2)\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           start persisting 2 new entries (2^1..3^2)\n"
        "[ 160] 3 > persisted 2 entry (2^1..3^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 3\n"
        "           commit 2 new entries (2^1..3^2)\n");

    /* The auto-selection logic prefers server 3, since it's more up-to-date. */
    test_cluster_transfer(&f->cluster_, 1, 0);

    munit_assert_ullong(raft_transferee(CLUSTER_RAFT(1)), ==, 3);

    return MUNIT_OK;
}

/* It's possible to transfer leadership also when pre-vote is active */
TEST(raft_transfer, PreVote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters, enabling pre-vote.*/
    for (id = 1; id <= 2; id++) {
        raft_set_pre_vote(CLUSTER_RAFT(id), true);
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start pre-election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote log is equal (1^1) -> pre-vote ok\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> pre-vote successful\n"
        "[ 130] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 140] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 160] 1 > recv append entries result from server 2\n");

    /* Perform a successful leadership transfer. */
    test_cluster_transfer(&f->cluster_, 1, 2);

    CLUSTER_TRACE(
        "[ 160] 1 > transfer leadership to 2\n"
        "           send timeout to 2\n"
        "[ 170] 2 > recv timeout now from server 1\n"
        "           convert to candidate, start election for term 3\n"
        "[ 180] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 190] 2 > recv request vote result from server 1\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 1 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}
