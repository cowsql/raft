#include "../../src/configuration.h"
#include "../../src/flags.h"
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

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Standard startup sequence, bootstrapping the cluster and electing server 0 */
#define BOOTSTRAP_START_AND_ELECT \
    CLUSTER_BOOTSTRAP;            \
    CLUSTER_START();              \
    CLUSTER_ELECT(0);             \
    ASSERT_TIME(1045)

/******************************************************************************
 *
 * Set up a cluster with a two servers.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(2);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the fixture time matches the given value */
#define ASSERT_TIME(TIME) munit_assert_int(CLUSTER_TIME, ==, TIME)

/******************************************************************************
 *
 * Log replication.
 *
 *****************************************************************************/

SUITE(replication)

/* A leader doesn't send an initial no-op barrier entry if its committed index
 * is as big as its last log index. */
TEST_V1(replication, NoInitialBarrier, setUp, tearDown, 0, NULL)
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
        "[   0] 2 > term 1, 1 entry (1^1)\n");

    /* Server 1 becomes candidate and sends a vote request to server 2. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    /* Server 1 receives the vote result and becomes leader. It does not append
     * any barrier entry. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}

/* A leader sends an initial no-op barrier entry if its committed index
 * is behind its last log index. */
TEST_V1(replication, InitialBarrier, setUp, tearDown, 0, NULL)
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

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n");

    /* Server 1 becomes candidate and sends a vote request to server 2. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is longer (2^1 vs 1^1) -> grant vote\n");

    /* Server 1 receives the vote result and becomes leader. It appends
     * a barrier in order to commit all entries from previous terms. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^2)\n"
        "           probe server 2 sending 1 entry (3^2)\n");

    return MUNIT_OK;
}

/* After receiving an AppendEntriesResult, a leader has set the feature flags of
 * a node. */
TEST_V1(replication, FeatureFlags, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;
    struct raft *raft;

    /* Bootstrap and start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader and sends the initial heartbeat. */
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

    /* Flags is empty */
    raft = CLUSTER_RAFT(1);
    munit_assert_ullong(raft->leader_state.progress[1].features, ==, 0);

    /* Server 2 receives the heartbeat and replies. When server 1 receives the
     * response, the feature flags are set. */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    munit_assert_ullong(raft->leader_state.progress[1].features, ==,
                        RAFT_DEFAULT_FEATURE_FLAGS);

    return MUNIT_OK;
}

/* A leader keeps sending heartbeat messages at regular intervals to
 * maintain leadership. */
TEST_V1(replication, Heartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

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

    /* Server 2 receives the first the heartbeat. */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    /* Server 2 receives a second heartbeat. */
    CLUSTER_TRACE(
        "[ 140] 1 > recv append entries result from server 2\n"
        "[ 170] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "[ 180] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    return MUNIT_OK;
}

/* If a leader replicates some entries during a given heartbeat interval, it
 * skips sending the heartbeat for that interval. */
TEST_V1(replication, SkipHeartbeatIfEntriesHaveSent, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;
    struct raft *raft;
    struct raft_entry entry;

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
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    raft = CLUSTER_RAFT(1);
    munit_assert_ullong(raft->leader_state.progress[1].last_send, ==, 120);

    /* Server 1 starts replicating a new entry after 5 milliseconds. The
     * heartbeat timeout gets postponed. */
    CLUSTER_ELAPSE(5);

    munit_assert_ullong(raft_timeout(CLUSTER_RAFT(1)), ==, 170);

    entry.term = 2;
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    CLUSTER_TRACE(
        "[ 145] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "           pipeline server 2 sending 1 entry (2^2)\n");

    munit_assert_ullong(raft->leader_state.progress[1].last_send, ==, 145);
    munit_assert_ullong(raft_timeout(CLUSTER_RAFT(1)), ==, 195);

    CLUSTER_TRACE(
        "[ 155] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 155] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 165] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 175] 1 > recv append entries result from server 2\n"
        "           commit 1 new entry (2^2)\n");

    /* When the heartbeat timeout expires again, server 1 sends a fresh
     * heartbeat round.
     *
     * XXX: should we immediately send a heartbeat after the commit index
     *      changes? In order to notify followers. */
    CLUSTER_TRACE(
        "[ 195] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n");

    munit_assert_ullong(raft->leader_state.progress[1].last_send, ==, 195);
    munit_assert_ullong(raft_timeout(CLUSTER_RAFT(1)), ==, 245);

    return MUNIT_OK;
}

/* The leader doesn't send replication messages to idle servers. */
TEST_V1(replication, SkipSpare, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    unsigned id;

    /* Bootstrap and start a cluster with one voter and one spare. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 1 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 self-elects, but it does not replicate any entry or send any
     * heartbeat to server 2. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "           self elect and convert to leader\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as leader\n");

    entry.term = 1;
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    CLUSTER_TRACE(
        "[ 100] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^1)\n"
        "[ 110] 1 > persisted 1 entry (2^1)\n"
        "           commit 1 new entry (2^1)\n");

    munit_assert_ullong(raft_commit_index(CLUSTER_RAFT(1)), ==, 2);
    munit_assert_ullong(raft_commit_index(CLUSTER_RAFT(2)), ==, 1);

    return MUNIT_OK;
}

/* A follower remains in probe mode until the leader receives a successful
 * AppendEntries response. */
TEST_V1(replication, Probe, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    unsigned id;

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

    /* Set a network latency higher than the heartbeat timeout for server 2, so
     * server 1 will send a second probe AppendEntries without transitioning to
     * pipeline mode. */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 100 /* msecs */);

    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    test_cluster_step(&f->cluster_);

    /* Server 1 receives a new entry after a few milliseconds. Since the
     * follower is still in probe mode and since an AppendEntries message was
     * already sent recently, it does not send the new entry immediately. */
    CLUSTER_ELAPSE(5);
    entry.term = 2;
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);

    test_cluster_submit(&f->cluster_, 1, &entry);

    CLUSTER_TRACE(
        "[ 135] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n");

    /* A heartbeat timeout elapses without receiving a response, so server 1
     * sends an new AppendEntries to server 2. This time it includes also the
     * new entry that was accepted in the meantime. */
    CLUSTER_TRACE(
        "[ 145] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 170] 1 > timeout as leader\n"
        "           probe server 2 sending 1 entry (2^2)\n");

    /* Now lower the network latency of server 2, so the AppendEntries result
     * for this last AppendEntries request will get delivered before the
     * response for the original heartbeat AppendEntries request . */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 10 /* msecs */);

    CLUSTER_TRACE(
        "[ 180] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 190] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n");

    /* Server 1 receives a second entry. Since the follower is still in probe
     * mode and since an AppendEntries message was already sent recently, it
     * does not send the new entry immediately. */
    test_cluster_step(&f->cluster_);
    CLUSTER_ELAPSE(5);
    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    CLUSTER_TRACE(
        "[ 195] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (3^2)\n");

    /* Eventually server 1 receives the AppendEntries result for the second
     * request, at that point it transitions to pipeline mode and sends
     * the second entry immediately. */
    CLUSTER_TRACE(
        "[ 200] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (3^2)\n"
        "           commit 1 new entry (2^2)\n");

    return MUNIT_OK;
}

/* A follower transitions to pipeline mode after the leader receives a
 * successful AppendEntries response from it. */
TEST_V1(replication, Pipeline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft *raft;
    unsigned id;

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
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    /* Server 1 receives a new entry after 5 milliseconds, just before the
     * heartbeat timeout expires. Since the follower has transitioned to
     * pipeline mode the new entry is sent immediately and the next index is
     * optimistically increased. */
    CLUSTER_ELAPSE(5);
    entry.term = 2;
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    CLUSTER_TRACE(
        "[ 145] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "           pipeline server 2 sending 1 entry (2^2)\n");

    raft = CLUSTER_RAFT(1);
    munit_assert_ullong(raft->leader_state.progress[1].next_index, ==, 3);

    /* After another 15 milliseconds, before receiving the response for this
     * last AppendEntries RPC and before the heartbeat timeout expires, server 1
     * accepts a second entry, which is also replicated immediately. */
    CLUSTER_TRACE(
        "[ 155] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 155] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 165] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n");

    test_cluster_step(&f->cluster_);
    CLUSTER_ELAPSE(5);
    entry.buf.base = raft_malloc(8);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    CLUSTER_TRACE(
        "[ 170] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (3^2)\n"
        "           pipeline server 2 sending 1 entry (3^2)\n");

    munit_assert_ullong(raft->leader_state.progress[1].next_index, ==, 4);

    /* Eventually server 1 receives AppendEntries results for both entries. */
    CLUSTER_TRACE(
        "[ 175] 1 > recv append entries result from server 2\n"
        "           commit 1 new entry (2^2)\n"
        "[ 180] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (3^2) has 1 vote out of 2\n"
        "[ 180] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (3^2)\n"
        "[ 190] 2 > persisted 1 entry (3^2)\n"
        "           send success result to 1\n"
        "[ 200] 1 > recv append entries result from server 2\n"
        "           commit 1 new entry (3^2)\n");

    return MUNIT_OK;
}

/* A follower disconnects while in probe mode. */
TEST_V1(replication, Disconnect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

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

    CLUSTER_DISCONNECT(1, 2);

    /* After the heartbeat timeout server 1 retries, this time it succeeds. */
    CLUSTER_TRACE(
        "[ 170] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    CLUSTER_RECONNECT(1, 2);

    CLUSTER_TRACE(
        "[ 180] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    return MUNIT_OK;
}

/* A follower disconnects while in pipeline mode. */
TEST_V1(replication, DisconnectPipeline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_entry entry;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader and then sends a first round of heartbeats,
     * transitioning server 2 into pipeline mode. */
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

    /* Server 1 starts to replicate a few entries, however server 2 disconnects
     * before it can receive them. */
    CLUSTER_ELAPSE(10);
    entry.term = 2;
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    CLUSTER_TRACE(
        "[ 150] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 150] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (3^2)\n"
        "           pipeline server 2 sending 1 entry (3^2)\n");

    raft = CLUSTER_RAFT(1);
    munit_assert_ullong(raft->leader_state.progress[1].next_index, ==, 4);

    CLUSTER_DISCONNECT(1, 2);

    /* A heartbeat timeout eventually kicks in, but sending the empty
     * AppendEntries message fails, transitioning server 2 back to probe
     * mode. */
    CLUSTER_TRACE(
        "[ 160] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 160] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 200] 1 > timeout as leader\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n");

    munit_assert_ullong(raft->leader_state.progress[1].next_index, ==, 2);

    /* After reconnection the follower eventually replicates the entries and
     * reports back. */
    CLUSTER_RECONNECT(1, 2);

    CLUSTER_TRACE(
        "[ 210] 2 > recv append entries from server 1\n"
        "           start persisting 2 new entries (2^2..3^2)\n"
        "[ 220] 2 > persisted 2 entry (2^2..3^2)\n"
        "           send success result to 1\n");

    return MUNIT_OK;
}

static char *send_oom_heap_fault_delay[] = {"5", NULL};
static char *send_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory failures. */
TEST(replication, sendOom, setUp, tearDown, 0, send_oom_params)
{
    struct fixture *f = data;
    return MUNIT_SKIP;
    struct raft_apply req;
    BOOTSTRAP_START_AND_ELECT;

    HEAP_FAULT_ENABLE;

    CLUSTER_APPLY_ADD_X(0, &req, 1, NULL);
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* A failure occurs upon submitting the I/O request. */
TEST(replication, sendIoError, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    return MUNIT_SKIP;
    struct raft_apply req;
    BOOTSTRAP_START_AND_ELECT;

    CLUSTER_IO_FAULT(0, 1, 1);

    CLUSTER_APPLY_ADD_X(0, &req, 1, NULL);
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* Receive the same entry a second time, before the first has been persisted. */
TEST_V1(replication, ReceiveSameEntryTwice, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters. The both have an additional
     * uncommitted entry at index 2. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        CLUSTER_START(id);
    }

    /* Set a high disk latency for server 2, so persisting the entry at index 2
     * will takes a long time. */
    CLUSTER_SET_DISK_LATENCY(2 /* ID */, 60 /* msecs */);

    /* Server 1 becomes leader and then sends a barrier since it has an
     * uncommitted entry at index 1. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 1, 2 entries (1^1..2^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (2^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^2)\n"
        "           probe server 2 sending 1 entry (3^2)\n");

    /* Server 2 takes a long time to persist the entry, and since replication
     * for server 2 is still in the probe state, server 1 eventually sends again
     * the same entry. Server 2 receives it, but it doesn't persist it again to
     * disk, since the first persist request is still in flight. */
    CLUSTER_TRACE(
        "[ 130] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^1) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (3^2)\n"
        "[ 170] 1 > timeout as leader\n"
        "           probe server 2 sending 1 entry (3^2)\n");

    /* Eventually the original persist entries request from server 2 succeeds,
     * and is reported back to server 1. */
    CLUSTER_TRACE(
        "[ 180] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 190] 2 > persisted 1 entry (3^2)\n"
        "           send success result to 1\n"
        "[ 190] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (3^2)\n"
        "           next uncommitted entry (2^1) has 2 votes out of 2\n"
        "[ 200] 1 > recv append entries result from server 2\n"
        "           commit 2 new entries (2^1..3^2)\n");

    return MUNIT_OK;
}

/* If the term in the request is stale, the server rejects it. */
TEST_V1(replication, AppendEntriesRequestHasStaleTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 3 voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 wins the elections. */
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
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Partition server 1 from the other two and set a very high election
     * timeout on it, so it will keep sending heartbeats. */
    CLUSTER_SET_ELECTION_TIMEOUT(1 /* ID */, 250 /* timeout */, 0 /* delta */);
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);

    /* Server 2 eventually times out and starts an election. */
    CLUSTER_SET_ELECTION_TIMEOUT(2 /* ID */, 30 /* timeout */, 0 /* delta */);
    CLUSTER_TRACE(
        "[ 140] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    /* Reconnect server 1 with server 3, so server 3 will receive the next
     * hearbeat that server 1 sends to it */
    CLUSTER_RECONNECT(1, 3);
    CLUSTER_RECONNECT(3, 1);

    /* Eventually server 2 gets elected and server 1 sends a new heartbeat. */
    CLUSTER_TRACE(
        "[ 150] 3 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 160] 2 > recv request vote result from server 3\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 1 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Server 3 receives the heartbeat from server 1 and rejects it. */
    CLUSTER_TRACE(
        "[ 170] 3 > recv append entries from server 2\n"
        "           no new entries to persist\n"
        "[ 170] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 180] 2 > recv append entries result from server 3\n"
        "[ 180] 3 > recv append entries from server 1\n"
        "           local term is higher (3 vs 2) -> reject\n");

    /* Server 1 receives the reject message and steps down. */
    CLUSTER_TRACE(
        "[ 190] 1 > recv append entries result from server 3\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n");

    return MUNIT_OK;
}

/* If the log of the receiving server is shorter than prevLogIndex, the request
 * is rejected . */
TEST_V1(replication, FollowerHasMissingEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters. Server 1 has an entry that
     * server 2 doesn't have. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        if (id == 1) {
            CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        }
        CLUSTER_START(id);
    }

    /* Server 1 wins the election because it has a longer log. */
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
        "           probe server 2 sending 1 entry (3^2)\n"
        "[ 130] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^1) has 1 vote out of 2\n");

    /* Server 1 replicates a no-op entry to server 2, which initially rejects
     * it, because it's missing the one before. */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n");

    /* Server 1 sends the missing entry. */
    CLUSTER_TRACE(
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           probe server 2 sending 2 entries (2^1..3^2)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 2 new entries (2^1..3^2)\n"
        "[ 160] 2 > persisted 2 entry (2^1..3^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           commit 2 new entries (2^1..3^2)\n");

    return MUNIT_OK;
}

/* If the term of the last log entry on the server is different from the one
 * in prevLogTerm, and value of prevLogIndex is greater than the server's commit
 * index (i.e. this is a normal inconsistency), we reject the request. */
TEST_V1(replication, PrevLogTermMismatch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;
    return 0;

    /* Bootstrap and start a cluster with 2 voters. The two servers have an
     * entry with conflicting terms at index 2. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 3 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    }
    CLUSTER_ADD_ENTRY(1, RAFT_COMMAND, 3 /* term */, 0 /* payload */);
    CLUSTER_ADD_ENTRY(2, RAFT_COMMAND, 2 /* term */, 0 /* payload */);
    CLUSTER_START(1);
    CLUSTER_START(2);

    /* Server 1 becomes leader because its last entry has a higher term. */
    CLUSTER_TRACE(
        "[   0] 1 > term 3, 2 entries (1^1..2^3)\n"
        "[   0] 2 > term 3, 2 entries (1^1..2^2)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 4\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (4 vs 3) -> bump term\n"
        "           local log older (2^2 vs 2^3) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new entry (3^4)\n"
        "           probe server 2 sending 1 entry (3^4)\n");

    /* Server 2 rejects the initial AppendEntries request from server 1. */
    CLUSTER_TRACE(
        "[ 130] 1 > persisted 1 entry (3^4)\n"
        "           next uncommitted entry (3^4) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           previous term mismatch -> reject\n");

    /* Server 1 overwrites server 2's log. */
    CLUSTER_TRACE(
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           probe server 2 sending 2 entries (2.3..3.4)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           log mismatch (2^2 vs 2^3) -> truncate\n"
        "           start persisting 2 new entries (2^3..3^4)\n"
        "[ 160] 2 > persisted 2 entry (2^3..3^4)\n"
        "           send success result to 1\n"
        "[ 160] 1 > timeout as leader\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           commit 2 new entries (2^3..3^4)\n");

    return MUNIT_OK;
}

/* The follower has an uncommitted log entry that conflicts with a new one sent
 * by the leader (same index but different term). The follower's conflicting log
 * entry happens to be a configuration change. In that case the follower
 * discards the conflicting entry from its log and rolls back its configuration
 * to the initial one contained in the log entry at index 1. */
TEST_V1(replication, RollbackConfigurationToInitial, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;
    struct raft_configuration conf; /* Uncommitted configuration at index 2 */
    struct raft_entry entry;
    struct raft *raft;
    int rv;

    /* Bootstrap and start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 2 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    }

    /* Both servers have an entry at index 2, but with conflicting terms. The
     * entry of the second server is a configuration change. */
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_COMMAND, 2 /* term */, 0 /* payload */);

    CLUSTER_FILL_CONFIGURATION(&conf, 2 /* n */, 2 /* voters */, 0 /* stand */);
    entry.type = RAFT_CHANGE;
    entry.term = 1;
    rv = raft_configuration_add(&conf, 3, "3", 2);
    munit_assert_int(rv, ==, 0);
    raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);
    CLUSTER_ADD_ENTRY_RAW(2 /* ID */, &entry);
    raft_free(entry.buf.base);
    raft_configuration_close(&conf);

    /* At startup the server 2 uses the most recent configuration, i.e. the
     * one contained in the entry that we just added. The server can't know yet
     * if it's committed or not, and regards it as pending configuration
     * change. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 2, 2 entries (1^1..2^2)\n"
        "[   0] 2 > term 2, 2 entries (1^1..2^1)\n");

    raft = CLUSTER_RAFT(2);
    munit_assert_uint(raft->configuration.n, ==, 3);
    munit_assert_ullong(raft->configuration_uncommitted_index, ==, 2);
    munit_assert_ullong(raft->configuration_committed_index, ==, 1);

    /* Server 1 gets elected. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is more recent (2^2 vs 2^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^3)\n"
        "           probe server 2 sending 1 entry (3^3)\n");

    /* Server 2 eventually replicates the server 1's log entry at index 2,
     * truncating its own log and rolling back to the configuration contained in
     * the log entry at index 1. */
    CLUSTER_TRACE(
        "[ 130] 1 > persisted 1 entry (3^3)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           previous term mismatch -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           probe server 2 sending 2 entries (2^2..3^3)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           log mismatch (2^1 vs 2^2) -> truncate\n"
        "           roll back uncommitted configuration (2^1)\n"
        "           start persisting 2 new entries (2^2..3^3)\n");

    munit_assert_uint(raft->configuration.n, ==, 2);
    munit_assert_ullong(raft->configuration_uncommitted_index, ==, 0);
    munit_assert_ullong(raft->configuration_committed_index, ==, 1);

    return MUNIT_OK;
}

/* The follower has an uncommitted log entry that conflicts with a new one sent
 * by the leader (same index but different term). The follower's conflicting log
 * entry happens to be a configuration change. There's also an older committed
 * configuration entry present. In that case the follower discards the
 * conflicting entry from its log and rolls back its configuration to the
 * committed one in the older configuration entry. */
TEST_V1(replication, RollbackConfigurationToPrevious, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;
    struct raft_entry entry;
    struct raft_configuration conf; /* Uncommitted configuration at index 3 */
    struct raft *raft;
    int rv;

    /* Bootstrap a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 3 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    }

    /* Both servers have a matching configuration entry at index 2. */
    CLUSTER_ADD_ENTRY(1, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_ADD_ENTRY(2, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);

    /* Both servers have an entry at index 3, but with conflicting terms. The
     * entry of the second server is a configuration change. */
    CLUSTER_ADD_ENTRY(1, RAFT_COMMAND, 2 /* term */, 0 /* payload */);

    CLUSTER_FILL_CONFIGURATION(&conf, 2 /* n */, 2 /* voters */, 0 /* stand */);
    entry.type = RAFT_CHANGE;
    entry.term = 1;
    raft_configuration_add(&conf, 3, "3", 2);
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);
    CLUSTER_ADD_ENTRY_RAW(2, &entry);
    raft_configuration_close(&conf);
    raft_free(entry.buf.base);

    CLUSTER_START(1);
    CLUSTER_START(2);

    /* At startup the second server uses the most recent configuration, i.e. the
     * one contained in the log entry at index 3. The server can't know yet if
     * it's committed or not, and regards it as pending configuration change. */
    CLUSTER_TRACE(
        "[   0] 1 > term 3, 3 entries (1^1..3^2)\n"
        "[   0] 2 > term 3, 3 entries (1^1..3^1)\n");

    raft = CLUSTER_RAFT(2);
    munit_assert_uint(raft->configuration.n, ==, 3);
    munit_assert_ullong(raft->configuration_uncommitted_index, ==, 3);
    munit_assert_ullong(raft->configuration_committed_index, ==, 2);

    /* The first server gets elected. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 4\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (4 vs 3) -> bump term\n"
        "           remote log is more recent (3^2 vs 3^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (4^4)\n"
        "           probe server 2 sending 1 entry (4^4)\n");
    return 0;

    /* Server 2 eventually replicates the server 1's log entry at index 3,
     * truncating its own log and rolling back to the configuration contained in
     * the log entry at index 2. */
    CLUSTER_TRACE(
        "[ 130] 1 > persisted 1 entry (4^4)\n"
        "           next uncommitted entry (4^4) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           previous term mismatch -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           probe server 2 sending 2 entries (3.2..4.4)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           log mismatch (3^1 vs 3^2) -> truncate\n"
        "           roll back uncommitted configuration (3^1)\n"
        "           start persisting 2 new entries (3^2..4^4)\n")

    munit_assert_uint(raft->configuration.n, ==, 2);
    munit_assert_ullong(raft->configuration_uncommitted_index, ==, 0);
    munit_assert_ullong(raft->configuration_committed_index, ==, 2);

    return MUNIT_OK;
}

/* The follower has an uncommitted log entry that conflicts with a new one sent
 * by the leader (same index but different term). The follower's conflicting log
 * entry happens to be a configuration change. The follower's log has been
 * truncated after a snashot and does not contain the previous committed
 * configuration anymore. In that case the follower discards the conflicting
 * entry from its log and rolls back its configuration to the previous committed
 * one, which was cached when the snapshot was restored. */
TEST_V1(replication, RollbackConfigurationToSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_configuration conf; /* Uncommitted configuration at index 2 */
    struct raft *raft;
    int rv;

    /* Bootstrap server 1, creating a log entry at index 1 containing
     * the initial configuration. */
    CLUSTER_SET_TERM(1 /* ID */, 3 /* term */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);

    /* Server 2 has a snapshot up to entry 1. Entry 1 is not present in the
     * log. */
    CLUSTER_SET_TERM(2 /* ID */, 3 /* term */);
    CLUSTER_SET_SNAPSHOT(2 /*                                               */,
                         1 /* last index                                    */,
                         1 /* last term                                     */,
                         2 /* N servers                                     */,
                         2 /* N voting                                      */,
                         1 /* conf index                                    */);

    /* Both servers have an entry at index 2, but with conflicting terms. The
     * entry of the second server is a configuration change and gets appended to
     * the truncated log. */
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_COMMAND, 3 /* term */, 0 /* payload */);

    CLUSTER_FILL_CONFIGURATION(&conf, 2 /* n */, 2 /* voters */, 0 /* stand */);
    entry.type = RAFT_CHANGE;
    entry.term = 2;
    rv = raft_configuration_add(&conf, 3, "3", 2);
    munit_assert_int(rv, ==, 0);
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);
    CLUSTER_ADD_ENTRY_RAW(2 /* ID */, &entry);
    raft_configuration_close(&conf);
    raft_free(entry.buf.base);

    /* At startup server 2 uses the most recent configuration, i.e. the one
     * contained in the log entry at index 2. The server can't know yet if it's
     * committed or not, and regards it as pending configuration change. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 3, 2 entries (1^1..2^3)\n"
        "[   0] 2 > term 3, 1 snapshot (1^1), 1 entry (2^2)\n");

    raft = CLUSTER_RAFT(2);
    munit_assert_uint(raft->configuration.n, ==, 3);
    munit_assert_ullong(raft->configuration_uncommitted_index, ==, 2);
    munit_assert_ullong(raft->configuration_committed_index, ==, 1);

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 4\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (4 vs 3) -> bump term\n"
        "           remote log is more recent (2^3 vs 2^2) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^4)\n"
        "           probe server 2 sending 1 entry (3^4)\n");

    /* Server 2 eventually replicates the server 1's log entry at index 3,
     * truncating its own log and rolling back to the configuration contained in
     * the snapshot, which is not present in the log anymore but was cached at
     * startup. */
    CLUSTER_TRACE(
        "[ 130] 1 > persisted 1 entry (3^4)\n"
        "           next uncommitted entry (2^3) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           previous term mismatch -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           probe server 2 sending 2 entries (2^3..3^4)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           log mismatch (2^2 vs 2^3) -> truncate\n"
        "           roll back uncommitted configuration (2^2)\n"
        "           start persisting 2 new entries (2^3..3^4)\n");

    munit_assert_uint(raft->configuration.n, ==, 2);
    munit_assert_ullong(raft->configuration_uncommitted_index, ==, 0);
    munit_assert_ullong(raft->configuration_committed_index, ==, 1);

    return MUNIT_OK;
}

/* If any of the new entry has the same index of an existing entry in our log,
 * but different term, and that entry index is already committed, we bail out
 * with an error. */
TEST(replication, recvPrevIndexConflict, setUp, tearDown, 0, NULL)
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

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log, they will be skipped. */
TEST_V1(replication, SkipExistingEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    unsigned id;

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

    /* The follower receives and responds to the first heartbeat, and the leader
     * transitions it to pipeline mode. */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    /* Submit a new entry. */
    entry.term = 2;
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    /* The follower eventually receive the entry. */
    CLUSTER_TRACE(
        "[ 140] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 150] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 160] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n");

    /* The follower disconnects and the leader does not get notified about
     * the result. The leader tries to send a heartbeat but fails, so it
     * transitions the follower back to probe mode and eventually sends the
     * entry again. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);

    CLUSTER_TRACE(
        "[ 190] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "[ 240] 1 > timeout as leader\n"
        "           probe server 2 sending 1 entry (2^2)\n");

    /* The follower reconnects, it receives the duplicate entry but does not
     * persist again. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_RECONNECT(2, 1);
    CLUSTER_TRACE(
        "[ 250] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    return MUNIT_OK;
}

/* If the index and term of the last snapshot on the server matches prevLogIndex
 * and prevLogTerm the request is accepted. */
TEST_V1(replication, MatchingLastSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Server 1 has 2 entries, at index 1 and. 2 */
    CLUSTER_SET_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_COMMAND, 2 /* term */, 0 /* payload */);

    /* Server 2 has a snapshot up to entry 2 */
    CLUSTER_SET_TERM(2 /* ID */, 2 /* term */);
    CLUSTER_SET_SNAPSHOT(2, /* ID                                        */
                         2, /* last index                                */
                         2, /* last term                                 */
                         2, /* N servers                                 */
                         2, /* N voting                                  */
                         1 /* conf index                                */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);

    CLUSTER_TRACE(
        "[   0] 1 > term 2, 2 entries (1^1..2^2)\n"
        "[   0] 2 > term 2, 1 snapshot (2^2)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (2^2) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^3)\n"
        "           probe server 2 sending 1 entry (3^3)\n");

    CLUSTER_TRACE(
        "[ 130] 1 > persisted 1 entry (3^3)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (3^3)\n"
        "[ 140] 2 > persisted 1 entry (3^3)\n"
        "           send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           commit 2 new entries (2^2..3^3)\n");

    return MUNIT_OK;
}

/* If a candidate server receives a request containing the same term as its
 * own, it it steps down to follower and accept the request . */
TEST_V1(replication, CandidateRecvRequestWithSameTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 3 voters and one existing entry. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 2 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 2 /* term */, 0 /* payload */);
        CLUSTER_START(id);
    }

    /* Disconnect server 3 from the other two and set a low election timeout on
     * it, so it will immediately start an election. */
    CLUSTER_DISCONNECT(3, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 2);
    CLUSTER_DISCONNECT(2, 3);
    CLUSTER_SET_ELECTION_TIMEOUT(3 /* ID */, 50 /* timeout */, 40 /* delta */);

    CLUSTER_TRACE(
        "[   0] 1 > term 2, 2 entries (1^1..2^2)\n"
        "[   0] 2 > term 2, 2 entries (1^1..2^2)\n"
        "[   0] 3 > term 2, 2 entries (1^1..2^2)\n"
        "[  90] 3 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    /* Server 1 wins the election and start replicating a no-op entry since
     * this an uncommitted log entry from another term. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (2^2) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^3)\n"
        "           probe server 2 sending 1 entry (3^3)\n"
        "           probe server 3 sending 1 entry (3^3)\n");

    /* Now reconnect server 3, which eventually steps down and replicates the
     * barrier entry. */
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_RECONNECT(1, 3);

    CLUSTER_TRACE(
        "[ 130] 1 > persisted 1 entry (3^3)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (3^3)\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           discovered leader (1) -> step down \n"
        "           start persisting 1 new entry (3^3)\n");

    return MUNIT_OK;
}

/* If a candidate server receives an append entries request contaning an higher
 * term than its own, it it steps down to follower and accept the request. */
TEST_V1(replication,
        CandidateRecvRequestWithHigherTerm,
        setUp,
        tearDown,
        0,
        NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 3 voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }

    /* Set a high election timeout on server 2, so it won't become candidate */
    CLUSTER_SET_ELECTION_TIMEOUT(2 /* ID */, 250 /* timeout */, 0 /* delta */);

    /* Disconnect server 3 from the other two. */
    CLUSTER_DISCONNECT(3, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 2);
    CLUSTER_DISCONNECT(2, 3);

    /* Set a low election timeout on server 1, and disconnect it from server 2,
     * so by the time it starts the second round, server 3 will have turned
     * candidate */
    CLUSTER_SET_ELECTION_TIMEOUT(1 /* ID */, 50 /* timeout */, 47 /* delta */);
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);

    /* Server 3 becomes candidate, and server 1 already is candidate. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n"
        "[  97] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 160] 3 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* Server 1 starts a new election, while server 3 is still candidate */
    CLUSTER_SET_ELECTION_TIMEOUT(1 /* ID */, 50 /* timeout */, 14 /* delta */);
    CLUSTER_TRACE(
        "[ 161] 1 > timeout as candidate\n"
        "           stay candidate, start election for term 3\n");

    /* Reconnect the server 1 and server 2, let the election succeed and
     * the initial heartbeat to be sent. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_RECONNECT(2, 1);
    CLUSTER_TRACE(
        "[ 171] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 181] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Now reconnect the server 3, which eventually steps down when it receives
     * the heartbeat. */
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_RECONNECT(1, 3);
    CLUSTER_TRACE(
        "[ 191] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 191] 3 > recv append entries from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           no new entries to persist\n");

    return MUNIT_OK;
}

/* If the server handling the response is not the leader, the result is
 * ignored. */
TEST_V1(replication, ReceiveResultButNotLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

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

    /* Set a very high-latency for server 2's outgoing messages, so server 1
     * won't get notified about the results for a while. */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 100 /* latency */);

    /* Set a low election timeout on server 1 so it will step down very soon. */
    CLUSTER_SET_ELECTION_TIMEOUT(1 /* ID */, 30 /* timeout */, 15 /* delta */);

    /* Eventually server 1 steps down becomes candidate. */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 150] 1 > timeout as leader\n"
        "           unable to contact majority of cluster -> step down\n"
        "[ 195] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    /* The AppendEntries result eventually gets delivered, but the candidate
     * ignores it. */
    CLUSTER_TRACE(
        "[ 205] 2 > recv request vote from server 1\n"
        "           local server has a leader (server 1) -> reject\n"
        "[ 230] 1 > recv append entries result from server 2\n"
        "           local server is not leader -> ignore\n");

    return MUNIT_OK;
}

/* If the response has a term which is lower than the server's one, it's
 * ignored. */
TEST_V1(replication, ReceiveResultWithLowerTerm, setUp, tearDown, 0, NULL)
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
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Set a very high-latency for the server 2's outgoing messages, so server 1
     * won't get notified about the results for a while. */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 80 /* latency */);

    /* Set a high election timeout on server 2, so it won't become candidate */
    CLUSTER_SET_ELECTION_TIMEOUT(2 /* ID */, 500 /* timeout */, 0 /* delta */);

    /* Disconnect server 1 from server 3 and set a low election timeout on it so
     * it will step down very soon. */
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);
    CLUSTER_SET_ELECTION_TIMEOUT(1 /* ID */, 20 /* timeout */, 20 /* delta */);

    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > timeout as leader\n"
        "           unable to contact majority of cluster -> step down\n");

    /* Make server 1 become leader again. */
    CLUSTER_RECONNECT(1, 3);
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_TRACE(
        "[ 180] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 190] 2 > recv request vote from server 1\n"
        "           local server has a leader (server 1) -> reject\n"
        "[ 190] 3 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 200] 1 > recv request vote result from server 3\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Eventually deliver to server 1 the result message sent by server 2 in the
     * previous term. */
    CLUSTER_TRACE(
        "[ 210] 1 > recv append entries result from server 2\n"
        "           local term is higher (3 vs 2) -> ignore\n");

    return MUNIT_OK;
}

/* If the response has a term which is higher than the server's one, step down
 * to follower. */
TEST_V1(replication, ReceiveResultWithHigherTerm, setUp, tearDown, 0, NULL)
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
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Set a very high election timeout for server 1 so it won't step down.
     */
    CLUSTER_SET_ELECTION_TIMEOUT(1 /* ID */, 500 /* timeout */, 0 /* delta */);

    /* Disconnect the server 1 from the rest of the cluster. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);

    /* Eventually a new leader gets electected */
    CLUSTER_TRACE(
        "[ 170] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 220] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 240] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 250] 3 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 260] 2 > recv request vote result from server 3\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 1 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Reconnect the old leader server 1 to the current follower server 3,
     * which eventually replies with an AppendEntries result containing an
     * higher term. */
    CLUSTER_RECONNECT(1, 3);
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_TRACE(
        "[ 270] 3 > recv append entries from server 2\n"
        "           no new entries to persist\n"
        "[ 270] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 280] 2 > recv append entries result from server 3\n"
        "[ 280] 3 > recv append entries from server 1\n"
        "           local term is higher (3 vs 2) -> reject\n"
        "[ 290] 1 > recv append entries result from server 3\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n");

    return MUNIT_OK;
}

static void applyAssertStatusCb(struct raft_apply *req,
                                int status,
                                void *result)
{
    (void)result;
    int status_expected = (int)(intptr_t)(req->data);
    munit_assert_int(status_expected, ==, status);
}

/* When the leader fails to write some new entries to disk, it steps down. */
TEST(replication, diskWriteFailure, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof(*req));
    req->data = (void *)(intptr_t)RAFT_IOERR;
    BOOTSTRAP_START_AND_ELECT;

    CLUSTER_IO_FAULT(0, 1, 1);
    CLUSTER_APPLY_ADD_X(0, req, 1, applyAssertStatusCb);
    /* The leader steps down when its disk write fails. */
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 2000);
    free(req);

    return MUNIT_OK;
}

/* A leader with slow disk commits an entry that it hasn't persisted yet,
 * because enough followers to have a majority have aknowledged that they have
 * appended the entry. The leader's last_stored field hence lags behind its
 * commit_index. A new leader gets elected, with a higher commit index, and
 * sends an an entry to the old leader, that needs to update its commit_index
 * taking into account its lagging last_stored. */
TEST_V1(replication,
        LastStoredLaggingBehindCommitIndex,
        setUp,
        tearDown,
        0,
        NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 3 voters. All servers have an
     * uncommitted log entry. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        CLUSTER_START(id);
    }

    /* Server 1 will take a long time to persist the initial barrier entry at
     * index 3. */
    CLUSTER_SET_DISK_LATENCY(1 /* ID */, 1000 /* latency */);

    /* Server 1 gets elected and creates a no-op entry at index 2 */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 1, 2 entries (1^1..2^1)\n"
        "[   0] 3 > term 1, 2 entries (1^1..2^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (2^1) -> grant vote\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (2^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^2)\n"
        "           probe server 2 sending 1 entry (3^2)\n"
        "           probe server 3 sending 1 entry (3^2)\n");

    /* Server 1 commits the no-op entry at index 2 even if it did not
     * persist it yet. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (3^2)\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           start persisting 1 new entry (3^2)\n"
        "[ 140] 2 > persisted 1 entry (3^2)\n"
        "           send success result to 1\n"
        "[ 140] 3 > persisted 1 entry (3^2)\n"
        "           send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           next uncommitted entry (2^1) has 2 votes out of 3\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           commit 2 new entries (2^1..3^2)\n");

    munit_assert_ullong(CLUSTER_RAFT(1)->last_stored, ==, 2);
    munit_assert_ullong(CLUSTER_RAFT(1)->commit_index, ==, 3);

    /* Server 2 stored barrier entry 3, but did not yet receive a
     * notification from server 1 about the new commit index. */
    munit_assert_ullong(CLUSTER_RAFT(2)->last_stored, ==, 3);
    munit_assert_ullong(CLUSTER_RAFT(2)->commit_index, ==, 1);

    /* Disconnect server 1 from server 2 and 3. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(1, 3);

    /* Set a very high election timeout on server 1, so it won't step down
     * for a while, even if disconnected. */
    CLUSTER_SET_ELECTION_TIMEOUT(1, 500 /* timeout */, 0 /* delta */);

    /* Server 2 and 3 eventually timeout and start an election, server 2
     * wins (lower election timeouts to make that happen faster). */
    CLUSTER_SET_ELECTION_TIMEOUT(2, 40 /* timeout */, 0 /* delta */);
    CLUSTER_SET_ELECTION_TIMEOUT(3, 40 /* timeout */, 10 /* delta */);
    CLUSTER_TRACE(
        "[ 170] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n"
        "[ 170] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 180] 1 > recv request vote from server 2\n"
        "           local server is leader -> reject\n"
        "[ 180] 3 > recv request vote from server 2\n"
        "           local server has a leader (server 1) -> reject\n"
        "[ 180] 3 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 190] 2 > recv request vote result from server 3\n"
        "           remote term is lower (2 vs 3) -> ignore\n"
        "[ 190] 1 > recv request vote from server 3\n"
        "           local server is leader -> reject\n"
        "[ 190] 2 > recv request vote from server 3\n"
        "           already voted for server 2 -> don't grant vote\n"
        "[ 200] 3 > recv request vote result from server 2\n"
        "           vote not granted\n"
        "[ 210] 2 > timeout as candidate\n"
        "           stay candidate, start election for term 4\n"
        "[ 220] 1 > recv request vote from server 2\n"
        "           local server is leader -> reject\n"
        "[ 220] 3 > recv request vote from server 2\n"
        "           remote term is higher (4 vs 3) -> bump term, step down\n"
        "           remote log is equal (3^2) -> grant vote\n"
        "[ 220] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 230] 2 > recv request vote result from server 3\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           replicate 1 new barrier entry (4^4)\n"
        "           probe server 1 sending 1 entry (4^4)\n"
        "           probe server 3 sending 1 entry (4^4)\n");

    /* Server 2 commits the barrier entry at index 4 that it created at the
     * start of its term. */
    CLUSTER_TRACE(
        "[ 240] 2 > persisted 1 entry (4^4)\n"
        "           next uncommitted entry (3^2) has 1 vote out of 3\n"
        "[ 240] 1 > recv append entries from server 2\n"
        "           remote term is higher (4 vs 2) -> bump term, step down\n"
        "           start persisting 1 new entry (4^4)\n"
        "[ 240] 3 > recv append entries from server 2\n"
        "           start persisting 1 new entry (4^4)\n"
        "[ 250] 3 > persisted 1 entry (4^4)\n"
        "           send success result to 2\n"
        "[ 260] 2 > recv append entries result from server 3\n"
        "           commit 3 new entries (2^1..4^4)\n");

    /* Reconnect server 1 to server 2, which will replicate entry 4 to
     * it. */
    CLUSTER_RECONNECT(1, 2);

    CLUSTER_TRACE(
        "[ 270] 2 > timeout as leader\n"
        "[ 280] 2 > timeout as leader\n"
        "           probe server 1 sending 1 entry (4^4)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n"
        "[ 290] 1 > recv append entries from server 2\n"
        "           no new entries to persist\n");

    return MUNIT_OK;
}

/* A leader with slow disk commits an entry that it hasn't persisted yet,
 * because enough followers to have a majority have aknowledged that they have
 * appended the entry. The leader's last_stored field hence lags behind its
 * commit_index. A new leader gets elected, with a higher commit index and sends
 * first a new entry than a heartbeat to the old leader, that needs to update
 * its commit_index taking into account its lagging last_stored.
 *
 * XXX: this test duplicates the one above, but it's kept because the change it
 * is associated with was fixing an assertion in the legacy compat layer. */
TEST(replication, lastStoredLaggingBehindCommitIndex, setUp, tearDown, 0, NULL)
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
TEST(replication, failPersistBarrier, setUp, tearDown, 0, NULL)
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

/* A follower finds that is has no leader anymore after it completes persisting
 * entries. No AppendEntries RPC result is sent in that case. */
TEST_V1(replication, NoLeaderAfterPersistingEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters. Both have an additional
     * entry at index 2.*/
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        CLUSTER_START(id);
    }

    /* Make sure that persisting entries on server 1 will take a long time. */
    CLUSTER_SET_DISK_LATENCY(1 /* ID */, 50 /* latency */);

    /* Server 1 becomes the leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 1, 2 entries (1^1..2^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (2^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^2)\n"
        "           probe server 2 sending 1 entry (3^2)\n");

    /* Disconnect server 1, so it will step down and become follower (lower
     * its election timeout to make this happen sooner). */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_SET_ELECTION_TIMEOUT(1 /* ID */, 40 /* timeout */, 0 /* delta*/);

    CLUSTER_TRACE(
        "[ 160] 1 > timeout as leader\n"
        "           unable to contact majority of cluster -> step down\n");

    /* Server 1 has stepped down and is now a follower, however its hasn't
     * persisted the barrier entry yet. */
    munit_assert_int(raft_state(CLUSTER_RAFT(1)), ==, RAFT_FOLLOWER);
    munit_assert_ullong(CLUSTER_RAFT(1)->last_stored, ==, 2);

    /* Wait for the long disk write to complete */
    CLUSTER_TRACE("[ 170] 1 > persisted 1 entry (3^2)\n");

    /* The writes have now completed, one for the barrier entry at the start
     * of the term and one for the command entry we submitted. */
    munit_assert_ullong(CLUSTER_RAFT(1)->last_stored, ==, 3);

    return MUNIT_OK;
}

/* While pipelining entries, the leader receives an AppendEntries response with
 * a stale reject index. */
TEST_V1(replication, StaleRejectedIndexPipeline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters. Server 1 has an additional
     * entry at index 2.*/
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        if (id == 1) {
            CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        }
        CLUSTER_START(id);
    }

    /* Server 1 becomes the leader. */
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
        "           probe server 2 sending 1 entry (3^2)\n"
        "[ 130] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^1) has 1 vote out of 2\n");

    /* Server 2 receives a heartbeat with an entry that it does not have, but
     * the network is slow, so server 1 will receive the response much later. */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 100 /* latency */);
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n"
        "[ 170] 1 > timeout as leader\n"
        "           probe server 2 sending 1 entry (3^2)\n");

    /* This time the network is faster, and server 2's response to the second
     * heartbeat will arrive before then first response. */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 10 /* latency */);
    CLUSTER_TRACE(
        "[ 180] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n"
        "[ 190] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           probe server 2 sending 2 entries (2^1..3^2)\n"
        "[ 200] 2 > recv append entries from server 1\n"
        "           start persisting 2 new entries (2^1..3^2)\n"
        "[ 210] 2 > persisted 2 entry (2^1..3^2)\n"
        "           send success result to 1\n"
        "[ 220] 1 > recv append entries result from server 2\n"
        "           commit 2 new entries (2^1..3^2)\n"
        "[ 220] 1 > timeout as leader\n"
        "[ 230] 1 > recv append entries result from server 2\n"
        "           stale rejected index (2 vs match index 3) -> ignore\n");

    return MUNIT_OK;
}

/* After having sent a snapshot and waiting for a response, the leader receives
 * an AppendEntries response with a stale reject index. */
TEST_V1(replication, StaleRejectedIndexSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    int rv;

    /* Set very low threshold and trailing entries number. */
    raft_set_snapshot_threshold(CLUSTER_RAFT(1), 3);
    raft_set_snapshot_trailing(CLUSTER_RAFT(1), 0);

    /* Bootstrap and start a cluster with 1 voter and 1 stand-by. Server 1 has
     * an additional entry. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);

        CLUSTER_FILL_CONFIGURATION(&configuration, 2, 1, 1 /* stand-by */);
        entry.type = RAFT_CHANGE;
        entry.term = 1;
        rv = raft_configuration_encode(&configuration, &entry.buf);
        munit_assert_int(rv, ==, 0);
        raft_configuration_close(&configuration);
        test_cluster_add_entry(&f->cluster_, id, &entry);
        raft_free(entry.buf.base);

        if (id == 1) {
            CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        }

        CLUSTER_START(id);
    }

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "           self elect and convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n");

    /* Server 2 receives a heartbeat with an entry that it does not have, but
     * the network is slow, so server 1 will receive the response much later. */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 80 /* latency */);
    CLUSTER_TRACE(
        "[  10] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n");

    /* Server 1 commits a new entry and takes a snapshot. */
    entry.term = 1;
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);
    munit_assert_not_null(entry.buf.base);
    test_cluster_submit(&f->cluster_, 1, &entry);

    CLUSTER_TRACE(
        "[  10] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (3^1)\n"
        "[  20] 1 > persisted 1 entry (3^1)\n"
        "           commit 1 new entry (3^1)\n"
        "[  20] 1 > new snapshot (3^1), 0 trailing entries\n");

    /* Eventually server 2 receives the snapshot, but takes a long time to
     * persist it. */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 10 /* latency */);
    CLUSTER_SET_DISK_LATENCY(2 /* ID */, 100 /* latency */);
    CLUSTER_TRACE(
        "[  50] 1 > timeout as leader\n"
        "           missing previous entry at index 2 -> needs snapshot\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[  60] 2 > recv append entries from server 1\n"
        "           missing previous entry (3^1) -> reject\n"
        "[  70] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (3^1) to server 2\n"
        "[  80] 2 > recv install snapshot from server 1\n"
        "           start persisting snapshot (3^1)\n");

    /* Server 1 finally receives the original AppendEntries response and ignores
     * it. */
    CLUSTER_TRACE(
        "[  90] 1 > recv append entries result from server 2\n"
        "           stale rejected index (2 vs snapshot index 3) -> ignore\n");

    return MUNIT_OK;
}
