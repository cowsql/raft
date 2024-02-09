#include "../../src/progress.h"
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

SUITE(replication)

/* A leader doesn't send an initial no-op barrier entry if its committed index
 * is as big as its last log index. */
TEST(replication, NoInitialBarrier, setUp, tearDown, 0, NULL)
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
TEST(replication, InitialBarrier, setUp, tearDown, 0, NULL)
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
TEST(replication, FeatureFlags, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    unsigned id;

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

    /* Features were already populated via RequestVote result. */
    raft = CLUSTER_RAFT(1);
    munit_assert_uint(raft->leader_state.progress[1].features, ==, 1);

    /* Server 2 receives the heartbeat and replies. When server 1 receives the
     * response, the feature flags are set. */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    munit_assert_uint(raft->leader_state.progress[1].features, ==, 1);

    return MUNIT_OK;
}

/* A leader keeps sending heartbeat messages at regular intervals to
 * maintain leadership. */
TEST(replication, Heartbeat, setUp, tearDown, 0, NULL)
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
TEST(replication, SkipHeartbeatIfEntriesHaveSent, setUp, tearDown, 0, NULL)
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
    entry.batch = entry.buf.base;
    CLUSTER_SUBMIT(1 /* ID */, &entry);

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
TEST(replication, SkipSpare, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
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

    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

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
TEST(replication, Probe, setUp, tearDown, 0, NULL)
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

    /* Set a network latency higher than the heartbeat timeout for server 2, so
     * server 1 will send a second probe AppendEntries without transitioning to
     * pipeline mode. */
    CLUSTER_SET_NETWORK_LATENCY(2 /* ID */, 100 /* msecs */);

    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    /* Server 1 receives a new entry after a few milliseconds. Since the
     * follower is still in probe mode and since an AppendEntries message was
     * already sent recently, it does not send the new entry immediately. */
    CLUSTER_ELAPSE(5);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

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
    CLUSTER_ELAPSE(5);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

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
TEST(replication, Pipeline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
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
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

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

    CLUSTER_ELAPSE(5);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

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
TEST(replication, Disconnect, setUp, tearDown, 0, NULL)
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
TEST(replication, PipelineDisconnect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
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
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

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

    /* A full election timeout eventually elapses, and since server 1 did not
     * receive any message from server 2, it transitions server 2 back to probe
     * mode. */
    CLUSTER_TRACE(
        "[ 160] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 160] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 200] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "[ 250] 1 > timeout as leader\n"
        "           server 2 is unreachable -> abort pipeline\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n");

    munit_assert_ullong(raft->leader_state.progress[1].next_index, ==, 2);

    /* After reconnection the follower eventually replicates the entries and
     * reports back. */
    CLUSTER_RECONNECT(1, 2);

    CLUSTER_TRACE(
        "[ 260] 2 > recv append entries from server 1\n"
        "           start persisting 2 new entries (2^2..3^2)\n"
        "[ 270] 2 > persisted 2 entry (2^2..3^2)\n"
        "           send success result to 1\n");

    return MUNIT_OK;
}

/* Receive the same entry a second time, before the first has been persisted. */
TEST(replication, ReceiveSameEntryTwice, setUp, tearDown, 0, NULL)
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
TEST(replication, AppendEntriesRequestHasStaleTerm, setUp, tearDown, 0, NULL)
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
TEST(replication, FollowerHasMissingEntries, setUp, tearDown, 0, NULL)
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
TEST(replication, PrevLogTermMismatch, setUp, tearDown, 0, NULL)
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
TEST(replication, RollbackConfigurationToInitial, setUp, tearDown, 0, NULL)
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
    CLUSTER_ADD_ENTRY(2 /* ID */, &entry);
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
TEST(replication, RollbackConfigurationToPrevious, setUp, tearDown, 0, NULL)
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
    CLUSTER_ADD_ENTRY(2, &entry);
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
TEST(replication, RollbackConfigurationToSnapshot, setUp, tearDown, 0, NULL)
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
    CLUSTER_ADD_ENTRY(2 /* ID */, &entry);
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

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log, they will be skipped. */
TEST(replication, SkipExistingEntries, setUp, tearDown, 0, NULL)
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

    /* The follower receives and responds to the first heartbeat, and the leader
     * transitions it to pipeline mode. */
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n");

    /* Submit a new entry. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

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
        "           server 2 is unreachable -> abort pipeline\n"
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
TEST(replication, MatchingLastSnapshot, setUp, tearDown, 0, NULL)
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
TEST(replication, CandidateRecvRequestWithSameTerm, setUp, tearDown, 0, NULL)
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
TEST(replication, CandidateRecvRequestWithHigherTerm, setUp, tearDown, 0, NULL)
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
TEST(replication, ReceiveResultButNotLeader, setUp, tearDown, 0, NULL)
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
TEST(replication, ReceiveResultWithLowerTerm, setUp, tearDown, 0, NULL)
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
TEST(replication, ReceiveResultWithHigherTerm, setUp, tearDown, 0, NULL)
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

/* A leader with slow disk commits an entry that it hasn't persisted yet,
 * because enough followers to have a majority have aknowledged that they have
 * appended the entry. The leader's last_stored field hence lags behind its
 * commit_index. A new leader gets elected, with a higher commit index, and
 * sends an an entry to the old leader, that needs to update its commit_index
 * taking into account its lagging last_stored. */
TEST(replication, LastStoredLaggingBehindCommitIndex, setUp, tearDown, 0, NULL)
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
        "           server 2 is unreachable -> abort pipeline\n"
        "           server 3 is unreachable -> abort pipeline\n"
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
        "           server 3 is unreachable -> abort pipeline\n"
        "           probe server 1 sending 1 entry (4^4)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
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

/* A follower finds that is has no leader anymore after it completes persisting
 * entries. No AppendEntries RPC result is sent in that case. */
TEST(replication, NoLeaderAfterPersistingEntries, setUp, tearDown, 0, NULL)
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
TEST(replication, PipelineStaleRejectedIndex, setUp, tearDown, 0, NULL)
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

/* The raft_max_inflight_entries() setting controls how many un-acknowledged
 * entries can be in-flight in pipeline mode. */
TEST(replication, PipelineMaxInflight, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Set the maximum in-flight entries number to 2. */
    raft_set_max_inflight_entries(CLUSTER_RAFT(1), 2);

    /* Bootstrap and start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader and eventually switches server 2 to pipeline
     * mode. */
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
        "[ 140] 1 > recv append entries result from server 2\n"
        "[ 170] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n");

    /* Two entries are submitted and immediately sent to server 2. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[ 170] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 170] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (3^2)\n"
        "           pipeline server 2 sending 1 entry (3^2)\n");

    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    /* A third entry is submitted, but it's not replicated immediately, because
     * the inflight limit has been reached. */
    CLUSTER_TRACE(
        "[ 170] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (4^2)\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}

/* After having sent a snapshot and waiting for a response, the leader receives
 * an AppendEntries response with a stale reject index. */
TEST(replication, StaleRejectedIndexSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    int rv;

    /* Set very low threshold and trailing entries number. */
    CLUSTER_SET_SNAPSHOT_THRESHOLD(1 /* ID */, 3 /* n. entries */);
    CLUSTER_SET_SNAPSHOT_TRAILING(1 /* ID */, 0 /* n. entries */);

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
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

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

/* If a follower receives a heartbeat containing a prev_log_index which is
 * behind its last_stored index, it sets the last_log_index to the value of
 * prev_log_index. This prevents the follower from telling the leader that it
 * reached a certain index without first checking the log matching property. */
TEST(replication, LastStoredAheadOfPrevLogIndex, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    int rv;

    /* Bootstrap and start a cluster with 1 voters and 1 stand-by. The stand-by
     * has an additional entry. */
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

        if (id == 2) {
            CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 2 /* term */, 0 /* payload */);
        }

        CLUSTER_START(id);
    }

    /* Start the cluster, server 1 will self elect and send an heartbeat to
     * server 2. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "           self elect and convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[   0] 2 > term 1, 2 entries (1^1..2^2)\n");

    /* Submit an entry, which won't be immediately sent to server 2, because
     * it's still in probe mode. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);
    CLUSTER_TRACE(
        "[   0] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^1)\n"
        "[  10] 1 > persisted 1 entry (2^1)\n"
        "           commit 1 new entry (2^1)\n");

    /* Server 2 receives the heartbeat from server 1. The two servers have now a
     * conflicting entry at term 2, but since this heartbeat only contains up to
     * entry 1, no conflict is detected yet by server 2, which sends a success
     * result. */
    CLUSTER_TRACE(
        "[  10] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[  20] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (2^1)\n");

    /* Disconnect server 2, which eventually gets back to probe mode. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_TRACE(
        "[  70] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "[ 120] 1 > timeout as leader\n"
        "           server 2 is unreachable -> abort pipeline\n"
        "           probe server 2 sending 1 entry (2^1)\n");

    /* Reconnect server 2, which eventually receives the conflicting entry at
     * index 2 and truncates its log. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           log mismatch (2^2 vs 2^1) -> truncate\n"
        "           start persisting 1 new entry (2^1)\n");

    return MUNIT_OK;
}

/* If a follower completes persisting an entry at an index that was not yet sent
 * by the current leader and checked via the log matching property, no
 * successful result for it will be sent. */
TEST(replication, LastStoredAheadOfLastMatched, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 5 voters. */
    for (id = 1; id <= 5; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 5 /* servers */, 5 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n"
        "[   0] 4 > term 1, 1 entry (1^1)\n"
        "[   0] 5 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 110] 4 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 110] 5 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum not reached, only 2 votes out of 5\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           quorum reached with 3 votes out of 5 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           probe server 4 sending a heartbeat (no entries)\n"
        "           probe server 5 sending a heartbeat (no entries)\n");

    /* Submit a new entry. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    /* Disconnect server 1 from all servers except server 3, which will be the
     * only one receiving it. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 4);
    CLUSTER_DISCONNECT(4, 1);
    CLUSTER_DISCONNECT(1, 5);
    CLUSTER_DISCONNECT(5, 1);

    /* Server 3 receives it but will take a long time to persist it. */
    CLUSTER_SET_DISK_LATENCY(3 /* ID */, 130 /* latency */);
    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 5\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           pipeline server 3 sending 1 entry (2^2)\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n");

    /* Crash server 1. Eventually server 2 becomes leader with votes from server
     * 4 and 5. */
    CLUSTER_STOP(1 /* ID */);

    CLUSTER_TRACE(
        "[ 240] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 250] 3 > recv request vote from server 2\n"
        "           local server has a leader (server 1) -> reject\n"
        "[ 250] 4 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 250] 5 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 260] 2 > recv request vote result from server 3\n"
        "           remote term is lower (2 vs 3) -> ignore\n"
        "[ 260] 2 > recv request vote result from server 4\n"
        "           quorum not reached, only 2 votes out of 5\n"
        "[ 260] 2 > recv request vote result from server 5\n"
        "           quorum reached with 3 votes out of 5 -> convert to leader\n"
        "           probe server 1 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           probe server 4 sending a heartbeat (no entries)\n"
        "           probe server 5 sending a heartbeat (no entries)\n");

    /* Server 3 receives the heartbeat from server 2 and chances its term and
     * leader. */
    CLUSTER_TRACE(
        "[ 270] 3 > recv append entries from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           no new entries to persist\n");
    CLUSTER_DISCONNECT(3, 2);

    /* Server 3 completes persisting the entry at index 2 that was sent to it by
     * server 1 at term 2. */
    CLUSTER_TRACE(
        "[ 270] 4 > recv append entries from server 2\n"
        "           no new entries to persist\n"
        "[ 270] 5 > recv append entries from server 2\n"
        "           no new entries to persist\n"
        "[ 280] 3 > persisted 1 entry (2^2)\n"
        "           send success result to 2\n");
    CLUSTER_RECONNECT(3, 2);

    /* Submit a new entry to server 2, which will have the same index of the
     * entry that server 3 just persisted. */
    CLUSTER_SUBMIT(2 /* ID */, COMMAND, 8 /* size */);
    CLUSTER_TRACE(
        "[ 280] 2 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^3)\n"
        "[ 280] 2 > recv append entries result from server 4\n"
        "           pipeline server 4 sending 1 entry (2^3)\n"
        "[ 280] 2 > recv append entries result from server 5\n"
        "           pipeline server 5 sending 1 entry (2^3)\n"
        "[ 290] 2 > persisted 1 entry (2^3)\n"
        "           next uncommitted entry (2^3) has 1 vote out of 5\n");

    /* Server 2 receives the result from server 3, which does *not* contain the
     * conflicting entry. */
    CLUSTER_TRACE(
        "[ 290] 2 > recv append entries result from server 3\n"
        "           pipeline server 3 sending 1 entry (2^3)\n");

    /* Server 3 receives the conflicting entry and replaces its own one. */
    CLUSTER_TRACE(
        "[ 290] 4 > recv append entries from server 2\n"
        "           start persisting 1 new entry (2^3)\n"
        "[ 290] 5 > recv append entries from server 2\n"
        "           start persisting 1 new entry (2^3)\n"
        "[ 300] 3 > recv append entries from server 2\n"
        "           log mismatch (2^2 vs 2^3) -> truncate\n"
        "           start persisting 1 new entry (2^3)\n");

    return MUNIT_OK;
}
