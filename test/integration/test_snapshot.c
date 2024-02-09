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

SUITE(snapshot)

/* Install a snapshot on a follower that has fallen behind. */
TEST(snapshot, Install, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    int rv;

    /* Set very low threshold and trailing entries number */
    CLUSTER_SET_SNAPSHOT_THRESHOLD(1 /* ID */, 2 /* n. entries */);
    CLUSTER_SET_SNAPSHOT_TRAILING(1 /* ID */, 0 /* n. entries */);

    /* Don't let server 2 time out (just for terser traces). */
    raft_set_election_timeout(CLUSTER_RAFT(2), 200);

    /* Bootstrap and start a cluster with 1 voter and 1 stand-by. */
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

        CLUSTER_START(id);
    }

    /* Server 2 won't receive any entry from server 1. */
    CLUSTER_DISCONNECT(1, 2);

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "           self elect and convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n");

    /* Submit an entry which will to force a snapshot to be taken. */
    CLUSTER_ELAPSE(10);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[  10] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^1)\n"
        "[  20] 1 > persisted 1 entry (2^1)\n"
        "           commit 1 new entry (2^1)\n"
        "[  20] 1 > new snapshot (2^1), 0 trailing entries\n");

    /* Reconnect server 2, which eventually receives the snapshot. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_TRACE(
        "[  50] 1 > timeout as leader\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[  60] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n"
        "[  70] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (2^1) to server 2\n"
        "[  80] 2 > recv install snapshot from server 1\n"
        "           start persisting snapshot (2^1)\n"
        "[  90] 2 > persisted snapshot (2^1)\n"
        "           send success result to 1\n"
        "[ 100] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}

/* Install snapshot times out and leader retries */
TEST(snapshot, InstallTimeout, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    int rv;

    /* Set very low threshold and trailing entries number */
    CLUSTER_SET_SNAPSHOT_THRESHOLD(1 /* ID */, 2 /* n. entries */);
    CLUSTER_SET_SNAPSHOT_TRAILING(1 /* ID */, 0 /* n. entries */);

    /* Don't let server 2 time out (just for terser traces). */
    raft_set_election_timeout(CLUSTER_RAFT(2), 200);

    /* Bootstrap and start a cluster with 1 voter and 1 stand-by. */
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

        CLUSTER_START(id);
    }

    /* Server 2 won't receive any entry from server 1. */
    CLUSTER_DISCONNECT(1, 2);

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "           self elect and convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n");

    /* Submit an entry which will to force a snapshot to be taken. */
    CLUSTER_ELAPSE(10);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[  10] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^1)\n"
        "[  20] 1 > persisted 1 entry (2^1)\n"
        "           commit 1 new entry (2^1)\n"
        "[  20] 1 > new snapshot (2^1), 0 trailing entries\n");

    /* Reconnect server 2, which eventually receives the snapshot. Set a very
     * high disk latency on it, so it won't reply to server 1 fast enough,
     * making server 1 retry. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_SET_DISK_LATENCY(2, 80);

    CLUSTER_TRACE(
        "[  50] 1 > timeout as leader\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[  60] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n"
        "[  70] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (2^1) to server 2\n"
        "[  80] 2 > recv install snapshot from server 1\n"
        "           start persisting snapshot (2^1)\n"
        "[ 100] 1 > timeout as leader\n"
        "           missing previous entry at index 2 -> needs snapshot\n"
        "           snapshot server 2 sending a heartbeat (no entries)\n"
        "[ 110] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 120] 1 > recv append entries result from server 2\n"
        "[ 150] 1 > timeout as leader\n"
        "           timeout install snapshot at index 2\n"
        "           missing previous entry at index 0 -> needs snapshot\n"
        "           sending snapshot (2^1) to server 2\n");

    return MUNIT_OK;
}

/* Snapshots are not sent an offline nodes */
TEST(snapshot, SkipOffline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    int rv;

    /* Set very low threshold and trailing entries number */
    CLUSTER_SET_SNAPSHOT_THRESHOLD(1 /* ID */, 2 /* n. entries */);
    CLUSTER_SET_SNAPSHOT_TRAILING(1 /* ID */, 0 /* n. entries */);

    /* Bootstrap and start a cluster with 1 voter and 1 stand-by. Just start
     * server 1. */
    CLUSTER_SET_TERM(1, 1 /* term */);

    CLUSTER_FILL_CONFIGURATION(&configuration, 2, 1, 1 /* stand-by */);
    entry.type = RAFT_CHANGE;
    entry.term = 1;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);
    test_cluster_add_entry(&f->cluster_, 1, &entry);
    raft_free(entry.buf.base);

    CLUSTER_START(1);

    /* Server 1 becomes leader. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "           self elect and convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    /* Submit an entry which will to force a snapshot to be taken. */
    CLUSTER_ELAPSE(10);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[  10] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^1)\n"
        "[  20] 1 > persisted 1 entry (2^1)\n"
        "           commit 1 new entry (2^1)\n"
        "[  20] 1 > new snapshot (2^1), 0 trailing entries\n");

    /* Server 2 never comes online, so server 1 doesn't send it any
     * snapshot. */
    CLUSTER_TRACE(
        "[  50] 1 > timeout as leader\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 100] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 150] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}

/* A follower crashes while persisting a snapshot. After it resumes it sends a
 * reject response of the snapshot index. */
TEST(snapshot, AbortIfRejected, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Server 1 has a snapshot with last index 2. */
    CLUSTER_SET_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_SET_SNAPSHOT(1, /* ID                                        */
                         2, /* last index                                */
                         2, /* last term                                 */
                         2, /* N servers                                 */
                         2, /* N voting                                  */
                         1 /* conf index                                 */);
    CLUSTER_START(1 /* ID */);

    /* Server 2 has just the initial configuration entry at index 1. */
    CLUSTER_SET_TERM(2, 1 /* term */);
    CLUSTER_ADD_ENTRY(2, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_START(2);

    /* Server 1 becomes leader and eventually sends its snapshot to server 2. */
    CLUSTER_TRACE(
        "[   0] 1 > term 2, 1 snapshot (2^2)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log is more recent (2^2 vs 1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^2) -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (2^2) to server 2\n");

    /* Server 2 receives the snapshot, but crashes while persisting it. Then it
     * restarts. */
    CLUSTER_TRACE(
        "[ 150] 2 > recv install snapshot from server 1\n"
        "           start persisting snapshot (2^2)\n");
    CLUSTER_STOP(2);
    CLUSTER_START(2);
    CLUSTER_TRACE("[ 150] 2 > term 3, voted for 1, 1 entry (1^1)\n");

    /* Server 1 eventually sends server 2 a heartbeat, which server 2 rejects.
     * At that point server 1 sends again the snapshot. */
    CLUSTER_TRACE(
        "[ 170] 1 > timeout as leader\n"
        "           missing previous entry at index 2 -> needs snapshot\n"
        "           snapshot server 2 sending a heartbeat (no entries)\n"
        "[ 180] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^2) -> reject\n"
        "[ 190] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 0 -> needs snapshot\n"
        "           sending snapshot (2^2) to server 2\n");

    return MUNIT_OK;
}

/* A follower receives an AppendEntries message while installing a snapshot . */
TEST(snapshot, ReceiveAppendEntriesWhileInstalling, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Set a very low threshold and trailing entries number on server 1. */
    CLUSTER_SET_SNAPSHOT_THRESHOLD(1 /* ID */, 2 /* n. entries */);
    CLUSTER_SET_SNAPSHOT_TRAILING(1 /* ID */, 1 /* n. entries */);

    raft_set_install_snapshot_timeout(CLUSTER_RAFT(1), 100);

    /* Prevent server 3 from receving messages from server 1. */
    CLUSTER_DISCONNECT(1, 3);

    /* Set a high disk latency on server 3, so it will take a while to
     * complete installing the snapshot. */
    CLUSTER_SET_DISK_LATENCY(3, 250);

    /* Increase the election timeout on server 3, so it won't convert to
     * candidate. */
    raft_set_election_timeout(CLUSTER_RAFT(3), 250);

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
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (3^2)\n"
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 3\n"
        "[ 130] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 2 entries (2^2..3^2)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 2 new entries (2^2..3^2)\n"
        "[ 160] 2 > persisted 2 entry (2^2..3^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           commit 2 new entries (2^2..3^2)\n"
        "[ 170] 1 > new snapshot (3^2), 1 trailing entry\n");

    /* Reconnect server 3 and wait for it to receive the snapshot. */
    CLUSTER_RECONNECT(1, 3);

    CLUSTER_TRACE(
        "[ 170] 1 > timeout as leader\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 180] 3 > recv append entries from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           missing previous entry (3^2) -> reject\n"
        "[ 190] 1 > recv append entries result from server 3\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (3^2) to server 3\n"
        "[ 190] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "[ 200] 3 > recv install snapshot from server 1\n"
        "           start persisting snapshot (3^2)\n");

    /* Apply a new entry, server 1 won't send it to server 3 since it is
     * waiting for it to complete installing the snapshot. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[ 200] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (4^2)\n"
        "           pipeline server 2 sending 1 entry (4^2)\n"
        "[ 200] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 210] 1 > persisted 1 entry (4^2)\n"
        "           next uncommitted entry (4^2) has 1 vote out of 3\n"
        "[ 210] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (4^2)\n"
        "[ 210] 1 > recv append entries result from server 2\n"
        "[ 220] 2 > persisted 1 entry (4^2)\n"
        "           send success result to 1\n"
        "[ 230] 1 > recv append entries result from server 2\n"
        "           commit 1 new entry (4^2)\n");

    /* Transfer leadership from server 0 to server 1. */
    test_cluster_transfer(&f->cluster_, 1, 2);
    CLUSTER_TRACE(
        "[ 230] 1 > transfer leadership to 2\n"
        "           send timeout to 2\n"
        "[ 240] 2 > recv timeout now from server 1\n"
        "           convert to candidate, start election for term 3\n"
        "[ 240] 1 > timeout as leader\n"
        "           missing previous entry at index 3 -> needs snapshot\n"
        "           snapshot server 3 sending a heartbeat (no entries)\n"
        "[ 250] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log is equal (4^2) -> grant vote\n"
        "[ 250] 3 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "[ 250] 3 > recv append entries from server 1\n"
        "           local term is higher (3 vs 2) -> reject\n"
        "[ 260] 2 > recv request vote result from server 1\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           replicate 1 new barrier entry (5^3)\n"
        "           probe server 1 sending 1 entry (5^3)\n"
        "           probe server 3 sending 1 entry (5^3)\n"
        "[ 260] 2 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n"
        "[ 260] 1 > recv append entries result from server 3\n"
        "           local server is not leader -> ignore\n"
        "[ 270] 2 > persisted 1 entry (5^3)\n"
        "           next uncommitted entry (4^2) has 1 vote out of 3\n"
        "[ 270] 1 > recv append entries from server 2\n"
        "           start persisting 1 new entry (5^3)\n"
        "[ 270] 3 > recv append entries from server 2\n"
        "           snapshot install in progress -> ignore\n");

    return MUNIT_OK;
}

/* An InstallSnapshot RPC arrives while persisting Entries */
TEST(snapshot, InstallDuringEntriesWrite, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    unsigned id;
    int rv;

    /* Set very low threshold and trailing entries number */
    CLUSTER_SET_SNAPSHOT_THRESHOLD(1 /* ID */, 3 /* n. entries */);
    CLUSTER_SET_SNAPSHOT_TRAILING(1 /* ID */, 0 /* n. entries */);

    /* Don't let server 2 time out (just for terser traces). */
    raft_set_election_timeout(CLUSTER_RAFT(2), 200);

    /* Bootstrap and start a cluster with 1 voter and 1 stand-by. Server 1 has
     * an additional entry that server 2 does not have. */
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

    /* Server 1 starts and eventually replicates the entry that server 2 is
     * missing. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "           self elect and convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[  10] 2 > recv append entries from server 1\n"
        "           missing previous entry (2^1) -> reject\n"
        "[  20] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           probe server 2 sending 1 entry (2^1)\n");

    /* Set a large disk latency on server 2, so later the InstallSnapshot
     * message will arrive while the entry is still being persisted. */
    CLUSTER_SET_DISK_LATENCY(2, 60);
    CLUSTER_TRACE(
        "[  30] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^1)\n");

    /* Apply an entry, to force a snapshot to be taken. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[  30] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (3^1)\n"
        "[  40] 1 > persisted 1 entry (3^1)\n"
        "           commit 1 new entry (3^1)\n"
        "[  40] 1 > new snapshot (3^1), 0 trailing entries\n");

    /* Eventually server 1 replicates the snapshot to server 2. The inital write
     * never gets fired because it's stale. */
    CLUSTER_TRACE(
        "[  70] 1 > timeout as leader\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (3^1) to server 2\n"
        "[  80] 2 > recv install snapshot from server 1\n"
        "           start persisting snapshot (3^1)\n"
        "[ 120] 1 > timeout as leader\n"
        "           server 2 is unreachable -> abort snapshot\n"
        "           missing previous entry at index 0 -> needs snapshot\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}

/* A new term starts while a node is installing a snapshot. */
TEST(snapshot, NewTermWhileInstalling, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Set very low threshold and trailing entries number */
    CLUSTER_SET_SNAPSHOT_THRESHOLD(1 /* ID */, 2 /* n. entries */);
    CLUSTER_SET_SNAPSHOT_TRAILING(1 /* ID */, 0 /* n. entries */);

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
        "           local server is leader -> ignore\n");

    /* Disconnect server 3, so it won't get any new entry. */
    CLUSTER_DISCONNECT(1, 3);

    /* Submit a new entry, to trigger a snapshot on server 1. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 3\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 160] 2 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           commit 1 new entry (2^2)\n"
        "[ 170] 1 > new snapshot (2^2), 0 trailing entries\n");

    /* Reconnect server 3, so it receive the snapshot. */
    CLUSTER_RECONNECT(1, 3);

    /* Set a very high disk latency so server 3 will take a lot of time to
     * install the snapshot and server 1 will have stepped down in the
     * meantime. */
    CLUSTER_SET_DISK_LATENCY(3, 250);

    CLUSTER_TRACE(
        "[ 170] 1 > timeout as leader\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 180] 3 > recv append entries from server 1\n"
        "           missing previous entry (2^2) -> reject\n"
        "[ 190] 1 > recv append entries result from server 3\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (2^2) to server 3\n"
        "[ 190] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "[ 200] 3 > recv install snapshot from server 1\n"
        "           start persisting snapshot (2^2)\n");

    /* Disconnect server 1 from server 2 and 3, so it will step down. */
    CLUSTER_SET_ELECTION_TIMEOUT(1 /* ID */, 20 /* timeout */, 0 /* delta */);
    CLUSTER_SET_ELECTION_TIMEOUT(2 /* ID */, 80 /* timeout */, 0 /* delta */);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(3, 1);
    CLUSTER_DISCONNECT(1, 3);

    CLUSTER_TRACE(
        "[ 210] 1 > timeout as leader\n"
        "           server 2 is unreachable -> abort pipeline\n"
        "[ 230] 1 > timeout as leader\n"
        "           server 3 is unreachable -> abort snapshot\n"
        "           unable to contact majority of cluster -> step down\n");

    /* Let server 2 win the elections */
    CLUSTER_RECONNECT(2, 1);
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_RECONNECT(1, 3);

    CLUSTER_TRACE(
        "[ 230] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 240] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (2^2) -> grant vote\n"
        "[ 240] 3 > recv request vote from server 2\n"
        "           local server has a leader (server 1) -> reject\n"
        "[ 250] 2 > recv request vote result from server 1\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           replicate 1 new barrier entry (3^3)\n"
        "           probe server 1 sending 1 entry (3^3)\n"
        "           probe server 3 sending 1 entry (3^3)\n"
        "[ 250] 2 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n"
        "[ 260] 2 > persisted 1 entry (3^3)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 3\n"
        "[ 260] 1 > recv append entries from server 2\n"
        "           start persisting 1 new entry (3^3)\n"
        "[ 260] 3 > recv append entries from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           snapshot install in progress -> ignore\n");

    return MUNIT_OK;
}
