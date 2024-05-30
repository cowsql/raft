#include "../lib/cluster.h"
#include "../lib/runner.h"

#define N_SERVERS 3

/******************************************************************************
 *
 * Fixture with a test raft cluster.
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

#define STEP_N(N) raft_fixture_step_n(&f->cluster, N)

/******************************************************************************
 *
 * Set up a cluster with a three servers.
 *
 *****************************************************************************/

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
 * raft_voter_contacts
 *
 *****************************************************************************/

SUITE(raft_voter_contacts)

TEST(raft_voter_contacts, upToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    for (unsigned id = 1; id <= N_SERVERS; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, N_SERVERS /* servers */,
                          N_SERVERS /* voters */);
        CLUSTER_START(id);
    }

    /* No communication yet, so all counts should be 0 */
    for (unsigned id = 1; id <= N_SERVERS; id++) {
        int count = raft_voter_contacts(CLUSTER_RAFT(id));
        munit_assert_int(count, ==, -1);
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
        "           local server is leader -> ignore\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "[ 170] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n");

    /* Cluster election has succeeded and heartbeats have been sent, so the
     * leader should know about all of the followers. */
    for (unsigned id = 1; id <= N_SERVERS; id++) {
        int count = raft_voter_contacts(CLUSTER_RAFT(id));
        if (id == 1 /* leader */) {
            munit_assert_int(count, ==, N_SERVERS);
        } else {
            munit_assert_int(count, ==, -1);
        }
    }

    /* Stop the cluster leader, so a new leader is elected and the number of
     * voters should be decreased */
    CLUSTER_STOP(1);

    /* Run until a leader is elected */
    CLUSTER_TRACE(
        "[ 260] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 270] 3 > recv request vote from server 2\n"
        "           local server has a leader (server 1) -> reject\n"
        "[ 280] 2 > recv request vote result from server 3\n"
        "           remote term is lower (2 vs 3) -> ignore\n"
        "[ 290] 3 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 300] 2 > recv request vote from server 3\n"
        "           already voted for server 2 -> don't grant vote\n"
        "[ 310] 3 > recv request vote result from server 2\n"
        "           vote not granted\n"
        "[ 390] 2 > timeout as candidate\n"
        "           stay candidate, start election for term 4\n"
        "[ 400] 3 > recv request vote from server 2\n"
        "           remote term is higher (4 vs 3) -> bump term, step down\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 410] 2 > recv request vote result from server 3\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 1 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n");

    /* Leader hasn't had a timeout yet, so it hasn't calculated the voter count
     * yet. This could potentially be improved, but it's a very small period of
     * time where the incorrect value will be available. */
    for (unsigned id = 1; id < N_SERVERS; id++) {
        int count = raft_voter_contacts(CLUSTER_RAFT(id));
        if (id == 2 /* leader */) {
            munit_assert_int(count, ==, 1);
        } else {
            munit_assert_int(count, ==, -1);
        }
    }

    /* Continue until leader timeout */
    CLUSTER_TRACE(
        "[ 420] 3 > recv append entries from server 2\n"
        "           no new entries to persist\n"
        "[ 430] 2 > recv append entries result from server 3\n"
        "[ 460] 2 > timeout as leader\n"
        "           probe server 1 sending a heartbeat (no entries)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n");

    for (unsigned id = 1; id < N_SERVERS; id++) {
        int count = raft_voter_contacts(CLUSTER_RAFT(id));
        if (id == 2 /* leader */) {
            munit_assert_int(count, ==, N_SERVERS - 1);
        } else {
            munit_assert_int(count, ==, -1);
        }
    }

    /* Move on by 10ms to make the node 1 restart distinct in time from the
     * previous trace */
    CLUSTER_ELAPSE(10);

    /* Revive the old leader, so the count should go back up */
    CLUSTER_START(1);

    CLUSTER_TRACE(
        "[ 470] 1 > term 2, voted for 1, 1 entry (1^1)\n"
        "[ 470] 1 > recv append entries from server 2\n"
        "           remote term is higher (4 vs 2) -> bump term\n"
        "           no new entries to persist\n"
        "[ 470] 3 > recv append entries from server 2\n"
        "           no new entries to persist\n"
        "[ 480] 2 > recv append entries result from server 1\n"
        "[ 480] 2 > recv append entries result from server 3\n"
        "[ 510] 2 > timeout as leader\n"
        "           pipeline server 1 sending a heartbeat (no entries)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n");

    for (unsigned id = 1; id < N_SERVERS; id++) {
        int count = raft_voter_contacts(CLUSTER_RAFT(id));
        if (id == 2 /* leader */) {
            munit_assert_int(count, ==, N_SERVERS);
        } else {
            munit_assert_int(count, ==, -1);
        }
    }

    return MUNIT_OK;
}
