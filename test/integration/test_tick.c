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

SUITE(tick)

/* If the election timeout expires, the follower is a voting server, and it
 * hasn't voted yet in this term, then become candidate and start a new
 * election. */
TEST(tick, ConvertToCandidate, setUp, tearDown, 0, NULL)
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

    /* Server 1 becomes leader */
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

    /* Stop server 1, eventually server 2 converts to candidate. */
    CLUSTER_STOP(1);

    CLUSTER_TRACE(
        "[ 240] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    /* The term has been incremeted. */
    raft = CLUSTER_RAFT(2);
    munit_assert_ullong(raft_current_term(raft), ==, 3);

    /* We have voted for ouselves. */
    munit_assert_ullong(raft_voted_for(raft), ==, 2);

    /* We are candidate */
    munit_assert_int(raft_state(raft), ==, RAFT_CANDIDATE);

    /* The vote results array is initialized */
    munit_assert_ptr_not_null(raft->candidate_state.votes);
    munit_assert_false(raft->candidate_state.votes[0].grant);
    munit_assert_true(raft->candidate_state.votes[1].grant);

    return MUNIT_OK;
}

static char *elapse_non_voter_n_voting[] = {"1", NULL};

static MunitParameterEnum elapse_non_voter_params[] = {
    {"n_voting", elapse_non_voter_n_voting},
    {NULL, NULL},
};

/* If the election timeout has elapsed, but we're not part of the current
 * configuration, stay follower. */
TEST(tick, NotInCurrentConfiguration, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_SET_TERM(2, 1 /* term */);
    CLUSTER_ADD_ENTRY(2, RAFT_CHANGE, 1 /* servers */, 1 /* voters */);
    CLUSTER_START(2);

    CLUSTER_TRACE(
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 130] 2 > timeout as follower\n"
        "           server not in current configuration -> stay follower\n");

    munit_assert_int(raft_state(CLUSTER_RAFT(2)), ==, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* If the election timeout has elapsed, but we're not voters, stay follower. */
TEST(tick, NotVoter, setUp, tearDown, 0, elapse_non_voter_params)
{
    struct fixture *f = data;
    CLUSTER_SET_TERM(2, 1 /* term */);
    CLUSTER_ADD_ENTRY(2, RAFT_CHANGE, 2 /* servers */, 1 /* voters */);
    CLUSTER_START(2);

    CLUSTER_TRACE(
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 130] 2 > timeout as follower\n"
        "           spare server -> stay follower\n");

    munit_assert_int(raft_state(CLUSTER_RAFT(2)), ==, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* If we're leader and an election timeout elapses without hearing from a
 * majority of the cluster, step down. */
TEST(tick, StepDownIfNoContact, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }

    /* Server 1 becomes leader */
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

    /* Stop server 2, eventually server 1 steps down. */
    CLUSTER_STOP(2);
    CLUSTER_TRACE(
        "[ 170] 1 > timeout as leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 220] 1 > timeout as leader\n"
        "           unable to contact majority of cluster -> step down\n");

    return MUNIT_OK;
}

/* If we're candidate and the election timeout has elapsed, start a new
 * election. */
TEST(tick, NewElection, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    CLUSTER_SET_TERM(1, 1 /* term */);
    CLUSTER_ADD_ENTRY(1, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_START(1);

    /* Server 1 becomes candidate. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    CLUSTER_TRACE(
        "[ 200] 1 > timeout as candidate\n"
        "           stay candidate, start election for term 3\n");

    return MUNIT_OK;
}
