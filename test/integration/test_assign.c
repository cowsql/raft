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

struct result
{
    int status;
    bool done;
};

/* Add a an empty server to the cluster and start it. */
#define GROW                                \
    {                                       \
        int rv__;                           \
        CLUSTER_GROW;                       \
        rv__ = raft_start(CLUSTER_RAFT(2)); \
        munit_assert_int(rv__, ==, 0);      \
    }

static void changeCbAssertResult(struct raft_change *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

static bool changeCbHasFired(struct raft_fixture *f, void *arg)
{
    struct result *result = arg;
    (void)f;
    return result->done;
}

/* Submit an add request. */
#define ADD_SUBMIT(I, ID)                                                     \
    struct raft_change _req;                                                  \
    char _address[16];                                                        \
    struct result _result = {0, false};                                       \
    int _rv;                                                                  \
    _req.data = &_result;                                                     \
    sprintf(_address, "%d", ID);                                              \
    _rv =                                                                     \
        raft_add(CLUSTER_RAFT(I), &_req, ID, _address, changeCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

#define ADD(I, ID)                                            \
    do {                                                      \
        ADD_SUBMIT(I, ID);                                    \
        CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 2000); \
    } while (0)

/* Submit an assign role request. */
#define ASSIGN_SUBMIT(I, ID, ROLE)                                             \
    struct raft_change _req;                                                   \
    struct result _result = {0, false};                                        \
    int _rv;                                                                   \
    _req.data = &_result;                                                      \
    _rv = raft_assign(CLUSTER_RAFT(I), &_req, ID, ROLE, changeCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

/* Expect the request callback to fire with the given status. */
#define ASSIGN_EXPECT(STATUS) _result.status = STATUS;

/* Wait until a promote request completes. */
#define ASSIGN_WAIT CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 10000)

/* Submit a request to assign the I'th server to the given role and wait for the
 * operation to succeed. */
#define ASSIGN(I, ID, ROLE)         \
    do {                            \
        ASSIGN_SUBMIT(I, ID, ROLE); \
        ASSIGN_WAIT;                \
    } while (0)

/* Invoke raft_assign() against the I'th server and assert it the given error
 * code. */
#define ASSIGN_ERROR(I, ID, ROLE, RV, ERRMSG)                        \
    {                                                                \
        struct raft_change __req;                                    \
        int __rv;                                                    \
        __rv = raft_assign(CLUSTER_RAFT(I), &__req, ID, ROLE, NULL); \
        munit_assert_int(__rv, ==, RV);                              \
        munit_assert_string_equal(ERRMSG, CLUSTER_ERRMSG(I));        \
    }

/******************************************************************************
 *
 * Set up a cluster of 2 servers, with the first as leader.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(2);
    if (!v1) {
        CLUSTER_BOOTSTRAP;
        CLUSTER_START();
        CLUSTER_ELECT(0);
    }
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

/* Assert the values of the committed and uncommitted configuration indexes on
 * the raft instance with the given index. */
#define ASSERT_CONFIGURATION_INDEXES(I, COMMITTED, UNCOMMITTED)                \
    {                                                                          \
        struct raft *raft_ = CLUSTER_RAFT(I);                                  \
        munit_assert_int(raft_->configuration_committed_index, ==, COMMITTED); \
        munit_assert_int(raft_->configuration_uncommitted_index, ==,           \
                         UNCOMMITTED);                                         \
    }

/* Assert that the state of the current catch up round matches the given
 * values. */
#define ASSERT_CATCH_UP_ROUND(I, PROMOTEED_ID, NUMBER, DURATION)              \
    {                                                                         \
        struct raft *raft_ = CLUSTER_RAFT(I);                                 \
        munit_assert_int(raft_->leader_state.promotee_id, ==, PROMOTEED_ID);  \
        munit_assert_int(raft_->leader_state.round_number, ==, NUMBER);       \
        munit_assert_int(                                                     \
            raft_->io->time(raft_->io) - raft_->leader_state.round_start, >=, \
            DURATION);                                                        \
    }

/******************************************************************************
 *
 * raft_assign
 *
 *****************************************************************************/

SUITE(raft_assign)

/* Trying to promote a server on a raft instance which is not the leader results
 * in an error. */
TEST_V1(raft_assign, NotLeader, setUp, tearDown, 0, NULL)
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

    event.time = f->cluster_.time;
    event.type = RAFT_SUBMIT;
    event.submit.n = 1;
    event.submit.entries = &entry;

    rv = raft_step(CLUSTER_RAFT(2), &event, &update);
    munit_assert_int(rv, ==, RAFT_NOTLEADER);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* If leadership is lost before the configuration change log entry for setting
 * the new server role is committed, the leader configuration gets rolled back
 * and the role of server being changed is reverted. */
TEST(raft_assign, leadershipLost, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    const struct raft_server *server;
    /* TODO: fix */
    return MUNIT_SKIP;
    GROW;
    ADD(0, 3);
    CLUSTER_STEP_N(2);

    ASSIGN_SUBMIT(0, 3, RAFT_VOTER);

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    ASSERT_CATCH_UP_ROUND(0, 0, 0, 0);
    ASSERT_CONFIGURATION_INDEXES(0, 2, 3);
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_int(server->role, ==, RAFT_VOTER);

    /* Lose leadership. */
    CLUSTER_DEPOSE;

    /* A new leader gets elected */
    CLUSTER_ELECT(1);
    CLUSTER_STEP_N(5);

    /* Server 3 is not being considered voting anymore. */
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_int(server->role, ==, RAFT_STANDBY);

    return MUNIT_OK;
}

/* Trying to assign the voter role to an unresponsive server eventually
 * fails. */
TEST(raft_assign, promoteUnresponsive, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_MAKE_PROGRESS;
    GROW;
    ADD(0, 3);

    ASSIGN_SUBMIT(0, 3, RAFT_VOTER);
    CLUSTER_KILL(2);

    ASSIGN_EXPECT(RAFT_NOCONNECTION);
    ASSIGN_WAIT;

    return MUNIT_OK;
}

/* Demote a voter node to stand-by. */
TEST(raft_assign, demoteToStandBy, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ASSIGN(0, 2, RAFT_STANDBY);
    return MUNIT_OK;
}

/* The leader can be demoted to stand-by and will no longer act as leader */
TEST(raft_assign, demoteLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ASSIGN_SUBMIT(0, 1, RAFT_STANDBY);
    munit_assert_int(CLUSTER_LEADER, ==, 0);
    ASSIGN_WAIT;
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    munit_assert_int(CLUSTER_LEADER, !=, 0);
    return MUNIT_OK;
}

/* The leader can be demoted to spare and will no longer act as leader */
TEST(raft_assign, demoteLeaderToSpare, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ASSIGN_SUBMIT(0, 1, RAFT_SPARE);
    munit_assert_int(CLUSTER_LEADER, ==, 0);
    ASSIGN_WAIT;
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    munit_assert_int(CLUSTER_LEADER, !=, 0);
    return MUNIT_OK;
}
