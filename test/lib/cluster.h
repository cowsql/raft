/* Setup and drive a test raft cluster. */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <stdlib.h>

#include "../../include/raft.h"
#include "../../include/raft/fixture.h"
#include "fsm.h"
#include "heap.h"
#include "macros.h"
#include "munit.h"
#include "snapshot.h"

#define TEST_CLUSTER_N_SERVERS 8

static bool v1 = false;

#define TEST_V1(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS)         \
    static void *setUp__##S##_##C(const MunitParameter params[], \
                                  void *user_data)               \
    {                                                            \
        v1 = true;                                               \
        return SETUP(params, user_data);                         \
    }                                                            \
    static void tearDown__##S##_##C(void *data)                  \
    {                                                            \
        TEAR_DOWN(data);                                         \
        v1 = false;                                              \
    }                                                            \
    TEST(S, C, setUp__##S##_##C, tearDown__##S##_##C, OPTIONS, PARAMS)

#define FIXTURE_CLUSTER                                     \
    FIXTURE_HEAP;                                           \
    union {                                                 \
        struct                                              \
        { /* v0 */                                          \
            struct raft_fsm fsms[RAFT_FIXTURE_MAX_SERVERS]; \
            struct raft_fixture cluster;                    \
        };                                                  \
        struct                                              \
        { /* v1 */                                          \
            struct test_cluster cluster_;                   \
        };                                                  \
    }

#define SETUP_CLUSTER(N)     \
    if (v1) {                \
        SETUP_CLUSTER_V1();  \
    } else {                 \
        SETUP_CLUSTER_V0(N); \
    }

#define SETUP_CLUSTER_V1() test_cluster_setup(params, &f->cluster_)

/* N is the default number of servers, but can be tweaked with the cluster-n
 * parameter. */
#define SETUP_CLUSTER_V0(DEFAULT_N)                                            \
    SET_UP_HEAP;                                                               \
    do {                                                                       \
        unsigned _n = DEFAULT_N;                                               \
        bool _pre_vote = false;                                                \
        int _fsm_version = 2;                                                  \
        unsigned _hb = 0;                                                      \
        unsigned _i;                                                           \
        int _rv;                                                               \
        if (munit_parameters_get(params, CLUSTER_N_PARAM) != NULL) {           \
            _n = atoi(munit_parameters_get(params, CLUSTER_N_PARAM));          \
        }                                                                      \
        if (munit_parameters_get(params, CLUSTER_PRE_VOTE_PARAM) != NULL) {    \
            _pre_vote =                                                        \
                atoi(munit_parameters_get(params, CLUSTER_PRE_VOTE_PARAM));    \
        }                                                                      \
        if (munit_parameters_get(params, CLUSTER_HEARTBEAT_PARAM) != NULL) {   \
            _hb = atoi(munit_parameters_get(params, CLUSTER_HEARTBEAT_PARAM)); \
        }                                                                      \
        if (munit_parameters_get(params, CLUSTER_FSM_VERSION_PARAM) != NULL) { \
            _fsm_version =                                                     \
                atoi(munit_parameters_get(params, CLUSTER_FSM_VERSION_PARAM)); \
        }                                                                      \
        munit_assert_int(_n, >, 0);                                            \
        _rv = raft_fixture_init(&f->cluster);                                  \
        munit_assert_int(_rv, ==, 0);                                          \
        for (_i = 0; _i < _n; _i++) {                                          \
            FsmInit(&f->fsms[_i], _fsm_version);                               \
            _rv = raft_fixture_grow(&f->cluster, &f->fsms[_i]);                \
            munit_assert_int(_rv, ==, 0);                                      \
        }                                                                      \
        for (_i = 0; _i < _n; _i++) {                                          \
            raft_set_pre_vote(raft_fixture_get(&f->cluster, _i), _pre_vote);   \
            if (_hb) {                                                         \
                raft_set_heartbeat_timeout(raft_fixture_get(&f->cluster, _i),  \
                                           _hb);                               \
            }                                                                  \
        }                                                                      \
    } while (0)

#define TEAR_DOWN_CLUSTER     \
    if (v1) {                 \
        TEAR_DOWN_CLUSTER_V1; \
    } else {                  \
        TEAR_DOWN_CLUSTER_V0; \
    }

#define TEAR_DOWN_CLUSTER_V1 test_cluster_tear_down(&f->cluster_)

#define TEAR_DOWN_CLUSTER_V0              \
    do {                                  \
        unsigned i;                       \
        raft_fixture_close(&f->cluster);  \
        for (i = 0; i < CLUSTER_N; i++) { \
            FsmClose(&f->fsms[i]);        \
        }                                 \
    } while (0);                          \
    TEAR_DOWN_HEAP;

/* Munit parameter for setting the number of servers */
#define CLUSTER_N_PARAM "cluster-n"

/* Munit parameter for setting the number of voting servers */
#define CLUSTER_N_VOTING_PARAM "cluster-n-voting"

/* Munit parameter for enabling pre-vote */
#define CLUSTER_PRE_VOTE_PARAM "cluster-pre-vote"

/* Munit parameter for setting HeartBeat timeout */
#define CLUSTER_HEARTBEAT_PARAM "cluster-heartbeat"

/* Munit parameter for setting fsm version */
#define CLUSTER_FSM_VERSION_PARAM "fsm-version"

/* Get the number of servers in the cluster. */
#define CLUSTER_N raft_fixture_n(&f->cluster)

/* Get the cluster time. */
#define CLUSTER_TIME raft_fixture_time(&f->cluster)

/* Index of the current leader, or CLUSTER_N if there's no leader. */
#define CLUSTER_LEADER raft_fixture_leader_index(&f->cluster)

/* True if the cluster has a leader. */
#define CLUSTER_HAS_LEADER CLUSTER_LEADER < CLUSTER_N

#define CLUSTER_RAFT(X) (v1 ? CLUSTER_RAFT_V1(X) : CLUSTER_RAFT_V0(X))

/* Get the struct raft object with the given ID. */
#define CLUSTER_RAFT_V1(ID) test_cluster_raft(&f->cluster_, ID)

/* Get the struct raft object of the I'th server. */
#define CLUSTER_RAFT_V0(I) raft_fixture_get(&f->cluster, I)

/* Get the state of the I'th server. */
#define CLUSTER_STATE(I) raft_state(raft_fixture_get(&f->cluster, I))

/* Get the current term of the I'th server. */
#define CLUSTER_TERM(I) raft_fixture_get(&f->cluster, I)->current_term

/* Get the struct fsm object of the I'th server. */
#define CLUSTER_FSM(I) &f->fsms[I]

/* Return the last applied index on the I'th server. */
#define CLUSTER_LAST_APPLIED(I) \
    raft_last_applied(raft_fixture_get(&f->cluster, I))

/* Return the ID of the server the I'th server has voted for. */
#define CLUSTER_VOTED_FOR(I) raft_fixture_voted_for(&f->cluster, I)

/* Return a description of the last error occurred on the I'th server. */
#define CLUSTER_ERRMSG(I) raft_errmsg(CLUSTER_RAFT(I))

/* Populate the given configuration with all servers in the fixture. All servers
 * will be voting. */
#define CLUSTER_CONFIGURATION(CONF)                                     \
    {                                                                   \
        int rv_;                                                        \
        rv_ = raft_fixture_configuration(&f->cluster, CLUSTER_N, CONF); \
        munit_assert_int(rv_, ==, 0);                                   \
    }

/* Bootstrap all servers in the cluster. All servers will be voting, unless the
 * cluster-n-voting parameter is used. */
#define CLUSTER_BOOTSTRAP                                                    \
    {                                                                        \
        unsigned n_ = CLUSTER_N;                                             \
        int rv_;                                                             \
        struct raft_configuration configuration;                             \
        if (munit_parameters_get(params, CLUSTER_N_VOTING_PARAM) != NULL) {  \
            n_ = atoi(munit_parameters_get(params, CLUSTER_N_VOTING_PARAM)); \
        }                                                                    \
        rv_ = raft_fixture_configuration(&f->cluster, n_, &configuration);   \
        munit_assert_int(rv_, ==, 0);                                        \
        rv_ = raft_fixture_bootstrap(&f->cluster, &configuration);           \
        munit_assert_int(rv_, ==, 0);                                        \
        raft_configuration_close(&configuration);                            \
    }

/* Bootstrap all servers in the cluster. Only the first N servers will be
 * voting. */
#define CLUSTER_BOOTSTRAP_N_VOTING(N)                                      \
    {                                                                      \
        int rv_;                                                           \
        struct raft_configuration configuration_;                          \
        rv_ = raft_fixture_configuration(&f->cluster, N, &configuration_); \
        munit_assert_int(rv_, ==, 0);                                      \
        rv_ = raft_fixture_bootstrap(&f->cluster, &configuration_);        \
        munit_assert_int(rv_, ==, 0);                                      \
        raft_configuration_close(&configuration_);                         \
    }

/* Start the server with the given ID, using the state persisted on its disk. */
#define CLUSTER_START_V1(ID) test_cluster_start(&f->cluster_, ID)

/* Start all servers in the test cluster. */
#define CLUSTER_START_V0()                    \
    {                                         \
        int rc;                               \
        rc = raft_fixture_start(&f->cluster); \
        munit_assert_int(rc, ==, 0);          \
    }

#define FUNC_CHOOSER(_f1, _f2, ...) _f2
#define FUNC_RECOMPOSER(argsWithParentheses) FUNC_CHOOSER argsWithParentheses

#define CLUSTER_START__ARG_COUNT(...) \
    FUNC_RECOMPOSER((__VA_ARGS__, CLUSTER_START_V1, ))

#define CLUSTER_START__NO_ARG() , CLUSTER_START_V0

#define CLUSTER_START__CHOOSER(...) \
    CLUSTER_START__ARG_COUNT(CLUSTER_START__NO_ARG __VA_ARGS__())

#define CLUSTER_START(...) CLUSTER_START__CHOOSER(__VA_ARGS__)(__VA_ARGS__)

#define CLUSTER_TRACE(EXPECTED)                        \
    if (!test_cluster_trace(&f->cluster_, EXPECTED)) { \
        munit_error("trace does not match");           \
    }

#define CLUSTER_ELAPSE(MSECS) test_cluster_elapse(&f->cluster_, MSECS)

/* Step the cluster. */
#define CLUSTER_STEP raft_fixture_step(&f->cluster);

/* Step the cluster N times. */
#define CLUSTER_STEP_N(N)                   \
    {                                       \
        unsigned i_;                        \
        for (i_ = 0; i_ < N; i_++) {        \
            raft_fixture_step(&f->cluster); \
        }                                   \
    }

/* Step until the given function becomes true. */
#define CLUSTER_STEP_UNTIL(FUNC, ARG, MSECS)                            \
    {                                                                   \
        bool done_;                                                     \
        done_ = raft_fixture_step_until(&f->cluster, FUNC, ARG, MSECS); \
        munit_assert_true(done_);                                       \
    }

/* Step the cluster until a leader is elected or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_ELAPSED(MSECS) \
    raft_fixture_step_until_elapsed(&f->cluster, MSECS)

/* Step the cluster until a leader is elected or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_HAS_LEADER(MAX_MSECS)                           \
    {                                                                      \
        bool done;                                                         \
        done = raft_fixture_step_until_has_leader(&f->cluster, MAX_MSECS); \
        munit_assert_true(done);                                           \
        munit_assert_true(CLUSTER_HAS_LEADER);                             \
    }

/* Step the cluster until there's no leader or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_HAS_NO_LEADER(MAX_MSECS)                           \
    {                                                                         \
        bool done;                                                            \
        done = raft_fixture_step_until_has_no_leader(&f->cluster, MAX_MSECS); \
        munit_assert_true(done);                                              \
        munit_assert_false(CLUSTER_HAS_LEADER);                               \
    }

/* Step the cluster until the given index was applied by the given server (or
 * all if N) or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_APPLIED(I, INDEX, MAX_MSECS)                        \
    {                                                                          \
        bool done;                                                             \
        done =                                                                 \
            raft_fixture_step_until_applied(&f->cluster, I, INDEX, MAX_MSECS); \
        munit_assert_true(done);                                               \
    }

/* Step the cluster until the state of the server with the given index matches
 * the given value, or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_STATE_IS(I, STATE, MAX_MSECS)               \
    {                                                                  \
        bool done;                                                     \
        done = raft_fixture_step_until_state_is(&f->cluster, I, STATE, \
                                                MAX_MSECS);            \
        munit_assert_true(done);                                       \
    }

/* Step the cluster until the term of the server with the given index matches
 * the given value, or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_TERM_IS(I, TERM, MAX_MSECS)                        \
    {                                                                         \
        bool done;                                                            \
        done =                                                                \
            raft_fixture_step_until_term_is(&f->cluster, I, TERM, MAX_MSECS); \
        munit_assert_true(done);                                              \
    }

/* Step the cluster until server I has voted for server J, or #MAX_MSECS have
 * elapsed. */
#define CLUSTER_STEP_UNTIL_VOTED_FOR(I, J, MAX_MSECS)                        \
    {                                                                        \
        bool done;                                                           \
        done =                                                               \
            raft_fixture_step_until_voted_for(&f->cluster, I, J, MAX_MSECS); \
        munit_assert_true(done);                                             \
    }

/* Step the cluster until all messages from server I to server J have been
 * delivered, or #MAX_MSECS elapse. */
#define CLUSTER_STEP_UNTIL_DELIVERED(I, J, MAX_MSECS)                        \
    {                                                                        \
        bool done;                                                           \
        done =                                                               \
            raft_fixture_step_until_delivered(&f->cluster, I, J, MAX_MSECS); \
        munit_assert_true(done);                                             \
    }

/* Request to apply an FSM command to add the given value to x. */
#define CLUSTER_APPLY_ADD_X(I, REQ, VALUE, CB)      \
    {                                               \
        struct raft_buffer buf_;                    \
        struct raft *raft_;                         \
        int rv_;                                    \
        FsmEncodeAddX(VALUE, &buf_);                \
        raft_ = raft_fixture_get(&f->cluster, I);   \
        rv_ = raft_apply(raft_, REQ, &buf_, 1, CB); \
        munit_assert_int(rv_, ==, 0);               \
    }

/* Kill the I'th server. */
#define CLUSTER_KILL(I) raft_fixture_kill(&f->cluster, I);

/* Stop the server with the given ID. */
#define CLUSTER_STOP(ID) test_cluster_stop(&f->cluster_, ID);

/* Revive the I'th server */
#define CLUSTER_REVIVE(I) raft_fixture_revive(&f->cluster, I);

/* Kill the leader. */
#define CLUSTER_KILL_LEADER CLUSTER_KILL(CLUSTER_LEADER)

/* Kill a majority of servers, except the leader (if there is one). */
#define CLUSTER_KILL_MAJORITY                                \
    {                                                        \
        size_t i2;                                           \
        size_t n;                                            \
        for (i2 = 0, n = 0; n < (CLUSTER_N / 2) + 1; i2++) { \
            if (i2 == CLUSTER_LEADER) {                      \
                continue;                                    \
            }                                                \
            CLUSTER_KILL(i2)                                 \
            n++;                                             \
        }                                                    \
    }

/* Grow the cluster adding one server. */
#define CLUSTER_GROW                                               \
    {                                                              \
        int rv_;                                                   \
        FsmInit(&f->fsms[CLUSTER_N], 2);                           \
        rv_ = raft_fixture_grow(&f->cluster, &f->fsms[CLUSTER_N]); \
        munit_assert_int(rv_, ==, 0);                              \
    }

/* Add a new pristine server to the cluster, connected to all others. Then
 * submit a request to add it to the configuration as an idle server. */
#define CLUSTER_ADD(REQ)                                               \
    {                                                                  \
        int rc;                                                        \
        struct raft *new_raft;                                         \
        CLUSTER_GROW;                                                  \
        rc = raft_start(CLUSTER_RAFT(CLUSTER_N - 1));                  \
        munit_assert_int(rc, ==, 0);                                   \
        new_raft = CLUSTER_RAFT(CLUSTER_N - 1);                        \
        rc = raft_add(CLUSTER_RAFT(CLUSTER_LEADER), REQ, new_raft->id, \
                      new_raft->address, NULL);                        \
        munit_assert_int(rc, ==, 0);                                   \
    }

/* Assign the given role to the server that was added last. */
#define CLUSTER_ASSIGN(REQ, ROLE)                                              \
    do {                                                                       \
        unsigned _id;                                                          \
        int _rv;                                                               \
        _id = CLUSTER_N; /* Last server that was added. */                     \
        _rv = raft_assign(CLUSTER_RAFT(CLUSTER_LEADER), REQ, _id, ROLE, NULL); \
        munit_assert_int(_rv, ==, 0);                                          \
    } while (0)

/* Ensure that the cluster can make progress from the current state.
 *
 * - If no leader is present, wait for one to be elected.
 * - Submit a request to apply a new FSM command and wait for it to complete. */
#define CLUSTER_MAKE_PROGRESS                                          \
    {                                                                  \
        struct raft_apply *req_ = munit_malloc(sizeof *req_);          \
        if (!(CLUSTER_HAS_LEADER)) {                                   \
            CLUSTER_STEP_UNTIL_HAS_LEADER(10000);                      \
        }                                                              \
        CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, req_, 1, NULL);            \
        CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, req_->index, 3000); \
        free(req_);                                                    \
    }

/* Elect the I'th server. */
#define CLUSTER_ELECT(I) raft_fixture_elect(&f->cluster, I)

/* Start to elect the I'th server. */
#define CLUSTER_START_ELECT(I) raft_fixture_start_elect(&f->cluster, I)

/* Depose the current leader */
#define CLUSTER_DEPOSE raft_fixture_depose(&f->cluster)

/* Disconnect I from J. */
#define CLUSTER_DISCONNECT(A, B)     \
    if (v1) {                        \
        CLUSTER_DISCONNECT_V1(A, B); \
    } else {                         \
        CLUSTER_DISCONNECT_V0(A, B); \
    }

#define CLUSTER_DISCONNECT_V1(ID1, ID2) \
    test_cluster_disconnect(&f->cluster_, ID1, ID2)

#define CLUSTER_DISCONNECT_V0(I, J) raft_fixture_disconnect(&f->cluster, I, J)

/* Reconnect I to J. */
#define CLUSTER_RECONNECT(A, B)     \
    if (v1) {                       \
        CLUSTER_RECONNECT_V1(A, B); \
    } else {                        \
        CLUSTER_RECONNECT_V0(A, B); \
    }

#define CLUSTER_RECONNECT_V1(ID1, ID2) \
    test_cluster_reconnect(&f->cluster_, ID1, ID2)

#define CLUSTER_RECONNECT_V0(I, J) raft_fixture_reconnect(&f->cluster, I, J)

/* Saturate the connection from I to J. */
#define CLUSTER_SATURATE(I, J) raft_fixture_saturate(&f->cluster, I, J)

/* Saturate the connection from I to J and from J to I, in both directions. */
#define CLUSTER_SATURATE_BOTHWAYS(I, J) \
    CLUSTER_SATURATE(I, J);             \
    CLUSTER_SATURATE(J, I)

/* Desaturate the connection between I and J, making messages flow again. */
#define CLUSTER_DESATURATE(I, J) raft_fixture_desaturate(&f->cluster, I, J)

/* Reconnect two servers. */
#define CLUSTER_DESATURATE_BOTHWAYS(I, J) \
    CLUSTER_DESATURATE(I, J);             \
    CLUSTER_DESATURATE(J, I)

/* Set the network latency of outgoing messages of server I. */
#define CLUSTER_SET_NETWORK_LATENCY(I, MSECS)                     \
    if (v1) {                                                     \
        test_cluster_set_network_latency(&f->cluster_, I, MSECS); \
    } else {                                                      \
        raft_fixture_set_network_latency(&f->cluster, I, MSECS);  \
    }

/* Set the disk I/O latency of server I. */
#define CLUSTER_SET_DISK_LATENCY(I, MSECS)                     \
    if (v1) {                                                  \
        test_cluster_set_disk_latency(&f->cluster_, I, MSECS); \
    } else {                                                   \
        raft_fixture_set_disk_latency(&f->cluster, I, MSECS);  \
    }

#define CLUSTER_SET_TERM(...)             \
    if (v1) {                             \
        CLUSTER_SET_TERM_V1(__VA_ARGS__); \
    } else {                              \
        CLUSTER_SET_TERM_V0(__VA_ARGS__); \
    }

#define CLUSTER_SET_VOTE(ID, VOTE) \
    test_cluster_set_vote(&f->cluster_, ID, VOTE);

#define CLUSTER_SET_ELECTION_TIMEOUT(ID, TIMEOUT, DELTA) \
    test_cluster_set_election_timeout(&f->cluster_, ID, TIMEOUT, DELTA)

/* Set the term persisted on the I'th server. This must be called before
 * starting the cluster. */
#define CLUSTER_SET_TERM_V0(I, TERM) raft_fixture_set_term(&f->cluster, I, TERM)

/* Set the persisted term of the server with the given ID. Must me called before
 * starting the server. */
#define CLUSTER_SET_TERM_V1(ID, TERM) \
    test_cluster_set_term(&f->cluster_, ID, TERM)

#define CLUSTER_SET_SNAPSHOT(...)             \
    if (v1) {                                 \
        CLUSTER_SET_SNAPSHOT_V1(__VA_ARGS__); \
    } else {                                  \
        CLUSTER_SET_SNAPSHOT_V0(__VA_ARGS__); \
    }

/* Helper to populate a struct raft_configuration object CONF, adding N servers
 * to it, among which N_VOTERS are voters and N_STANDBYS are standbys. */
#define CLUSTER_FILL_CONFIGURATION(CONF, N, N_VOTERS, N_STANDBYS)             \
    do {                                                                      \
        unsigned _i;                                                          \
        int __rv;                                                             \
        munit_assert_int(N, >=, 1);                                           \
        munit_assert_int(N_VOTERS, <=, N);                                    \
        raft_configuration_init(CONF);                                        \
        for (_i = 0; _i < N; _i++) {                                          \
            raft_id _id = _i + 1;                                             \
            int _role = RAFT_SPARE;                                           \
            char _address[64];                                                \
            if (_i < N_VOTERS) {                                              \
                _role = RAFT_VOTER;                                           \
            } else if (N_STANDBYS > 0 && (int)(_i - N_VOTERS) < N_STANDBYS) { \
                _role = RAFT_STANDBY;                                         \
            }                                                                 \
            sprintf(_address, "%llu", _id);                                   \
            __rv = raft_configuration_add(CONF, _id, _address, _role);        \
            munit_assert_int(__rv, ==, 0);                                    \
        }                                                                     \
    } while (0)

/* Set the persisted snapshot of the server with the given ID. Must me called
 * before starting the server. */
#define CLUSTER_SET_SNAPSHOT_V1(ID, INDEX, TERM, CONF_N, CONF_N_VOTING,        \
                                CONF_INDEX)                                    \
    do {                                                                       \
        struct test_snapshot *_snapshot = munit_malloc(sizeof *_snapshot);     \
        _snapshot->metadata.index = INDEX;                                     \
        _snapshot->metadata.term = TERM;                                       \
        CLUSTER_FILL_CONFIGURATION(&_snapshot->metadata.configuration, CONF_N, \
                                   CONF_N_VOTING, 0);                          \
        _snapshot->metadata.configuration_index = CONF_INDEX;                  \
        _snapshot->data.len = 8;                                               \
        _snapshot->data.base = munit_malloc(_snapshot->data.len);              \
        test_cluster_set_snapshot(&f->cluster_, ID, _snapshot);                \
    } while (0)

/* Set the snapshot persisted on the I'th server. This must be called before
 * starting the cluster. */
#define CLUSTER_SET_SNAPSHOT_V0(I, LAST_INDEX, LAST_TERM, CONF_INDEX, X, Y) \
    {                                                                       \
        struct raft_configuration configuration_;                           \
        struct raft_snapshot *snapshot_;                                    \
        CLUSTER_CONFIGURATION(&configuration_);                             \
        CREATE_SNAPSHOT(snapshot_, LAST_INDEX, LAST_TERM, configuration_,   \
                        CONF_INDEX, X, Y);                                  \
        raft_fixture_set_snapshot(&f->cluster, I, snapshot_);               \
    }

#define CLUSTER_ADD_ENTRY__CHOOSER(...)                                  \
    GET_5TH_ARG(__VA_ARGS__, CLUSTER_ADD_ENTRY_V1, CLUSTER_ADD_ENTRY_V1, \
                CLUSTER_ADD_ENTRY_RAW, )

#define CLUSTER_ADD_ENTRY(...) \
    CLUSTER_ADD_ENTRY__CHOOSER(__VA_ARGS__)(__VA_ARGS__)

#define CLUSTER_ADD_ENTRY_V1(ID, TYPE, ...) \
    CLUSTER_ADD_ENTRY__##TYPE(ID, __VA_ARGS__)

#define CLUSTER_ADD_ENTRY__RAFT_CHANGE(ID, CONF_N, CONF_N_VOTING)              \
    do {                                                                       \
        struct raft_configuration _configuration;                              \
        struct raft_entry _entry;                                              \
        int _rv;                                                               \
                                                                               \
        CLUSTER_FILL_CONFIGURATION(&_configuration, CONF_N, CONF_N_VOTING, 0); \
        _entry.type = RAFT_CHANGE;                                             \
        _entry.term = 1;                                                       \
        _rv = raft_configuration_encode(&_configuration, &_entry.buf);         \
        munit_assert_int(_rv, ==, 0);                                          \
        raft_configuration_close(&_configuration);                             \
                                                                               \
        test_cluster_add_entry(&f->cluster_, ID, &_entry);                     \
                                                                               \
        raft_free(_entry.buf.base);                                            \
    } while (0);

#define CLUSTER_ADD_ENTRY__RAFT_COMMAND(ID, TERM, PAYLOAD) \
    do {                                                   \
        uint64_t _payload = PAYLOAD;                       \
        struct raft_entry _entry;                          \
                                                           \
        _entry.type = RAFT_COMMAND;                        \
        _entry.term = TERM;                                \
        _entry.buf.base = &_payload;                       \
        _entry.buf.len = sizeof _payload;                  \
        test_cluster_add_entry(&f->cluster_, ID, &_entry); \
    } while (0);

/* Add an entry to the ones persisted on the I'th server. This must be called
 * before starting the cluster. */
#define CLUSTER_ADD_ENTRY_RAW(I, ENTRY)                 \
    if (v1) {                                           \
        test_cluster_add_entry(&f->cluster_, I, ENTRY); \
    } else {                                            \
        raft_fixture_add_entry(&f->cluster, I, ENTRY);  \
    }

/* Make an I/O error occur on the I'th server after @DELAY operations. */
#define CLUSTER_IO_FAULT(I, DELAY, REPEAT) \
    raft_fixture_io_fault(&f->cluster, I, DELAY, REPEAT)

/* Return the number of messages sent by the given server. */
#define CLUSTER_N_SEND(I, TYPE) raft_fixture_n_send(&f->cluster, I, TYPE)

/* Return the number of messages sent by the given server. */
#define CLUSTER_N_RECV(I, TYPE) raft_fixture_n_recv(&f->cluster, I, TYPE)

/* Set a fixture hook that randomizes election timeouts, disk latency and
 * network latency. */
#define CLUSTER_RANDOMIZE                \
    cluster_randomize_init(&f->cluster); \
    raft_fixture_hook(&f->cluster, cluster_randomize)

void cluster_randomize_init(struct raft_fixture *f);
void cluster_randomize(struct raft_fixture *f,
                       struct raft_fixture_event *event);

/* Test snapshot that is just persisted in-memory. */
struct test_snapshot
{
    struct raft_snapshot_metadata metadata;
    struct raft_buffer data;
};

/* Persisted state of a single node.
 *
 * The data contained in this struct is passed to raft_step() as RAFT_START
 * event when starting a server, and is updated as the server makes progress. */
struct test_disk
{
    raft_term term;
    raft_id voted_for;
    struct test_snapshot *snapshot;
    raft_index start_index;
    struct raft_entry *entries;
    unsigned n_entries;
};

/* Wrap a @raft instance and maintain disk and network state. */
struct test_cluster;
struct test_server
{
    struct test_disk disk;        /* Persisted data */
    struct raft_tracer tracer;    /* Custom tracer */
    struct raft raft;             /* Raft instance */
    struct test_cluster *cluster; /* Parent cluster */
    raft_time timeout;            /* Next scheduled timeout */
    unsigned network_latency;     /* Network latency */
    unsigned disk_latency;        /* Disk latency */
    bool running;                 /* Whether the server is running */

    /* The randomized_election_timeout field stores the value that the raft
     * instance will obtain the next time it calls RandomWithinRange() to obtain
     * a random number in the [election_timeout, election_timeout * 2] range. We
     * do that by passing raft_seed() a value that makes the pseudor random
     * number generator produce exactly randomized_election_timeout. That value
     * is what we store in the seed field below. Since calculating the seed that
     * matches the desired randomized_election_timeout is somehow expensive, we
     * also use randomized_election_timeout_prev to store the previous value of
     * randomized_election_timeout, in order to re-use the same seed if nothing
     * has changed.
     *
     * See serverSeed for more details. */
    unsigned randomized_election_timeout;
    unsigned randomized_election_timeout_prev;
    unsigned seed;
};

/* Cluster of test raft servers instances with fake disk and network I/O. */
struct test_cluster
{
    struct test_server servers[TEST_CLUSTER_N_SERVERS]; /* Cluster servers */
    raft_time time;                                     /* Global time */
    bool in_tear_down;                                  /* Tearing down */
    char trace[8192];                                   /* Captured messages */
    void *operations[2];                                /* In-flight I/O */
    void *disconnect[2];                                /* Network faults */
};

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c);
void test_cluster_tear_down(struct test_cluster *c);

/* Return the raft object with the given @id. */
struct raft *test_cluster_raft(struct test_cluster *c, raft_id id);

/* Set the persisted term of the given server to the given value. Must me called
 * before starting the server. */
void test_cluster_set_term(struct test_cluster *c, raft_id id, raft_term term);

/* Set the persisted vote of the given server to the given value. Must me called
 * before starting the server. */
void test_cluster_set_vote(struct test_cluster *c, raft_id id, raft_id term);

/* Set the last persisted snapshot of the given server. Must me called before
 * starting the server. */
void test_cluster_set_snapshot(struct test_cluster *c,
                               raft_id id,
                               struct test_snapshot *snapshot);

void test_cluster_add_entry(struct test_cluster *c,
                            raft_id id,
                            const struct raft_entry *entry);

/* Set a custom election timeout for the given server. Must me called
 * before starting the server. The randomized timeout will be set to timeout +
 * delta. */
void test_cluster_set_election_timeout(struct test_cluster *c,
                                       raft_id id,
                                       unsigned timeout,
                                       unsigned delta);

/* Set the network latency of messages sent by the given server. */
void test_cluster_set_network_latency(struct test_cluster *c,
                                      raft_id id,
                                      unsigned latency);

/* Set the network latency of disk writes issued by the given server. */
void test_cluster_set_disk_latency(struct test_cluster *c,
                                   raft_id id,
                                   unsigned latency);

/* Start the server with the given @id, using the current state persisted on its
 * disk. */
void test_cluster_start(struct test_cluster *c, raft_id id);

/* Stop the server with the given @id. */
void test_cluster_stop(struct test_cluster *c, raft_id id);

/* Submit a new entry. */
void test_cluster_submit(struct test_cluster *c,
                         raft_id id,
                         struct raft_entry *entry);

/* Fire a leadership transfer. */
void test_cluster_transfer(struct test_cluster *c,
                           raft_id id,
                           raft_id transferee);

/* Advance the cluster by completing a single asynchronous operation or firing a
 * timeout. */
void test_cluster_step(struct test_cluster *c);

/* Let the given number of milliseconds elapse. This requires that no event
 * would be triggered by test_cluster_step() in the given time window. */
void test_cluster_elapse(struct test_cluster *c, unsigned msecs);

/* Stop delivering messages from id1 to id2 */
void test_cluster_disconnect(struct test_cluster *c, raft_id id1, raft_id id2);

/* Resume delivering messages from id1 to id2 */
void test_cluster_reconnect(struct test_cluster *c, raft_id id1, raft_id id2);

/* Crash a server and stop running it. */
void test_cluster_kill(struct test_cluster *c, raft_id id);

/* Compare the trace of all messages emitted by all servers with the given
 * expected trace. If they don't match, print the last line which differs and
 * return #false. */
bool test_cluster_trace(struct test_cluster *c, const char *expected);

#endif /* TEST_CLUSTER_H */
