/* Setup and drive a test raft cluster. */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <stdlib.h>

#include "../../include/raft.h"
#include "fsm.h"
#include "heap.h"
#include "macros.h"
#include "munit.h"
#include "snapshot.h"

#define FIXTURE_CLUSTER \
    FIXTURE_HEAP;       \
    struct test_cluster cluster_

#define SETUP_CLUSTER() test_cluster_setup(params, &f->cluster_)

#define TEAR_DOWN_CLUSTER() test_cluster_tear_down(&f->cluster_)

/* Start the server with the given ID, using the state persisted on its disk. */
#define CLUSTER_START(ID) test_cluster_start(&f->cluster_, ID)

/* Stop the server with the given ID. */
#define CLUSTER_STOP(ID) test_cluster_stop(&f->cluster_, ID);

/* Step the cluster until the all expected output is consumed. Fail the test if
 * a mismatch is found. */
#define CLUSTER_TRACE(EXPECTED)                        \
    if (!test_cluster_trace(&f->cluster_, EXPECTED)) { \
        munit_error("trace does not match");           \
    }

/* Step the cluster until the given amount of milliseconds has elapsed. */
#define CLUSTER_ELAPSE(MSECS) test_cluster_elapse(&f->cluster_, MSECS)

/* Disconnect the server with ID1 with the one with ID2. */
#define CLUSTER_DISCONNECT(ID1, ID2) \
    test_cluster_disconnect(&f->cluster_, ID1, ID2)

/* Reconnect two servers. */
#define CLUSTER_RECONNECT(ID1, ID2) \
    test_cluster_reconnect(&f->cluster_, ID1, ID2)

#define CLUSTER_SUBMIT__CHOOSER(...)                                     \
    GET_6TH_ARG(__VA_ARGS__, CLUSTER_SUBMIT__TYPE, CLUSTER_SUBMIT__TYPE, \
                CLUSTER_SUBMIT__TYPE, CLUSTER_SUBMIT__RAW, )

/* Submit an entry */
#define CLUSTER_SUBMIT(...) CLUSTER_SUBMIT__CHOOSER(__VA_ARGS__)(__VA_ARGS__)

#define CLUSTER_SUBMIT__TYPE(ID, TYPE, ...) \
    CLUSTER_SUBMIT__##TYPE(ID, __VA_ARGS__)

#define CLUSTER_SUBMIT__CHANGE(ID, N, N_VOTING, N_STANDBYS)          \
    do {                                                             \
        struct raft_entry entry_;                                    \
        struct raft_configuration conf_;                             \
        int rv_;                                                     \
        CLUSTER_FILL_CONFIGURATION(&conf_, N, N_VOTING, N_STANDBYS); \
        entry_.type = RAFT_CHANGE;                                   \
        entry_.term = raft_current_term(CLUSTER_RAFT(ID));           \
        rv_ = raft_configuration_encode(&conf_, &entry_.buf);        \
        munit_assert_int(rv_, ==, 0);                                \
        raft_configuration_close(&conf_);                            \
        entry_.batch = entry_.buf.base;                              \
        CLUSTER_SUBMIT__RAW(ID, &entry_);                            \
    } while (0)

#define CLUSTER_SUBMIT__COMMAND(ID, SIZE)                  \
    do {                                                   \
        struct raft_entry entry_;                          \
        entry_.type = RAFT_COMMAND;                        \
        entry_.term = raft_current_term(CLUSTER_RAFT(ID)); \
        entry_.buf.len = SIZE;                             \
        entry_.buf.base = raft_malloc(entry_.buf.len);     \
        munit_assert_not_null(entry_.buf.base);            \
        entry_.batch = entry_.buf.base;                    \
        CLUSTER_SUBMIT__RAW(ID, &entry_);                  \
    } while (0)

#define CLUSTER_SUBMIT__RAW(ID, ENTRY)                       \
    do {                                                     \
        int rv__;                                            \
        rv__ = test_cluster_submit(&f->cluster_, ID, ENTRY); \
        munit_assert_int(rv__, ==, 0);                       \
    } while (0)

/* Set the persisted vote of the server with the given ID. Must me called before
 * starting the server. */
#define CLUSTER_SET_VOTE(ID, VOTE) \
    test_cluster_set_vote(&f->cluster_, ID, VOTE);

/* Set the persisted term of the server with the given ID. Must me called before
 * starting the server. */
#define CLUSTER_SET_TERM(ID, TERM) test_cluster_set_term(&f->cluster_, ID, TERM)

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
#define CLUSTER_SET_SNAPSHOT(ID, INDEX, TERM, CONF_N, CONF_N_VOTING,           \
                             CONF_INDEX)                                       \
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

#define CLUSTER_ADD_ENTRY__CHOOSER(...)                                        \
    GET_5TH_ARG(__VA_ARGS__, CLUSTER_ADD_ENTRY__TYPE, CLUSTER_ADD_ENTRY__TYPE, \
                CLUSTER_ADD_ENTRY__RAW, )

#define CLUSTER_ADD_ENTRY(...) \
    CLUSTER_ADD_ENTRY__CHOOSER(__VA_ARGS__)(__VA_ARGS__)

#define CLUSTER_ADD_ENTRY__TYPE(ID, TYPE, ...) \
    CLUSTER_ADD_ENTRY__##TYPE(ID, __VA_ARGS__)

/* Add an entry to the ones persisted on the server with the given ID. This must
 * be called before starting the cluster. */
#define CLUSTER_ADD_ENTRY__RAW(ID, ENTRY) \
    test_cluster_add_entry(&f->cluster_, ID, ENTRY)

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
        CLUSTER_ADD_ENTRY__RAW(ID, &_entry);                                   \
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
        CLUSTER_ADD_ENTRY__RAW(ID, &_entry);               \
    } while (0);

/* Return the struct raft object with the given ID. */
#define CLUSTER_RAFT(ID) test_cluster_raft(&f->cluster_, ID)

/* Set the snapshot threshold of the server with the given ID. */
#define CLUSTER_SET_SNAPSHOT_THRESHOLD(ID, THRESHOLD) \
    test_cluster_set_snapshot_threshold(&f->cluster_, ID, THRESHOLD)

/* Set the trailing entries to keep after taking a snapshot on the server with
 * the given ID. */
#define CLUSTER_SET_SNAPSHOT_TRAILING(ID, TRAILING) \
    test_cluster_set_snapshot_trailing(&f->cluster_, ID, TRAILING)

/* Set the network latency for outgoing messages sent by the server with the
 * given ID. */
#define CLUSTER_SET_NETWORK_LATENCY(ID, MSECS) \
    test_cluster_set_network_latency(&f->cluster_, ID, MSECS)

/* Set the disk I/O latency of server I. */
#define CLUSTER_SET_DISK_LATENCY(ID, MSECS) \
    test_cluster_set_disk_latency(&f->cluster_, ID, MSECS)

#define CLUSTER_SET_ELECTION_TIMEOUT(ID, TIMEOUT, DELTA) \
    test_cluster_set_election_timeout(&f->cluster_, ID, TIMEOUT, DELTA)

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
    unsigned short size; /* Disk size in bytes */
};

/* Wrap a @raft instance and maintain disk and network state. */
struct test_cluster;
struct test_server
{
    struct test_disk disk;        /* Persisted data */
    struct raft_tracer tracer;    /* Custom tracer */
    struct raft raft;             /* Raft instance */
    struct test_cluster *cluster; /* Parent cluster */
    raft_index last_applied;      /* Last processed committed index. */
    raft_time timeout;            /* Next scheduled timeout */
    unsigned network_latency;     /* Network latency */
    unsigned disk_latency;        /* Disk latency */
    char address[8];              /* Server address */
    struct
    {
        unsigned threshold; /* Number of entries before taking a snapshot. */
        unsigned trailing;  /* Number of entries to leave after a snapshot. */
        bool installing;    /* True if installing a snapshot */
    } snapshot;
    bool running; /* Whether the server is running */

    struct
    {
        raft_index start;
        struct raft_entry *entries;
        unsigned n;
    } log; /* Cache of the on-disk log. */

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

#define TEST_CLUSTER_N_SERVERS 8

/* Cluster of test raft servers instances with fake disk and network I/O. */
struct test_cluster
{
    struct test_server servers[TEST_CLUSTER_N_SERVERS]; /* Cluster servers */
    raft_time time;                                     /* Global time */
    bool in_tear_down;                                  /* Tearing down */
    char trace[8192];                                   /* Captured messages */
    void *steps[2];                                     /* Pending events */
    void *send[2];                                      /* Pending messages */
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

/* Set the threshold for taking snapshots on the given server. */
void test_cluster_set_snapshot_threshold(struct test_cluster *c,
                                         raft_id id,
                                         unsigned threshold);

/* Set the number of entries to leave after a snapshot. */
void test_cluster_set_snapshot_trailing(struct test_cluster *c,
                                        raft_id id,
                                        unsigned trailing);

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
int test_cluster_submit(struct test_cluster *c,
                        raft_id id,
                        struct raft_entry *entry);

/* Start to catch-up a server. */
void test_cluster_catch_up(struct test_cluster *c,
                           raft_id id,
                           raft_id catch_up_id);

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
