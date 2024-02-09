#include "../../src/configuration.h"
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

SUITE(election)

/* Test a successful election with 2 voters. */
TEST(election, TwoVoters, setUp, tearDown, 0, NULL)
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

    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    CLUSTER_TRACE(
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    return MUNIT_OK;
}

/* If we have already voted and the same candidate requests the vote again, the
 * vote is granted. */
TEST(election, GrantAgain, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 2 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    }

    /* Prevent server 2 from timing out. */
    CLUSTER_SET_ELECTION_TIMEOUT(2 /* ID */, 250 /* timeout */, 0 /* delta */);

    /* Now start the cluster. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n");

    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    /* Disconnect server 2 from server 1, so server 1 does not receive the
     * result. Then stop and restart server 1, artifcially resetting its term,
     * so it will ask again server 2's vote for term 2. */
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_STOP(1 /* ID */);
    CLUSTER_SET_TERM(1 /* ID */, 1 /* term */);
    CLUSTER_SET_VOTE(1 /* ID */, 0 /* vote */);
    CLUSTER_START(1);
    CLUSTER_TRACE(
        "[ 110] 1 > term 1, 1 entry (1^1)\n"
        "[ 210] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* Server 2 grants again its vote. */
    CLUSTER_TRACE(
        "[ 220] 2 > recv request vote from server 1\n"
        "           remote log is equal (1^1) -> grant vote\n");

    return MUNIT_OK;
}

/* If the requester last log entry index is the same, the vote is granted. */
TEST(election, GrantIfLastIndexIsSame, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 2 voters having each 2 equal entries. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 2 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        CLUSTER_START(id);
    }

    CLUSTER_TRACE(
        "[   0] 1 > term 2, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 2, 2 entries (1^1..2^1)\n");

    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (2^1) -> grant vote\n");

    return MUNIT_OK;
}

/* If the requester last log entry index is higher, the vote is granted. */
TEST(election, GrantIfRemoteLastIndexIsHigher, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Bootstrap a cluster with 2 voters, the first having 2 entries. */
    CLUSTER_SET_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
    CLUSTER_START(1 /* ID */);

    CLUSTER_SET_TERM(2 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_START(2 /* ID */);

    CLUSTER_TRACE(
        "[   0] 1 > term 2, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n");

    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log is longer (2^1 vs 1^1) -> grant vote\n");

    return MUNIT_OK;
}

/* If the requester last log entry term is higher, the vote is granted. */
TEST(election, GrantIfRemoteLastTermIsHigher, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Bootstrap a cluster with 2 voters. Both have 2 entries, however server
     * 1's last entry at index 2 has term 2, while server's 2 has term 1. */
    CLUSTER_SET_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_COMMAND, 2 /* term */, 0 /* payload */);
    CLUSTER_START(1 /* ID */);

    CLUSTER_SET_TERM(2 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
    CLUSTER_START(2 /* ID */);

    CLUSTER_TRACE(
        "[   0] 1 > term 2, 2 entries (1^1..2^2)\n"
        "[   0] 2 > term 1, 2 entries (1^1..2^1)\n");

    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log is more recent (2^2 vs 2^1) -> grant vote\n");

    return MUNIT_OK;
}

/* If a candidate receives a vote request response granting the vote but the
 * quorum is not reached, it stays candidate. */
TEST(election, WaitQuorum, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start 2 servers, having a configuration with 4 voters. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 4 /* servers */, 4 /* voters */);
        CLUSTER_START(id);
    }

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n");

    /* The first server converts to candidate and sends vote requests. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* Server 2 receives the request, grants its vote and sends the reply. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    /* Server 1 receives server 2's RequestVote result RPC but stays candidate
     * since it has only 2 votes, and 3 are required. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           quorum not reached, only 2 votes out of 4\n");

    return MUNIT_OK;
}

/* The vote request gets rejected if the term of the candidate is lower. */
TEST(election, RejectIfRemoteTermLower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Bootstrap a cluster with 2 voters. The second server is at term 3 */
    CLUSTER_SET_TERM(1 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(1 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_START(1 /* ID */);

    CLUSTER_SET_TERM(2 /* ID */, 3 /* term */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
    CLUSTER_START(2 /* ID */);

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 3, 1 entry (1^1)\n");

    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is lower (2 vs 3) -> reject\n");

    /* The first server receives the RequestVote result and converts to follower
     * because it discovers the newer term. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           local server is follower -> ignore\n");

    return 0;
}

/* If the server already has a leader, the vote is not granted (even if the
 * request has a higher term). */
TEST(election, RejectIfHasLeader, setUp, tearDown, 0, NULL)
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

    /* Server 1 receives the vote from server 3 as well. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n");

    /* Disconnect server 2, which eventually becomes candidate. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_SET_ELECTION_TIMEOUT(2 /* ID */, 30 /* timeout */, 0 /* delta */);

    CLUSTER_TRACE(
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "[ 140] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    /* Server 3 rejects the vote request because it has a leader. */
    CLUSTER_TRACE(
        "[ 150] 1 > recv request vote from server 2\n"
        "           local server is leader -> reject\n"
        "[ 150] 3 > recv request vote from server 2\n"
        "           local server has a leader (server 1) -> reject\n");

    return MUNIT_OK;
}

/* If a server has already voted, vote is not granted. */
TEST(election, RejectIfAlreadyVoted, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap and start a cluster with 3 voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }

    /* Disconnect server 2 from server 1 and change its randomized election
     * timeout to match the one of server 1. This way server 2 will convert to
     * candidate but not receive vote requests from server 1 (and viceversa). */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_SET_ELECTION_TIMEOUT(2 /* ID */, 100 /* timeout */, 0 /* delta */);

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n");

    /* Server 1 and server 2 both become candidates. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 100] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* Server 3 receives the vote request from server 1 and grants it. */
    CLUSTER_TRACE(
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    /* Server 3 receives the vote request from server 1 and rejects it because
     * it has already voted. */
    CLUSTER_TRACE(
        "[ 110] 3 > recv request vote from server 2\n"
        "           already voted for server 1 -> don't grant vote\n");

    return MUNIT_OK;
}

/* If the requester last log entry term is lower than ours, the vote is not
 * granted. */
TEST(election, RejectIfRemoteLastTermIsLower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned i;

    /* Bootstrap a cluster with 2 voters. Both servers have a command entry at
     * index 2, but server 1 has it with term 1 while server 2 has it with
     * term 2. */
    for (i = 1; i <= 2; i++) {
        CLUSTER_SET_TERM(i, 1 /* term */);
        CLUSTER_ADD_ENTRY(i, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_ADD_ENTRY(i, RAFT_COMMAND, i /* term */, 0 /* payload */);
        CLUSTER_START(i);
    }

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 2 entries (1^1..2^1)\n"
        "[   0] 2 > term 1, 2 entries (1^1..2^2)\n");

    /* The first server becomes candidate. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log older (2^1 vs 2^2) -> don't grant vote\n");

    return MUNIT_OK;
}

/* If the requester last log entry index is lower, the vote is not
 * granted. */
TEST(election, RejectIfRemoteLastIndexIsLower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 2 voters. Server 2 has an entry at
     * index 2, while server 1 hasn't. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        if (id == 2) {
            CLUSTER_ADD_ENTRY(id, RAFT_COMMAND, 1 /* term */, 0 /* payload */);
        }
        CLUSTER_START(id);
    }

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 2 entries (1^1..2^1)\n");

    /* Server 1 becomes candidate. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* Server 2 receives a RequestVote RPC and rejects the vote for server 1. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log shorter (1^1 vs 2^1) -> don't grant vote\n");

    return MUNIT_OK;
}

/* If we are not a voting server, the vote is not granted. */
TEST(election, RejectIfNotVoter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    struct raft_event event;
    struct raft_update update;
    int rv;

    CLUSTER_SET_TERM(2 /* ID */, 1 /* term */);
    CLUSTER_ADD_ENTRY(2 /* ID */, RAFT_CHANGE, 2 /* servers */, 1 /* voters */);
    CLUSTER_START(2);

    CLUSTER_TRACE("[   0] 2 > term 1, 1 entry (1^1)\n");

    message.type = RAFT_REQUEST_VOTE;
    message.server_id = 1;
    message.server_address = "1";
    message.request_vote.version = 2;
    message.request_vote.term = 2;
    message.request_vote.candidate_id = 1;
    message.request_vote.last_log_index = 1;
    message.request_vote.last_log_term = 1;
    message.request_vote.disrupt_leader = false;
    message.request_vote.pre_vote = false;

    event.time = f->cluster_.time;
    event.type = RAFT_RECEIVE;
    event.receive.message = &message;

    rv = raft_step(CLUSTER_RAFT(2), &event, &update);
    munit_assert_int(rv, ==, 0);
    CLUSTER_TRACE(
        "[   0] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           local server is not voting -> don't grant vote\n");

    return MUNIT_OK;
}

/* Non-voting servers are skipped when sending vote requests. */
TEST(election, SkipNonVoters, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 3 servers, among which only server 1 and server
     * 2 are voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }

    /* Disconnect server 1 from server 2, so server 1 can't win the elections,
     * since it needs the vote from 2. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n");

    /* Server 1 becomes candidate. */
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* Server 1 stays candidate because it can't reach a quorum and eventually
     * server 2 becomes candidate as well. */
    CLUSTER_TRACE(
        "[ 130] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    return MUNIT_OK;
}

/* If a candidate server receives a response indicating that the vote was not
 * granted, nothing happens (e.g. the server has already voted for someone
 * else). */
TEST(election, ReceiveRejectResult, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 3 servers, all voters. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }

    /* Disconnect server 1 from server 2 and viceversa. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);

    /* Server 1 becomes candidate, server 2 is still follower. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* Server 3 receives a RequestVote RPC and grants its vote. */
    CLUSTER_TRACE(
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    /* Disconnect server 3 from server 1, so it doesn't receive further
     * messages. */
    CLUSTER_DISCONNECT(3, 1);
    CLUSTER_DISCONNECT(1, 3);

    /* Server 2 server eventually becomes candidate */
    CLUSTER_TRACE(
        "[ 130] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n");

    /* Server 3 receives a RequestVote RPC from server 2 but rejects its vote
     * since it has already voted for server 1. */
    CLUSTER_TRACE(
        "[ 140] 3 > recv request vote from server 2\n"
        "           already voted for server 1 -> don't grant vote\n");

    /* Server 5 receives the response and stays candidate. */
    CLUSTER_TRACE(
        "[ 150] 2 > recv request vote result from server 3\n"
        "           vote not granted\n");

    munit_assert_int(raft_state(CLUSTER_RAFT(2)), ==, RAFT_CANDIDATE);

    return MUNIT_OK;
}

/* Test an election round with two voters and pre-vote. */
TEST(election, PreVote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 5 servers, all voters with pre-vote enabled. */
    for (id = 1; id <= 2; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        raft_set_pre_vote(CLUSTER_RAFT(id), true);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 2 /* servers */, 2 /* voters */);
        CLUSTER_START(id);
    }

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start pre-election for term 2\n");

    /* Server 1 did not increment its term or persist its vote.*/
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(1)), ==, 1);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(1)), ==, 0);

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote log is equal (1^1) -> pre-vote ok\n");

    /* Server 2 has not incremented its term or persisted its vote.*/
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(2)), ==, 1);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(2)), ==, 0);

    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> pre-vote successful\n");

    /* Server 1 has now incremented its term and persisted its vote. */
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(1)), ==, 2);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(1)), ==, 1);

    CLUSTER_TRACE(
        "[ 130] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(2)), ==, 2);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(2)), ==, 1);

    CLUSTER_TRACE(
        "[ 140] 1 > recv request vote result from server 2\n"
        "           quorum reached with 2 votes out of 2 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}

/* A candidate receives votes then crashes. */
TEST(election, PreVoteWithcandidateCrash, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 3 servers, all voters with pre-vote enabled. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        raft_set_pre_vote(CLUSTER_RAFT(id), true);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        CLUSTER_START(id);
    }

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n");

    /* Server 1 eventually times out and converts to candidate, but it does not
     * increment its term yet.*/
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start pre-election for term 2\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(1)), ==, 1);

    /* Server 2 receives the pre-vote RequestVote RPC but does not increment its
     * term. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote log is equal (1^1) -> pre-vote ok\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(2)), ==, 1);

    /* Server 3 receives the pre-vote RequestVote RPC but does not increment its
     * term. */
    CLUSTER_TRACE(
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote log is equal (1^1) -> pre-vote ok\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(3)), ==, 1);

    /* Server 1 receives the pre-vote RequestVote results and starts the actual
     * election, incrementing its term and persisting its vote. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> pre-vote successful\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           receive stale pre-vote response -> ignore\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(1)), ==, 2);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(1)), ==, 1);

    /* Server 2 receives the actual RequestVote RPC */
    CLUSTER_TRACE(
        "[ 130] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    /* Server 3 receives the actual RequestVote RPC */
    CLUSTER_TRACE(
        "[ 130] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    /* Server 1 crashes. */
    CLUSTER_STOP(1);

    /* Server 2 times out and starts an election. It doesn't increment its term
     * yet. It also can't reset its vote since it's still in the same term. */
    CLUSTER_TRACE(
        "[ 260] 2 > timeout as follower\n"
        "           convert to candidate, start pre-election for term 3\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(2)), ==, 2);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(2)), ==, 1);

    /* Server 3 has already voted for server 1 in term 2, but it didn't vote yet
     * for term 3, so it grants its pre-vote, albeit without bumping the term or
     * resetting it previous vote. */
    CLUSTER_TRACE(
        "[ 270] 3 > recv request vote from server 2\n"
        "           remote log is equal (1^1) -> pre-vote ok\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(3)), ==, 2);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(3)), ==, 1);

    /* Server 2 receives the pre-vote RequestVote result from server 3. It now
     * starts the actual election, bumping its term and persisting its vote for
     * itself. */
    CLUSTER_TRACE(
        "[ 280] 2 > recv request vote result from server 3\n"
        "           votes quorum reached -> pre-vote successful\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(2)), ==, 3);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(2)), ==, 2);

    /* Server 3 receives the actual RequestVote RPC. */
    CLUSTER_TRACE(
        "[ 290] 3 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n");

    /* Server 2 receives the actual RequestVote result */
    CLUSTER_TRACE(
        "[ 300] 2 > recv request vote result from server 3\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 1 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n");

    return MUNIT_OK;
}

/* Ensure delayed pre-vote responses are not counted towards the real election
 * quorum. */
TEST(election, PreVoteNoStaleVotes, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 3 servers, all voters with pre-vote enabled.
     *
     * Server 3 is 1 term ahead of the other servers, this will allow it to send
     * stale pre-vote responses that pass the term checks. */
    for (id = 1; id <= 3; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        raft_set_pre_vote(CLUSTER_RAFT(id), true);
        CLUSTER_ADD_ENTRY(id, RAFT_CHANGE, 3 /* servers */, 3 /* voters */);
        if (id == 3) {
            CLUSTER_SET_TERM(3, 2);
        }
        CLUSTER_START(id);
    }

    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 2, 1 entry (1^1)\n");

    /* Server 1 eventually times out and converts to candidate, but it does not
     * increment its term yet.*/
    CLUSTER_TRACE(
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start pre-election for term 2\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(1)), ==, 1);

    /* Server 2 receives the pre-vote RequestVote RPC but does not increment its
     * term. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote log is equal (1^1) -> pre-vote ok\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(2)), ==, 1);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(2)), ==, 0);

    /* Slow down responses of server 3 */
    CLUSTER_SET_NETWORK_LATENCY(3 /* ID */, 20 /* latency */);

    /* Server 3 receives the pre-vote RequestVote RPC but does not increment its
     * term. */
    CLUSTER_TRACE(
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote log is equal (1^1) -> pre-vote ok\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(2)), ==, 1);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(2)), ==, 0);

    /* Server 1 receives the pre-vote RequestVote result from server 2 and it
     * starts the actual election. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> pre-vote successful\n");
    munit_assert_ulong(raft_current_term(CLUSTER_RAFT(1)), ==, 2);
    munit_assert_ulong(raft_voted_for(CLUSTER_RAFT(1)), ==, 1);

    /* Server one eventually receives server 3's delayed RequestVote result for
     * the pre-vote message, but does not count it as real vote. */
    CLUSTER_TRACE(
        "[ 130] 1 > recv request vote result from server 3\n"
        "           receive stale pre-vote response -> ignore\n");

    /* Make sure we haven't counted the pre-vote result as a real vote */
    munit_assert_int(raft_state(CLUSTER_RAFT(1)), ==, RAFT_CANDIDATE);

    return MUNIT_OK;
}

/* If a follower is a stand-by, it won't convert to candidate */
TEST(election, StayFollowerIfStandBy, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned i;
    unsigned id;
    struct raft_configuration configuration;
    struct raft_entry entry;
    int rv;

    /* Bootstrap a cluster with 4 servers, with 3 voters and 1 stand-by. */
    entry.type = RAFT_CHANGE;
    entry.term = 1;
    CLUSTER_FILL_CONFIGURATION(&configuration, 4 /* n servers */,
                               3 /* voters */, 1 /* stand-by */);
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    entry.batch = NULL;
    for (id = 1; id <= 4; id++) {
        CLUSTER_SET_TERM(id, 1 /* term */);
        CLUSTER_ADD_ENTRY(id, &entry);
        CLUSTER_START(id);
    }
    raft_free(entry.buf.base);

    /* Server 2 takes a very long time to persist entries. */
    CLUSTER_SET_DISK_LATENCY(2, 1000);

    /* Disconnect server 2 from server 1, so it won't vote for it. */
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 2);

    /* Increase the election timeout of server 2, so it will start just 1
     * election. */
    CLUSTER_SET_ELECTION_TIMEOUT(2 /* ID */, 150 /* timeout */, 0 /* delta */);

    /* Server 1 wins elections for term 2, with a vote from server 3. */
    CLUSTER_TRACE(
        "[   0] 1 > term 1, 1 entry (1^1)\n"
        "[   0] 2 > term 1, 1 entry (1^1)\n"
        "[   0] 3 > term 1, 1 entry (1^1)\n"
        "[   0] 4 > term 1, 1 entry (1^1)\n"
        "[ 100] 1 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log is equal (1^1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           quorum reached with 2 votes out of 3 -> convert to leader\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           probe server 4 sending a heartbeat (no entries)\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    /* Demote server 2 to stand-by. */
    entry.term = 2;
    configuration.servers[1].role = RAFT_STANDBY;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    entry.batch = entry.buf.base;

    CLUSTER_SUBMIT(1 /* ID */, &entry);

    CLUSTER_TRACE(
        "[ 130] 1 > submit 1 new client entry\n"
        "           replicate 1 new configuration entry (2^2)\n");

    /* While the configuration change is in progress, server 2 times out and
     * starts an unsuccessful election. */
    CLUSTER_TRACE(
        "[ 130] 4 > recv append entries from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           no new entries to persist\n"
        "[ 140] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 2\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           pipeline server 3 sending 1 entry (2^2)\n"
        "[ 140] 1 > recv append entries result from server 4\n"
        "           pipeline server 4 sending 1 entry (2^2)\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 150] 4 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 150] 2 > timeout as follower\n"
        "           convert to candidate, start election for term 2\n"
        "[ 160] 3 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 160] 4 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 160] 3 > recv request vote from server 2\n"
        "           local server has a leader (server 1) -> reject\n");

    /* The configuration change is committed. */
    CLUSTER_TRACE(
        "[ 170] 1 > recv append entries result from server 3\n"
        "           commit 1 new entry (2^2)\n"
        "[ 170] 1 > recv append entries result from server 4\n"
        "[ 170] 2 > recv request vote result from server 3\n"
        "           vote not granted\n"
        "[ 170] 1 > timeout as leader\n"
        "           probe server 2 sending 1 entry (2^2)\n");

    /* Promote server 4 to voter. */
    entry.term = 2;
    configuration.servers[3].role = RAFT_VOTER;
    rv = raft_configuration_encode(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);
    entry.batch = entry.buf.base;

    CLUSTER_SUBMIT(1 /* ID */, &entry);
    CLUSTER_TRACE(
        "[ 170] 1 > submit 1 new client entry\n"
        "           replicate 1 new configuration entry (3^2)\n"
        "           pipeline server 3 sending 1 entry (3^2)\n"
        "           pipeline server 4 sending 1 entry (3^2)\n");

    /* Wait for server 4 to persist the configuration change and apply it,
     * becoming aware that it's a voter. */
    CLUSTER_TRACE(
        "[ 180] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (3^2) has 1 vote out of 3\n"
        "[ 180] 3 > recv append entries from server 1\n"
        "           start persisting 1 new entry (3^2)\n"
        "[ 180] 4 > recv append entries from server 1\n"
        "           start persisting 1 new entry (3^2)\n"
        "[ 190] 3 > persisted 1 entry (3^2)\n"
        "           send success result to 1\n"
        "[ 190] 4 > persisted 1 entry (3^2)\n"
        "           send success result to 1\n");

    munit_assert_int(CLUSTER_RAFT(4)->configuration.servers[3].role, ==,
                     RAFT_VOTER);

    /* Server 2 is still candidate. */
    munit_assert_int(raft_state(CLUSTER_RAFT(2)), ==, RAFT_CANDIDATE);

    /* Reconnect server 2 to server 1, so it will receive up to index 4,
     * although it won't persist it since it has a high disk latency. */
    CLUSTER_RECONNECT(2, 1);
    CLUSTER_RECONNECT(1, 2);

    /* Server 2 to gets contacted by server 1, steps down and receives
     * entries from it */
    CLUSTER_TRACE(
        "[ 200] 1 > recv append entries result from server 3\n"
        "           commit 1 new entry (3^2)\n"
        "[ 200] 1 > recv append entries result from server 4\n"
        "[ 220] 1 > timeout as leader\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n"
        "           pipeline server 4 sending a heartbeat (no entries)\n"
        "[ 230] 2 > recv append entries from server 1\n"
        "           discovered leader (1) -> step down \n"
        "           start persisting 2 new entries (2^2..3^2)\n"
        "[ 230] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 230] 4 > recv append entries from server 1\n"
        "           no new entries to persist\n");

    /* Create a network partition, with server 1 and 4 in one partition and
     * server 2 and 3 in another partition. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);
    CLUSTER_DISCONNECT(4, 2);
    CLUSTER_DISCONNECT(2, 4);
    CLUSTER_DISCONNECT(4, 3);
    CLUSTER_DISCONNECT(3, 4);

    munit_assert_int(raft_state(CLUSTER_RAFT(2)), ==, RAFT_FOLLOWER);

    /* Eventually both server 2 and server 3 time out because they have been
     * disconnected from the leader.
     *
     * Server 3 immediately converts to candidate. Server 2 is still persisting
     * entries, but it applies configuration changes immediately so it finds out
     * it's a stand-by and stays follower. */
    CLUSTER_TRACE(
        "[ 240] 1 > recv append entries result from server 4\n"
        "[ 270] 1 > timeout as leader\n"
        "           server 3 is unreachable -> abort pipeline\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           pipeline server 4 sending a heartbeat (no entries)\n"
        "[ 280] 4 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 290] 1 > recv append entries result from server 4\n"
        "[ 320] 1 > timeout as leader\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           pipeline server 4 sending a heartbeat (no entries)\n"
        "[ 330] 4 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 340] 1 > recv append entries result from server 4\n"
        "[ 370] 1 > timeout as leader\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           pipeline server 4 sending a heartbeat (no entries)\n"
        "[ 380] 4 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 380] 2 > timeout as follower\n"
        "           stand-by server -> stay follower\n"
        "[ 390] 1 > recv append entries result from server 4\n"
        "[ 390] 3 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n");

    munit_assert_int(raft_state(CLUSTER_RAFT(2)), ==, RAFT_FOLLOWER);
    munit_assert_int(raft_state(CLUSTER_RAFT(3)), ==, RAFT_CANDIDATE);

    /* Server 3 can't win the election, because it does not consider server 2 a
     * voter, according to the configuration at index 4.
     *
     * Server 2 also can't win the election, because the it stays follower. */
    CLUSTER_TRACE(
        "[ 420] 1 > timeout as leader\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           pipeline server 4 sending a heartbeat (no entries)\n"
        "[ 430] 4 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 440] 1 > recv append entries result from server 4\n"
        "[ 470] 1 > timeout as leader\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           pipeline server 4 sending a heartbeat (no entries)\n"
        "[ 480] 4 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 490] 1 > recv append entries result from server 4\n"
        "[ 520] 1 > timeout as leader\n"
        "           probe server 2 sending 2 entries (2^2..3^2)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "           pipeline server 4 sending a heartbeat (no entries)\n"
        "[ 530] 4 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 530] 2 > timeout as follower\n"
        "           stand-by server -> stay follower\n"
        "[ 540] 1 > recv append entries result from server 4\n");

    for (i = 0; i < 40; i++) {
        test_cluster_step(&f->cluster_);
    }
    munit_assert_int(raft_state(CLUSTER_RAFT(2)), !=, RAFT_LEADER);
    munit_assert_int(raft_state(CLUSTER_RAFT(3)), !=, RAFT_LEADER);

    /* Server 1 is still leader, since it can contact server 4. */
    munit_assert_int(raft_state(CLUSTER_RAFT(1)), ==, RAFT_LEADER);

    return MUNIT_OK;
}

/* If a follower is installing a snapshot, it won't convert to
 * candidate */
TEST(election, StayFollowerIfInstallingSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Set very low threshold and trailing entries number */
    CLUSTER_SET_SNAPSHOT_THRESHOLD(1 /* ID */, 2 /* n. entries */);
    CLUSTER_SET_SNAPSHOT_TRAILING(1 /* ID */, 0 /* n. entries */);

    /* Bootstrap a cluster with 3 servers. */
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

    /* Submit a couple of entries, causing server 1 to eventually take a
     * snapshot. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);

    /* Disconnect server 2 from server 1, so it won't receive these entries. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 2);

    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (3^2)\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n"
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 3\n"
        "[ 130] 1 > persisted 1 entry (3^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 3\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           pipeline server 3 sending 2 entries (2^2..3^2)\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           start persisting 2 new entries (2^2..3^2)\n"
        "[ 160] 3 > persisted 2 entry (2^2..3^2)\n"
        "           send success result to 1\n"
        "[ 170] 1 > recv append entries result from server 3\n"
        "           commit 2 new entries (2^2..3^2)\n"
        "[ 170] 1 > new snapshot (3^2), 0 trailing entries\n");

    /* Reconnect server 2 to server 1, so it will receive the snapshot. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_RECONNECT(2, 2);

    /* Server 2 takes a very long time to persist the snapshot. */
    CLUSTER_SET_DISK_LATENCY(2, 1000);

    CLUSTER_TRACE(
        "[ 170] 1 > timeout as leader\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "[ 180] 2 > recv append entries from server 1\n"
        "           missing previous entry (3^2) -> reject\n"
        "[ 190] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries\n"
        "           missing previous entry at index 1 -> needs snapshot\n"
        "           sending snapshot (3^2) to server 2\n"
        "[ 190] 1 > timeout as leader\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n"
        "[ 200] 2 > recv install snapshot from server 1\n"
        "           start persisting snapshot (3^2)\n");

    /* Disconnect server 1 from server 2 and 3. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);

    /* Server 2 eventually times out, but it does not convert to candidate
     * because it's still persisting the snapshot. */
    CLUSTER_TRACE(
        "[ 240] 1 > timeout as leader\n"
        "           server 3 is unreachable -> abort pipeline\n"
        "           timeout install snapshot at index 3\n"
        "           missing previous entry at index 0 -> needs snapshot\n"
        "           sending snapshot (3^2) to server 2\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 290] 1 > timeout as leader\n"
        "           server 2 is unreachable -> abort snapshot\n"
        "           missing previous entry at index 0 -> needs snapshot\n"
        "           probe server 2 sending a heartbeat (no entries)\n"
        "           probe server 3 sending a heartbeat (no entries)\n"
        "[ 310] 3 > timeout as follower\n"
        "           convert to candidate, start election for term 3\n"
        "[ 320] 2 > recv request vote from server 3\n"
        "           local server has a leader (server 1) -> reject\n"
        "[ 330] 3 > recv request vote result from server 2\n"
        "           remote term is lower (2 vs 3) -> ignore\n"
        "[ 330] 2 > timeout as follower\n"
        "           installing snapshot -> stay follower\n");

    return MUNIT_OK;
}

/* If a follower has entries that are still being persisted, it won't convert to
 * candidate */
TEST(election, StayFollowerIfPersistingEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned id;

    /* Bootstrap a cluster with 3 servers. */
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

    /* Server 2 takes a very long time to persist entries. */
    CLUSTER_SET_DISK_LATENCY(2, 1000);

    /* Submit an entry and replicate it to server 2 and 3. */
    CLUSTER_SUBMIT(1 /* ID */, COMMAND, 8 /* size */);
    CLUSTER_TRACE(
        "[ 120] 1 > submit 1 new client entry\n"
        "           replicate 1 new command entry (2^2)\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n"
        "[ 130] 1 > persisted 1 entry (2^2)\n"
        "           next uncommitted entry (2^2) has 1 vote out of 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no new entries to persist\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           pipeline server 2 sending 1 entry (2^2)\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           pipeline server 3 sending 1 entry (2^2)\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           start persisting 1 new entry (2^2)\n");

    /* Disconnect server 1 from server 2 and 3. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);

    /* Server 2 eventually times out, but stays follower because it's still
     * persisting the new entry. */
    CLUSTER_TRACE(
        "[ 160] 3 > persisted 1 entry (2^2)\n"
        "           send success result to 1\n"
        "[ 190] 1 > timeout as leader\n"
        "           pipeline server 2 sending a heartbeat (no entries)\n"
        "           pipeline server 3 sending a heartbeat (no entries)\n"
        "[ 240] 1 > timeout as leader\n"
        "           server 2 is unreachable -> abort pipeline\n"
        "           server 3 is unreachable -> abort pipeline\n"
        "           probe server 2 sending 1 entry (2^2)\n"
        "           probe server 3 sending 1 entry (2^2)\n"
        "[ 280] 2 > timeout as follower\n"
        "           persisting 1 entries -> stay follower\n");

    return MUNIT_OK;
}
