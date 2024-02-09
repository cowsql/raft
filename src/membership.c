#include "membership.h"

#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "heap.h"
#include "message.h"
#include "progress.h"
#include "queue.h"
#include "tracing.h"
#include "trail.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)

int membershipCanChangeConfiguration(struct raft *r)
{
    int rv;

    if (r->configuration_uncommitted_index != 0) {
        rv = RAFT_CANTCHANGE;
        goto err;
    }

    if (r->leader_state.promotee_id != 0) {
        rv = RAFT_CANTCHANGE;
        goto err;
    }

    /* In order to become leader at all we are supposed to have committed at
     * least the initial configuration at index 1. */
    assert(r->configuration_committed_index > 0);

    /* The index of the last committed configuration can't be greater than the
     * last log index. */
    assert(TrailLastIndex(&r->trail) >= r->configuration_committed_index);

    /* No catch-up round should be in progress. */
    assert(r->leader_state.round_number == 0);
    assert(r->leader_state.round_index == 0);
    assert(r->leader_state.round_start == 0);

    return 0;

err:
    assert(rv == RAFT_CANTCHANGE);
    ErrMsgFromCode(r->errmsg, rv);
    return rv;
}

bool membershipUpdateCatchUpRound(struct raft *r)
{
    unsigned server_index;
    raft_index match_index;
    raft_index last_index;
    raft_time round_duration;
    bool is_up_to_date;
    bool is_fast_enough;

    assert(r->state == RAFT_LEADER);
    assert(r->leader_state.promotee_id != 0);

    server_index =
        configurationIndexOf(&r->configuration, r->leader_state.promotee_id);
    assert(server_index < r->configuration.n);

    match_index = progressMatchIndex(r, server_index);

    /* If the server did not reach the target index for this round, it did not
     * catch up. */
    if (match_index < r->leader_state.round_index) {
        infof(
            "member (index: %u) not yet caught up match_index:%llu "
            "round_index:%llu",
            server_index, match_index, r->leader_state.round_index);
        return false;
    }

    last_index = TrailLastIndex(&r->trail);
    round_duration = r->now - r->leader_state.round_start;

    is_up_to_date = match_index == last_index;
    is_fast_enough = round_duration < r->election_timeout;

    infof("member is_up_to_date:%d is_fast_enough:%d", is_up_to_date,
          is_fast_enough);

    /* If the server's log is fully up-to-date or the round that just terminated
     * was fast enough, then the server as caught up. */
    if (is_up_to_date || is_fast_enough) {
        r->leader_state.round_number = 0;
        r->leader_state.round_index = 0;
        r->leader_state.round_start = 0;

        progressCatchUpFinish(r, server_index);

        return true;
    }

    /* If we get here it means that this catch-up round is complete, but there
     * are more entries to replicate, or it was not fast enough. Let's start a
     * new round. */
    r->leader_state.round_number++;
    r->leader_state.round_index = last_index;
    r->leader_state.round_start = r->now;

    return false;
}

int membershipUncommittedChange(struct raft *r,
                                const raft_index index,
                                const struct raft_entry *entry)
{
    struct raft_configuration configuration;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_LEADER);
    assert(entry != NULL);
    assert(entry->type == RAFT_CHANGE);

    rv = configurationDecode(&entry->buf, &configuration);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM || rv == RAFT_MALFORMED);
        goto err;
    }

    if (r->state == RAFT_LEADER) {
        /* Rebuild the progress array if the new configuration has a different
         * number of servers than the old one. */
        if (configuration.n != r->configuration.n) {
            rv = progressRebuildArray(r, &configuration);
            if (rv != 0) {
                assert(rv == RAFT_NOMEM);
                goto err_after_decode;
            }
        }
    }

    raft_configuration_close(&r->configuration);

    r->configuration = configuration;
    r->configuration_uncommitted_index = index;

    return 0;

err_after_decode:
    configurationClose(&configuration);
err:
    assert(rv == RAFT_NOMEM || rv == RAFT_MALFORMED);
    return rv;
}

int membershipRollback(struct raft *r)
{
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_FOLLOWER);
    assert(r->configuration_uncommitted_index > 0);

    infof("roll back uncommitted configuration (%llu^%llu)",
          r->configuration_uncommitted_index,
          TrailTermOf(&r->trail, r->configuration_uncommitted_index));

    /* Replace the current configuration with the last committed one. */
    assert(r->configuration_committed_index > 0);

    configurationClose(&r->configuration);
    rv = configurationCopy(&r->configuration_committed, &r->configuration);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        return rv;
    }

    r->configuration_uncommitted_index = 0;
    return 0;
}

int membershipLeadershipTransferStart(struct raft *r)
{
    const struct raft_server *server;
    struct raft_message message;
    int rv;

    assert(r->state == RAFT_LEADER);
    assert(r->leader_state.transferee != 0);
    assert(!r->leader_state.transferring);

    server = configurationGet(&r->configuration, r->leader_state.transferee);
    assert(server != NULL);

    message.type = RAFT_TIMEOUT_NOW;
    message.timeout_now.version = MESSAGE__TIMEOUT_NOW_VERSION;
    message.timeout_now.term = r->current_term;
    message.timeout_now.last_log_index = TrailLastIndex(&r->trail);
    message.timeout_now.last_log_term = TrailLastTerm(&r->trail);

    message.server_id = server->id;
    message.server_address = server->address;

    infof("send timeout to %llu", server->id);
    rv = MessageEnqueue(r, &message);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        ErrMsgPrintf(r->errmsg, "send timeout now to %llu", server->id);
        return rv;
    }

    /* Set the leadership transfer in progress flag. */
    r->leader_state.transferring = true;

    return 0;
}

#undef infof
