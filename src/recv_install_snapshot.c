#include "recv_install_snapshot.h"

#include "assert.h"
#include "convert.h"
#include "message.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"
#include "trail.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)

int recvInstallSnapshot(struct raft *r,
                        const raft_id id,
                        const char *address,
                        struct raft_install_snapshot *args)
{
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    int rv;
    int match;
    bool async;

    assert(address != NULL);

    result->rejected = args->last_index;
    result->last_log_index = TrailLastIndex(&r->trail);
    result->version = RAFT_APPEND_ENTRIES_RESULT_VERSION;
    result->features = 0;

    recvEnsureMatchingTerms(r, args->term, &match);

    if (match < 0) {
        infof("local term is higher (%llu vs %llu) -> reject", r->current_term,
              args->term);
        goto reply;
    }

    /* TODO: this logic duplicates the one in the AppendEntries handler */
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    assert(r->current_term == args->term);
    if (r->state == RAFT_CANDIDATE) {
        assert(match == 0);
        infof("discovered leader (%llu) -> step down ", id);
        convertToFollower(r);
    }

    rv = recvUpdateLeader(r, id, address);
    if (rv != 0) {
        return rv;
    }
    r->election_timer_start = r->now;
    r->update->flags |= RAFT_UPDATE_TIMEOUT;

    rv = replicationInstallSnapshot(r, args, &result->rejected, &async);
    if (rv != 0) {
        return rv;
    }

    if (async) {
        return 0;
    }

    if (result->rejected == 0) {
        /* Echo back to the leader the point that we reached. */
        result->last_log_index = args->last_index;
    }

reply:
    result->term = r->current_term;

    /* Free the snapshot data. */
    raft_configuration_close(&args->conf);
    raft_free(args->data.base);

    result->capacity = r->capacity;

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = id;
    message.server_address = address;

    rv = MessageEnqueue(r, &message);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef infof
