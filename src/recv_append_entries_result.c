#include "recv_append_entries_result.h"
#include "assert.h"
#include "configuration.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

int recvAppendEntriesResult(struct raft *r,
                            const raft_id id,
                            const char *address,
                            const struct raft_append_entries_result *result)
{
    const struct raft_server *server;
    int match;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(address != NULL);
    assert(result != NULL);

    /* XXX: Up to version 0.19.1 followers were erroneously setting
     * last_log_index to whatever their last log index was, regardless or the
     * index being rejected. If we detect such a case, we manually amend it
     * here. This code can be dropped once sufficient time has passed that we
     * are confident that no server is running the old buggy code. */
    if (result->rejected > 0 && result->last_log_index >= result->rejected) {
        ((struct raft_append_entries_result *)result)->last_log_index =
            result->rejected - 1;
    }

    if (r->state != RAFT_LEADER) {
        infof("local server is not leader -> ignore");
        return 0;
    }

    match = recvEnsureMatchingTerms(r, result->term);

    if (match < 0) {
        infof("local term is higher (%llu vs %llu) -> ignore", r->current_term,
              result->term);
        return 0;
    }

    /* If we have stepped down, abort here.
     *
     * From Figure 3.1:
     *
     *   [Rules for Servers] All Servers: If RPC request or response contains
     *   term T > currentTerm: set currentTerm = T, convert to follower.
     */
    if (match > 0) {
        assert(r->state == RAFT_FOLLOWER);
        return 0;
    }

    assert(result->term == r->current_term);

    /* Ignore responses from servers that have been removed */
    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        infof("unknown server -> ignore");
        return 0;
    }

    /* Update the progress of this server, possibly sending further entries. */
    rv = replicationUpdate(r, server, result);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef infof
#undef tracef
