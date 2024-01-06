#include <limits.h>

#include "progress.h"

#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "tracing.h"

#define infof(...) Infof(r->tracer, "  " __VA_ARGS__)
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

/* Initialize a single progress object. */
static void initProgress(struct raft_progress *p, raft_index last_index)
{
    p->next_index = last_index + 1;
    p->match_index = 0;
    p->last_send = ULLONG_MAX;
    p->last_recv = ULLONG_MAX;
    p->snapshot.index = 0;
    p->snapshot.last_send = ULLONG_MAX;
    p->state = PROGRESS__PROBE;
    p->catch_up = RAFT_CATCH_UP_NONE;
    p->features = 0;
}

int progressBuildArray(struct raft *r)
{
    struct raft_progress *progress;
    unsigned i;
    raft_index last_index = logLastIndex(r->log);
    progress = raft_malloc(r->configuration.n * sizeof *progress);
    if (progress == NULL) {
        return RAFT_NOMEM;
    }
    for (i = 0; i < r->configuration.n; i++) {
        initProgress(&progress[i], last_index);
        if (r->configuration.servers[i].id == r->id) {
            progress[i].match_index = r->last_stored;
        }
    }
    r->leader_state.progress = progress;
    return 0;
}

int progressRebuildArray(struct raft *r,
                         const struct raft_configuration *configuration)
{
    raft_index last_index = logLastIndex(r->log);
    struct raft_progress *progress;
    unsigned i;
    unsigned j;
    raft_id id;

    progress = raft_malloc(configuration->n * sizeof *progress);
    if (progress == NULL) {
        return RAFT_NOMEM;
    }

    /* First copy the progress information for the servers that exists both in
     * the current and in the new configuration. */
    for (i = 0; i < r->configuration.n; i++) {
        id = r->configuration.servers[i].id;
        j = configurationIndexOf(configuration, id);
        if (j == configuration->n) {
            /* This server is not present in the new configuration, so we just
             * skip it. */
            continue;
        }
        progress[j] = r->leader_state.progress[i];
    }

    /* Then reset the replication state for servers that are present in the new
     * configuration, but not in the current one. */
    for (i = 0; i < configuration->n; i++) {
        id = configuration->servers[i].id;
        j = configurationIndexOf(&r->configuration, id);
        if (j < r->configuration.n) {
            /* This server is present both in the new and in the current
             * configuration, so we have already copied its next/match index
             * value in the loop above. */
            continue;
        }
        assert(j == r->configuration.n);
        initProgress(&progress[i], last_index);
    }

    raft_free(r->leader_state.progress);
    r->leader_state.progress = progress;

    return 0;
}

bool progressIsUpToDate(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    raft_index last_index = logLastIndex(r->log);
    return p->next_index == last_index + 1;
}

bool progressShouldReplicate(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    raft_index last_index = logLastIndex(r->log);
    bool needs_heartbeat = false;
    bool result = false;

    /* We must be in a valid state. */
    assert(p->state == PROGRESS__PROBE || p->state == PROGRESS__PIPELINE ||
           p->state == PROGRESS__SNAPSHOT);

    /* The next index to send must be lower than the highest index in our
     * log. */
    assert(p->next_index <= last_index + 1);

    /* The last_send field is either at its max value (we never sent any
     * message), or it must be lower or equal than the current time.*/
    assert(p->last_send == ULLONG_MAX || p->last_send <= r->now);

    /* If we never sent any AppendEntries message to this follower, or if the
     * last time we sent it an AppendEntries message was more than a heartbeat
     * timeout ago, we need to send a heartbeat. */
    if (p->last_send == ULLONG_MAX ||
        r->now - p->last_send >= r->heartbeat_timeout) {
        needs_heartbeat = true;
    }

    switch (p->state) {
        case PROGRESS__SNAPSHOT:
            /* We are in snapshot mode, so we must have sent a snapshot. */
            assert(p->snapshot.last_send != ULLONG_MAX);

            /* Snapshot timed out, move to PROBE */
            if (r->now - p->snapshot.last_send >= r->install_snapshot_timeout) {
                infof("timeout install snapshot at index %llu",
                      p->snapshot.index);
                result = true;
                progressAbortSnapshot(r, i);
            } else {
                /* Enforce Leadership during follower Snapshot installation */
                result = needs_heartbeat;
            }
            break;
        case PROGRESS__PROBE:
            /* We send at most one message per heartbeat interval. */
            result = needs_heartbeat;
            break;
        case PROGRESS__PIPELINE:
            /* In pipeline mode we replicate new entries immediately, and send
             * empty append entries messages if we haven't sent anything in the
             * last heartbeat interval (i.e. there were no new entries in that
             * period). */
            result = !progressIsUpToDate(r, i) || needs_heartbeat;
            break;
    }
    return result;
}

raft_index progressNextIndex(struct raft *r, unsigned i)
{
    return r->leader_state.progress[i].next_index;
}

raft_index progressMatchIndex(struct raft *r, unsigned i)
{
    return r->leader_state.progress[i].match_index;
}

void progressUpdateLastSend(struct raft *r, unsigned i)
{
    r->leader_state.progress[i].last_send = r->now;
    r->update->flags |= RAFT_UPDATE_TIMEOUT;
}

void progressUpdateSnapshotLastSend(struct raft *r, unsigned i)
{
    r->leader_state.progress[i].snapshot.last_send = r->now;
}

void progressUpdateLastRecv(struct raft *r, unsigned i)
{
    r->leader_state.progress[i].last_recv = r->now;
}

inline void progressSetFeatures(struct raft *r,
                                const unsigned i,
                                raft_flags features)
{
    r->leader_state.progress[i].features = features;
}

inline raft_flags progressGetFeatures(struct raft *r, const unsigned i)
{
    return r->leader_state.progress[i].features;
}

raft_time progressGetLastSend(const struct raft *r, const unsigned i)
{
    return r->leader_state.progress[i].last_send;
}

raft_time progressGetLastRecv(const struct raft *r, const unsigned i)
{
    return r->leader_state.progress[i].last_recv;
}

void progressToSnapshot(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    p->state = PROGRESS__SNAPSHOT;
    p->snapshot.index = logSnapshotIndex(r->log);

    /* Set the next_index to the snapshot index + 1. While the snapshot is being
     * installed (or while we wait for the server to come online, before even
     * sending the snapshot) we'll send heartbeats using this next index, so
     * when we get back results we don't consider them as stale. */
    p->next_index = p->snapshot.index + 1;
}

void progressAbortSnapshot(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    p->next_index = p->match_index + 1;
    p->snapshot.index = 0;
    p->state = PROGRESS__PROBE;
}

int progressState(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    return p->state;
}

const char *progressStateName(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    const char *name;
    switch (p->state) {
        case PROGRESS__PROBE:
            name = "probe";
            break;
        case PROGRESS__PIPELINE:
            name = "pipeline";
            break;
        case PROGRESS__SNAPSHOT:
            name = "snapshot";
            break;
        default:
            assert(0);
            name = NULL;
            break;
    }
    return name;
}

bool progressMaybeDecrement(struct raft *r,
                            const unsigned i,
                            raft_index rejected,
                            raft_index last_index)
{
    struct raft_progress *p = &r->leader_state.progress[i];

    assert(p->state == PROGRESS__PROBE || p->state == PROGRESS__PIPELINE ||
           p->state == PROGRESS__SNAPSHOT);

    assert(rejected > 0);

    if (p->state == PROGRESS__SNAPSHOT) {
        /* The rejection must be stale or spurious if the rejected index does
         * not match the last snapshot index. */
        if (rejected != p->snapshot.index) {
            infof(
                "stale rejected index (%llu vs snapshot index %llu) -> ignore",
                rejected, p->snapshot.index);
            return false;
        }
        progressAbortSnapshot(r, i);
        return true;
    }

    if (p->state == PROGRESS__PIPELINE) {
        /* The rejection must be stale if the rejected index is smaller than
         * the matched one. */
        if (rejected <= p->match_index) {
            infof("stale rejected index (%llu vs match index %llu) -> ignore",
                  rejected, p->match_index);
            return false;
        }
        /* Directly decrease next to match + 1 */
        p->next_index = min(rejected, p->match_index + 1);
        progressToProbe(r, i);
        return true;
    }

    /* The rejection must be stale or spurious if the rejected index does not
     * match the next index minus one. */
    if (rejected != p->next_index - 1) {
        tracef("rejected index %llu different from next index %lld -> ignore ",
               rejected, p->next_index);
        return false;
    }

    p->next_index = min(rejected, last_index + 1);
    p->next_index = max(p->next_index, 1);

    return true;
}

void progressSetNextIndex(struct raft *r, unsigned i, raft_index next_index)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    p->next_index = next_index;
}

bool progressMaybeUpdate(struct raft *r, unsigned i, raft_index last_index)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    bool updated = false;
    if (p->match_index < last_index) {
        p->match_index = last_index;
        updated = true;
    }
    if (p->next_index < last_index + 1) {
        p->next_index = last_index + 1;
    }
    return updated;
}

void progressToProbe(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];

    if (p->state == PROGRESS__SNAPSHOT) {
        assert(p->snapshot.index > 0);
        p->snapshot.index = 0;
    } else {
        p->next_index = p->match_index + 1;
    }
    p->state = PROGRESS__PROBE;
}

void progressToPipeline(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    p->state = PROGRESS__PIPELINE;
}

bool progressSnapshotDone(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    assert(p->state == PROGRESS__SNAPSHOT);
    return p->match_index >= p->snapshot.index;
}

void progressCatchUpStart(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    assert(p->catch_up != RAFT_CATCH_UP_RUNNING);
    p->catch_up = RAFT_CATCH_UP_RUNNING;
}

void progressCatchUpAbort(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    assert(p->catch_up == RAFT_CATCH_UP_RUNNING);
    p->catch_up = RAFT_CATCH_UP_ABORTED;
}

void progressCatchUpFinish(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    assert(p->catch_up == RAFT_CATCH_UP_RUNNING);
    p->catch_up = RAFT_CATCH_UP_FINISHED;
}

int progressCatchUpStatus(const struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    return p->catch_up;
}

#undef infof
#undef tracef
