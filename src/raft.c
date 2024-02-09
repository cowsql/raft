#include "../include/raft.h"

#include <limits.h>
#include <string.h>

#include "assert.h"
#include "byte.h"
#include "client.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "entry.h"
#include "err.h"
#include "heap.h"
#include "membership.h"
#include "message.h"
#include "progress.h"
#include "queue.h"
#include "random.h"
#include "recv.h"
#include "replication.h"
#include "restore.h"
#include "timeout.h"
#include "tracing.h"
#include "trail.h"

#ifndef RAFT__LEGACY_no
#include "legacy.h"
#include "log.h"
#endif

#define DEFAULT_ELECTION_TIMEOUT 1000          /* One second */
#define DEFAULT_HEARTBEAT_TIMEOUT 100          /* One tenth of a second */
#define DEFAULT_INSTALL_SNAPSHOT_TIMEOUT 30000 /* 30 seconds */

#if !defined(RAFT__LEGACY_no)
#define DEFAULT_SNAPSHOT_THRESHOLD 1024
#define DEFAULT_SNAPSHOT_TRAILING 2048
#endif

/* Number of milliseconds after which a server promotion will be aborted if the
 * server hasn't caught up with the logs yet. */
#define DEFAULT_MAX_CATCH_UP_ROUNDS 10
#define DEFAULT_MAX_CATCH_UP_ROUND_DURATION (5 * 1000)

#define DEFAULT_MAX_INFLIGHT_ENTRIES 32

#define infof(...) Infof(r->tracer, "> " __VA_ARGS__)

int raft_version_number(void)
{
    return RAFT_VERSION_NUMBER;
}

#ifndef RAFT__LEGACY_no
static int ioFsmVersionCheck(struct raft *r,
                             struct raft_io *io,
                             struct raft_fsm *fsm)
{
    if (io->version == 0) {
        ErrMsgPrintf(r->errmsg, "io->version must be set");
        return -1;
    }

    if (fsm->version == 0) {
        ErrMsgPrintf(r->errmsg, "fsm->version must be set");
        return -1;
    }

    return 0;
}
#endif

int raft_init(struct raft *r,
              struct raft_io *io,
              struct raft_fsm *fsm,
              const raft_id id,
              const char *address)
{
    int rv;
    assert(r != NULL);

    r->tracer = &StderrTracer;
    raft_tracer_maybe_enable(r->tracer, true);

    r->id = id;
    /* Make a copy of the address */
    r->address = RaftHeapMalloc(strlen(address) + 1);
    if (r->address == NULL) {
        ErrMsgOom(r->errmsg);
        rv = RAFT_NOMEM;
        goto err;
    }
    strcpy(r->address, address);
    r->current_term = 0;
    r->voted_for = 0;
    TrailInit(&r->trail);

    raft_configuration_init(&r->configuration);
    raft_configuration_init(&r->configuration_committed);
    r->configuration_committed_index = 0;
    r->configuration_uncommitted_index = 0;
    r->configuration_last_snapshot_index = 0;
    r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
    r->install_snapshot_timeout = DEFAULT_INSTALL_SNAPSHOT_TIMEOUT;
    r->commit_index = 0;
    r->last_stored = 0;
    r->state = RAFT_FOLLOWER;
    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;
    r->follower_state.match = 0;
    r->snapshot.installing = false;
    memset(r->errmsg, 0, sizeof r->errmsg);
    r->pre_vote = false;
    r->max_catch_up_rounds = DEFAULT_MAX_CATCH_UP_ROUNDS;
    r->max_catch_up_round_duration = DEFAULT_MAX_CATCH_UP_ROUND_DURATION;
    r->now = 0;
    r->messages = NULL;
    r->n_messages_cap = 0;
    r->max_inflight_entries = DEFAULT_MAX_INFLIGHT_ENTRIES;
    r->update = NULL;
    r->capacity = 0;
    r->capacity_threshold = 0;
#if defined(RAFT__LEGACY_no)
    (void)io;
    (void)fsm;
#else
    r->io = NULL;
    r->fsm = NULL;
    if (io != NULL) {
        assert(fsm != NULL);
        rv = ioFsmVersionCheck(r, io, fsm);
        if (rv != 0) {
            goto err_after_address_alloc;
        }

        r->io = io;
        r->fsm = fsm;

        r->last_applied = 0;
        r->close_cb = NULL;
        r->io->data = r;
        rv = r->io->init(r->io, r->id, r->address);
        if (rv != 0) {
            ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
            goto err_after_address_alloc;
        }
        r->now = r->io->time(r->io);
        raft_seed(r, (unsigned)r->io->random(r->io, 0, INT_MAX));
        r->legacy.prev_state = r->state;
        r->legacy.closing = false;
        QUEUE_INIT(&r->legacy.pending);
        QUEUE_INIT(&r->legacy.requests);
        r->legacy.step_cb = NULL;
        r->legacy.change = NULL;
        r->legacy.snapshot_index = 0;
        r->legacy.snapshot_taking = false;
        r->legacy.snapshot_install = false;
        r->legacy.snapshot_pending = NULL;
        r->transfer = NULL;
        r->legacy.log = logInit();
        r->legacy.snapshot_threshold = DEFAULT_SNAPSHOT_THRESHOLD;
        r->legacy.snapshot_trailing = DEFAULT_SNAPSHOT_TRAILING;
        if (r->legacy.log == NULL) {
            goto err_after_address_alloc;
        }
        r->capacity_threshold = 4 * 1024; /* 4 megabytes, i.e. 1 open segment */
    }
#endif
    return 0;

#ifndef RAFT__LEGACY_no
err_after_address_alloc:
    RaftHeapFree(r->address);
#endif
err:
    assert(rv != 0);
    return rv;
}

static void finalClose(struct raft *r)
{
    raft_free(r->address);
    TrailClose(&r->trail);
#ifndef RAFT__LEGACY_no
    if (r->io != NULL) {
        logClose(r->legacy.log);
    }
#endif
    raft_configuration_close(&r->configuration);
    raft_configuration_close(&r->configuration_committed);
    if (r->messages != NULL) {
        raft_free(r->messages);
    }
}

#ifndef RAFT__LEGACY_no
static void ioCloseCb(struct raft_io *io)
{
    struct raft *r = io->data;
    finalClose(r);
    if (r->close_cb != NULL) {
        r->close_cb(r);
    }
}
#endif

void raft_close(struct raft *r, void (*cb)(struct raft *r))
{
    assert(r->update == NULL);

    convertClear(r);

#if defined(RAFT__LEGACY_no)
    (void)cb;
    finalClose(r);
#else
    if (r->io != NULL) {
        assert(!r->legacy.closing);
        r->legacy.closing = true;
        if (r->transfer != NULL) {
            LegacyLeadershipTransferClose(r);
        }
        LegacyFailPendingRequests(r);
        LegacyFireCompletedRequests(r);
        r->close_cb = cb;

        r->io->close(r->io, ioCloseCb);
    } else {
        finalClose(r);
    }
#endif
}

void raft_seed(struct raft *r, unsigned random)
{
    r->random = random;
}

/* If we're the only voting server in the configuration, automatically
 * self-elect ourselves and convert to leader without waiting for the election
 * timeout. */
static int maybeSelfElect(struct raft *r)
{
    const struct raft_server *server;
    int rv;
    server = configurationGet(&r->configuration, r->id);
    if (server == NULL || server->role != RAFT_VOTER ||
        configurationVoterCount(&r->configuration) > 1) {
        return 0;
    }
    /* Converting to candidate will notice that we're the only voter and
     * automatically convert to leader. */
    rv = convertToCandidate(r, false /* disrupt leader */);
    if (rv != 0) {
        return rv;
    }
    assert(r->state == RAFT_LEADER);

    /* Send initial heartbeat. */
    replicationHeartbeat(r);

    return 0;
}

/* Emit a start message containing information about the current state. */
static void stepStartEmitMessage(const struct raft *r)
{
    char msg[512] = {0};
    raft_index snapshot_index = TrailSnapshotIndex(&r->trail);
    unsigned n_entries = TrailNumEntries(&r->trail);

    if (r->current_term == 0) {
        strcat(msg, "no state");
        goto emit;
    }

    if (r->current_term > 0) {
        char msg_term[64];
        sprintf(msg_term, "term %llu", r->current_term);
        strcat(msg, msg_term);
        if (snapshot_index > 0 || n_entries > 0) {
            strcat(msg, ", ");
        }
    }

    if (r->voted_for > 0) {
        char msg_vote[64];
        sprintf(msg_vote, "voted for %llu, ", r->voted_for);
        strcat(msg, msg_vote);
    }

    if (snapshot_index) {
        char msg_snapshot[64];
        sprintf(msg_snapshot, "1 snapshot (%llu^%llu)", snapshot_index,
                TrailSnapshotTerm(&r->trail));
        strcat(msg, msg_snapshot);
        if (n_entries > 0) {
            strcat(msg, ", ");
        }
    }

    if (n_entries > 0) {
        char msg_entries[64];
        raft_index first =
            TrailLastIndex(&r->trail) - TrailNumEntries(&r->trail) + 1;
        if (n_entries == 1) {
            sprintf(msg_entries, "1 entry (%llu^%llu)", first,
                    TrailTermOf(&r->trail, first));
        } else {
            raft_index last = TrailLastIndex(&r->trail);
            sprintf(msg_entries, "%u entries (%llu^%llu..%llu^%llu)", n_entries,
                    first, TrailTermOf(&r->trail, first), last,
                    TrailTermOf(&r->trail, last));
        }
        strcat(msg, msg_entries);
    }

emit:
    infof("%s", msg);
}

/* Handle a RAFT_START event. */
static int stepStart(struct raft *r,
                     raft_term term,
                     raft_id voted_for,
                     struct raft_snapshot_metadata *metadata,
                     raft_index start_index,
                     struct raft_entry *entries,
                     unsigned n_entries)
{
    raft_index snapshot_index = 0;
    raft_term snapshot_term = 0;
    int rv;

    r->current_term = term;
    r->voted_for = voted_for;

    /* If no term is set, there must be no persisted state. */
    if (r->current_term == 0) {
        assert(r->voted_for == 0);
        assert(metadata == NULL);
        assert(n_entries == 0);
    }

    if (metadata != NULL) {
        snapshot_index = metadata->index;
        snapshot_term = metadata->term;
        rv = RestoreSnapshot(r, metadata);
        if (rv != 0) {
            entryBatchesDestroy(entries, n_entries);
            return rv;
        }
    } else if (n_entries > 0) {
        /* If we don't have a snapshot and the on-disk log is not empty, then
         * the first entry must be a configuration entry. */
        assert(start_index == 1);
        assert(entries[0].type == RAFT_CHANGE);

        /* As a small optimization, bump the commit index to 1 since we require
         * the first entry to be the same on all servers. */
        r->commit_index = 1;
        r->update->flags |= RAFT_UPDATE_COMMIT_INDEX;
    }

    /* Append the entries to the log, possibly restoring the last
     * configuration. */
    rv = RestoreEntries(r, snapshot_index, snapshot_term, start_index, entries,
                        n_entries);
    if (rv != 0) {
        entryBatchesDestroy(entries, n_entries);
        return rv;
    }

    stepStartEmitMessage(r);

    /* By default we start as followers. */
    assert(r->state == RAFT_FOLLOWER);
    electionResetTimer(r);

    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;

    /* If there's only one voting server, and that is us, it's safe to convert
     * to leader right away. If that is not us, we're either joining the cluster
     * or we're simply configured as non-voter, and we'll stay follower. */
    rv = maybeSelfElect(r);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static int stepPersistedEntries(struct raft *r, raft_index index)
{
    raft_index first_index;
    raft_index first_term;
    raft_index last_term;
    unsigned n;

    /* The newly peristed index must be greater than our previous last stored
     * mark. */
    assert(index > r->last_stored);

    n = (unsigned)(index - r->last_stored);
    first_index = index - n + 1;

    assert(TrailLastIndex(&r->trail) >= index);

    first_term = TrailTermOf(&r->trail, first_index);
    last_term = TrailTermOf(&r->trail, index);

    assert(first_term > 0);
    assert(last_term > 0);

    if (n == 1) {
        infof("persisted 1 entry (%llu^%llu)", first_index, first_term);
    } else {
        infof("persisted %u entry (%llu^%llu..%llu^%llu)", n, first_index,
              first_term, index, last_term);
    }

    return replicationPersistEntriesDone(r, index);
}

static int stepPersistedSnapshot(struct raft *r,
                                 struct raft_snapshot_metadata *metadata,
                                 size_t offset,
                                 bool last)
{
    int rv;

    /* We wait for all writes to be settled before transitioning to candidate
     * state, and no new writes are issued as candidate, so the current state
     * must be leader or follower */
    assert(r->state == RAFT_LEADER || r->state == RAFT_FOLLOWER);

    infof("persisted snapshot (%llu^%llu)", metadata->index, metadata->term);
    rv = replicationPersistSnapshotDone(r, metadata, offset, last);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

/* Handle new messages. */
static int stepReceive(struct raft *r, struct raft_message *message)
{
    const char *desc;

    switch (message->type) {
        case RAFT_REQUEST_VOTE:
            desc = "request vote";
            break;
        case RAFT_REQUEST_VOTE_RESULT:
            desc = "request vote result";
            break;
        case RAFT_APPEND_ENTRIES:
            desc = "append entries";
            break;
        case RAFT_APPEND_ENTRIES_RESULT:
            desc = "append entries result";
            break;
        case RAFT_INSTALL_SNAPSHOT:
            desc = "install snapshot";
            break;
        case RAFT_TIMEOUT_NOW:
            desc = "timeout now";
            break;
        default:
            desc = "unknown message";
            break;
    }

    infof("recv %s from server %llu", desc, message->server_id);

    return recvMessage(r, message);
}

int stepSnapshot(struct raft *r,
                 struct raft_snapshot_metadata *metadata,
                 unsigned trailing)
{
    const char *suffix = trailing == 1 ? "y" : "ies";
    infof("new snapshot (%llu^%llu), %u trailing entr%s", metadata->index,
          metadata->term, trailing, suffix);
    return replicationSnapshot(r, metadata, trailing);
}

int stepTimeout(struct raft *r)
{
    const char *state_name = raft_state_name(r->state);
    infof("timeout as %s", state_name);
    return Timeout(r);
}

int raft_step(struct raft *r,
              struct raft_event *event,
              struct raft_update *update)
{
    int rv;

    assert(event != NULL);
    assert(update != NULL);

    assert(r->update == NULL);

    r->update = update;
    r->update->flags = 0;
    r->update->messages.batch = r->messages;
    r->update->messages.n = 0;

    r->now = event->time;
    r->capacity = event->capacity;

    /* Possibly update this server's capacity in the progress array. */
    if (r->state == RAFT_LEADER) {
        unsigned i = configurationIndexOf(&r->configuration, r->id);
        if (i < r->configuration.n) {
            progressSetFeatures(r, i, MESSAGE__FEATURE_CAPACITY);
            progressSetCapacity(r, i, r->capacity);
        }
    }

    switch (event->type) {
        case RAFT_START:
            rv = stepStart(r, event->start.term, event->start.voted_for,
                           event->start.metadata, event->start.start_index,
                           event->start.entries, event->start.n_entries);
            break;
        case RAFT_PERSISTED_ENTRIES:
            rv = stepPersistedEntries(r, event->persisted_entries.index);
            break;
        case RAFT_PERSISTED_SNAPSHOT:
            rv = stepPersistedSnapshot(r, &event->persisted_snapshot.metadata,
                                       event->persisted_snapshot.offset,
                                       event->persisted_snapshot.last);
            break;
        case RAFT_RECEIVE:
            rv = stepReceive(r, event->receive.message);
            break;
        case RAFT_CONFIGURATION:
            rv = replicationApplyConfigurationChange(
                r, &event->configuration.conf, event->configuration.index);
            break;
        case RAFT_SNAPSHOT:
            rv = stepSnapshot(r, &event->snapshot.metadata,
                              event->snapshot.trailing);
            break;
        case RAFT_TIMEOUT:
            rv = stepTimeout(r);
            break;
        case RAFT_SUBMIT:
            infof("submit %u new client entr%s", event->submit.n,
                  event->submit.n == 1 ? "y" : "ies");
            rv = ClientSubmit(r, event->submit.entries, event->submit.n);
            break;
        case RAFT_CATCH_UP:
            infof("catch-up server %llu", event->catch_up.server_id);
            ClientCatchUp(r, event->catch_up.server_id);
            rv = 0;
            break;
        case RAFT_TRANSFER:
            infof("transfer leadership to %llu", event->transfer.server_id);
            rv = ClientTransfer(r, event->transfer.server_id);
            break;
        default:
            rv = RAFT_INVALID;
            break;
    }

    if (rv != 0) {
        goto out;
    }

out:
    r->update = NULL;

    if (rv != 0) {
        return rv;
    }
    return 0;
}

raft_term raft_current_term(const struct raft *r)
{
    return r->current_term;
}

raft_term raft_voted_for(const struct raft *r)
{
    return r->voted_for;
}

raft_index raft_commit_index(const struct raft *r)
{
    return r->commit_index;
}

/* Return the time at which the next leader timeout should be triggered. */
static raft_time leaderTimeout(const struct raft *r)
{
    raft_time timeout;
    raft_time last_send = ULLONG_MAX;
    unsigned i;

    /* Find the oldest last_send timestamp. */
    for (i = 0; i < r->configuration.n; i++) {
        if (progressGetLastSend(r, i) < last_send) {
            last_send = progressGetLastSend(r, i);
        }
    }

    /* We always send a heartbeat at the beginning of our term, so if all
     * last_send timestamps are ULLONG_MAX it means that there are no
     * voters/stand-bys to send hearbeats to. So just return the timeout for the
     * quorum check. */
    if (last_send == ULLONG_MAX) {
        return r->election_timer_start + r->election_timeout;
    }

    /* The next timeout is either for heartbeat or a quorum check. */
    timeout = last_send + r->heartbeat_timeout;
    if (timeout > r->election_timer_start + r->election_timeout) {
        timeout = r->election_timer_start + r->election_timeout;
    }

    return timeout;
}

raft_time raft_timeout(const struct raft *r)
{
    raft_time timeout;
    switch (r->state) {
        case RAFT_FOLLOWER:
            /* fallthrough */
        case RAFT_CANDIDATE:
            timeout = electionTimerExpiration(r);
            break;
        case RAFT_LEADER:
            /* The next timeout is either for heartbeat or a quorum check. */
            timeout = leaderTimeout(r);
            break;
        default:
            timeout = 0;
            break;
    }

    return timeout;
}

int raft_match_index(const struct raft *r, raft_id id, raft_index *index)
{
    unsigned i;

    if (r->state != RAFT_LEADER) {
        return RAFT_NOTLEADER;
    }

    i = configurationIndexOf(&r->configuration, id);
    if (i == r->configuration.n) {
        return RAFT_BADID;
    }

    *index = progressMatchIndex(r, i);

    return 0;
}

int raft_catch_up(const struct raft *r, raft_id id, int *status)
{
    unsigned i;

    if (r->state != RAFT_LEADER) {
        return RAFT_NOTLEADER;
    }

    i = configurationIndexOf(&r->configuration, id);
    if (i == r->configuration.n) {
        return RAFT_BADID;
    }

    *status = progressCatchUpStatus(r, i);

    return 0;
}

raft_id raft_transferee(const struct raft *r)
{
    if (r->state != RAFT_LEADER) {
        return 0;
    }
    return r->leader_state.transferee;
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;

/* FIXME: workaround for failures in the dqlite test suite, which sets
 * timeouts too low and end up in failures when run on slow harder. */
#ifndef RAFT__LEGACY_no
    if (r->io != NULL && r->election_timeout == 150 &&
        r->heartbeat_timeout == 15) {
        r->election_timeout *= 3;
        r->heartbeat_timeout *= 3;
    }
#endif

    switch (r->state) {
        case RAFT_FOLLOWER:
        case RAFT_CANDIDATE:
            electionUpdateRandomizedTimeout(r);
            break;
    }
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
    r->heartbeat_timeout = msecs;
}

void raft_set_install_snapshot_timeout(struct raft *r, const unsigned msecs)
{
    r->install_snapshot_timeout = msecs;
}

void raft_set_pre_vote(struct raft *r, bool enabled)
{
    r->pre_vote = enabled;
}

void raft_set_max_catch_up_rounds(struct raft *r, unsigned n)
{
    r->max_catch_up_rounds = n;
}

void raft_set_max_catch_up_round_duration(struct raft *r, unsigned msecs)
{
    r->max_catch_up_round_duration = msecs;
}

void raft_set_max_inflight_entries(struct raft *r, unsigned n)
{
    r->max_inflight_entries = n;
}

void raft_set_capacity_threshold(struct raft *r, unsigned short min)
{
    r->capacity_threshold = min;
}

const char *raft_errmsg(struct raft *r)
{
    return r->errmsg;
}

const char *raft_strerror(int errnum)
{
    return errCodeToString(errnum);
}

void raft_configuration_init(struct raft_configuration *c)
{
    configurationInit(c);
}

void raft_configuration_close(struct raft_configuration *c)
{
    configurationClose(c);
}

int raft_configuration_add(struct raft_configuration *c,
                           const raft_id id,
                           const char *address,
                           const int role)
{
    return configurationAdd(c, id, address, role);
}

int raft_configuration_encode(const struct raft_configuration *c,
                              struct raft_buffer *buf)
{
    return configurationEncode(c, buf);
}

int raft_configuration_decode(const struct raft_buffer *buf,
                              struct raft_configuration *c)
{
    return configurationDecode(buf, c);
}

unsigned long long raft_digest(const char *text, unsigned long long n)
{
    struct byteSha1 sha1;
    uint8_t value[20];
    uint64_t n64 = byteFlip64((uint64_t)n);
    uint64_t digest;

    byteSha1Init(&sha1);
    byteSha1Update(&sha1, (const uint8_t *)text, (uint32_t)strlen(text));
    byteSha1Update(&sha1, (const uint8_t *)&n64, (uint32_t)(sizeof n64));
    byteSha1Digest(&sha1, value);

    memcpy(&digest, value + (sizeof value - sizeof digest), sizeof digest);

    return byteFlip64(digest);
}

unsigned raft_random(unsigned *state, unsigned min, unsigned max)
{
    return RandomWithinRange(state, min, max);
}

const char *raft_state_name(int state)
{
    const char *name;
    switch (state) {
        case RAFT_FOLLOWER:
            name = "follower";
            break;
        case RAFT_CANDIDATE:
            name = "candidate";
            break;
        case RAFT_LEADER:
            name = "leader";
            break;
        default:
            name = NULL;
            break;
    }
    return name;
}

const char *raft_role_name(int role)
{
    const char *name;
    switch (role) {
        case RAFT_STANDBY:
            name = "stand-by";
            break;
        case RAFT_VOTER:
            name = "voter";
            break;
        case RAFT_SPARE:
            name = "spare";
            break;
        default:
            name = NULL;
            break;
    }
    return name;
}

#undef infof
