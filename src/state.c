#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "queue.h"
#include "trail.h"

enum raft_state raft_state(struct raft *r)
{
    return r->state;
}

void raft_leader(struct raft *r, raft_id *id, const char **address)
{
    switch (r->state) {
        case RAFT_CANDIDATE:
            *id = 0;
            *address = NULL;
            return;
        case RAFT_FOLLOWER:
            *id = r->follower_state.current_leader.id;
            *address = r->follower_state.current_leader.address;
            return;
        case RAFT_LEADER:
            if (r->leader_state.transferee != 0) {
                *id = 0;
                *address = NULL;
                return;
            }
            *id = r->id;
            *address = r->address;
            return;
    }
}

raft_index raft_last_index(struct raft *r)
{
    return TrailLastIndex(&r->trail);
}

int raft_role(struct raft *r)
{
    const struct raft_server *local =
        configurationGet(&r->configuration, r->id);
    if (local == NULL) {
        return -1;
    }
    return local->role;
}
