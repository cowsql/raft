#ifndef CLIENT_H_
#define CLIENT_H_

#include "../include/raft.h"

/* Submit the given entries and start replicating them. */
int ClientSubmit(struct raft *r, struct raft_entry *entries, unsigned n);

/* Apply a barrier entry.
 *
 * This function holds the common logic shared by raft_barrier() and
 * convertToLeader().
 *
 * XXX: This is needed because calling raft_barrier directly in convertToLeader
 * would in turn call raft_step() again via LegacyForwardToRaftIo. */
int clientBarrier(struct raft *r, struct raft_barrier *req, raft_barrier_cb cb);

#endif /* CLIENT_H_ */
