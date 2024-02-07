#ifndef CLIENT_H_
#define CLIENT_H_

#include "../include/raft.h"

/* Submit the given entries and start replicating them.
 *
 * Errors:
 *
 * RAFT_NOTLEADER
 *     The server is not leader, or a leadership transfer is in progress.
 *
 * RAFT_SPACE
 *     Not enough servers to form a majority are reporting to have remaining
 *     capacity over the configured threshold.
 *
 * RAFT_MALFORMED
 *     The submitted entry is of type RAFT_CHANGE, but the encoded configuration
 *     is invalid.
 *
 * RAFT_NOMEM
 *     Memory could not be allocated to store the new entry.
 */
int ClientSubmit(struct raft *r, struct raft_entry *entries, unsigned n);

/* Start catching-up the given server. */
void ClientCatchUp(struct raft *r, raft_id server_id);

/* Start transferring leadership to the given server. */
int ClientTransfer(struct raft *r, raft_id server_id);

#endif /* CLIENT_H_ */
