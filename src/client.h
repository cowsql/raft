#ifndef CLIENT_H_
#define CLIENT_H_

#include "../include/raft.h"

/* Submit the given entries and start replicating them. */
int ClientSubmit(struct raft *r, struct raft_entry *entries, unsigned n);

/* Start catching-up the given server. */
void ClientCatchUp(struct raft *r, raft_id server_id);

/* Start transferring leadership to the given server. */
int ClientTransfer(struct raft *r, raft_id server_id);

/* Submit a new RAFT_CHANGE entry with the given new configuration. */
int ClientChangeConfiguration(struct raft *r,
                              const struct raft_configuration *configuration);

#endif /* CLIENT_H_ */
