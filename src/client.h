#ifndef CLIENT_H_
#define CLIENT_H_

#include "../include/raft.h"

/* Submit the given entries and start replicating them. */
int ClientSubmit(struct raft *r, struct raft_entry *entries, unsigned n);

/* Start catching-up the given server. */
void ClientCatchUp(struct raft *r, raft_id server_id);

#endif /* CLIENT_H_ */
