#ifndef CLIENT_H_
#define CLIENT_H_

#include "../include/raft.h"

/* Submit the given entries and start replicating them. */
int ClientSubmit(struct raft *r, struct raft_entry *entries, unsigned n);

#endif /* CLIENT_H_ */
