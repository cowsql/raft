/* Logic to be invoked periodically. */

#ifndef TIMEOUT_H_
#define TIMEOUT_H_

#include "../include/raft.h"

/* Called when upon RAFT_TIMEOUT events. */
int Timeout(struct raft *r);

#endif /* TIMEOUT_H_ */
