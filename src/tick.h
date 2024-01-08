/* Logic to be invoked periodically. */

#ifndef TICK_H_
#define TICK_H_

#include "../include/raft.h"

/* Called when upon RAFT_TIMEOUT events. */
int Tick(struct raft *r);

#endif /* TICK_H_ */
