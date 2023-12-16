/* Logic to be invoked periodically. */

#ifndef TICK_H_
#define TICK_H_

#include "../include/raft.h"

/* Called when upon RAFT_TIMEOUT events. */
int Tick(struct raft *r);

/* Callback to be passed to the @raft_io implementation. It notifies us that a
 * certain amount of time has elapsed and will be invoked periodically. */
void tickCb(struct raft_io *io);

#endif /* TICK_H_ */
