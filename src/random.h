/* Pseudo-random number generator. */

#ifndef RAFT_RANDOM_H_
#define RAFT_RANDOM_H_

/* Generate a random number between min and max. */
unsigned RandomWithinRange(unsigned *state, unsigned min, unsigned max);

#endif /* RAFT_RANDOM_H_ */
