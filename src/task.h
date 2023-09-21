/* Enqueue asynchronous tasks. */

#ifndef RAFT_TASK_H_
#define RAFT_TASK_H_

#include "../include/raft.h"

/* Create and enqueue a raft_persist_term_and_vote task to persist the given
 * term and vote.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     The r->tasks array could not be resized to fit the new task.
 */
int TaskPersistTermAndVote(struct raft *r, raft_term term, raft_id voted_for);

#endif /* RAFT_TASK_H_ */
