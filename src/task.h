/* Enqueue asynchronous tasks. */

#ifndef RAFT_TASK_H_
#define RAFT_TASK_H_

#include "../include/raft.h"

/* Create and enqueue a RAFT_SEND_MESSAGE task to send the given message to the
 * server with the given ID and address.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     The r->tasks array could not be resized to fit the new task.
 */
int TaskSendMessage(struct raft *r,
                    raft_id id,
                    const char *address,
                    struct raft_message *message);

/* Create and enqueue a RAFT_PERSIST_TERM_AND_VOTE task to persist the given
 * term and vote.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     The r->tasks array could not be resized to fit the new task.
 */
int TaskPersistTermAndVote(struct raft *r, raft_term term, raft_id voted_for);

#endif /* RAFT_TASK_H_ */
