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
int TaskSendMessage(struct raft *r, struct raft_message *message);

#endif /* RAFT_TASK_H_ */
