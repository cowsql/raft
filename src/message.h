/* Enqueue messages to be sent. */

#ifndef RAFT_MESSAGE_H_
#define RAFT_MESSAGE_H_

#include "../include/raft.h"

/* Add the given message to the array of messages attached to next struct
 * raft_update to be returned.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     The r->messages array could not be resized to fit the new message.
 */
int MessageEnqueue(struct raft *r, struct raft_message *message);

#endif /* RAFT_MESSAGE_H_ */
