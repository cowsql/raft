/* Enqueue messages to be sent. */

#ifndef RAFT_MESSAGE_H_
#define RAFT_MESSAGE_H_

#include "../include/raft.h"

/* Message types */
#define MESSAGE__REQUEST_VOTE_VERSION 2
#define MESSAGE__REQUEST_VOTE_RESULT_VERSION 2
#define MESSAGE__APPEND_ENTRIES_VERSION 0
#define MESSAGE__APPEND_ENTRIES_RESULT_VERSION 2
#define MESSAGE__INSTALL_SNAPSHOT_VERSION 0
#define MESSAGE__TIMEOUT_NOW_VERSION 0

/* Feature flags */
#define MESSAGE__FEATURE_CAPACITY 1 << 0

/* Add the given message to the array of messages attached to the struct
 * raft_update to be returned.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     The r->messages array could not be resized to fit the new message.
 */
int MessageEnqueue(struct raft *r, struct raft_message *message);

#endif /* RAFT_MESSAGE_H_ */
