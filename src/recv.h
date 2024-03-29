/* Receive an RPC message. */

#ifndef RECV_H_
#define RECV_H_

#include "../include/raft.h"

/* Function to be invoked upon receiving an RPC message. */
int recvMessage(struct raft *r, struct raft_message *message);

/* Compare a request's term with the server's current term.
 *
 * Return 0 if the local term matches the request's term, to -1 if the request's
 * term is lower, and to 1 if the request's term is higher. */
int recvCheckMatchingTerms(const struct raft *r, raft_term term);

/* Bump the current term and possibly step down from candidate or leader
 * state. */
void recvBumpCurrentTerm(struct raft *r, raft_term term);

/* Common logic for RPC handlers, comparing the request's term with the server's
 * current term and possibly deciding to reject the request or step down from
 * candidate or leader.
 *
 * From Section 3.3:
 *
 *   If a candidate or leader discovers that its term is out of date, it
 *   immediately reverts to follower state. If a server receives a request with
 *   a stale term number, it rejects the request.
 *
 * The return value will be set to 0 if the local term matches the request's
 * term, to -1 if the request's term is lower, and to 1 if the request's term
 * was higher and we have bumped the local one to match it (and stepped down to
 * follower in that case, if we were not follower already). */
int recvEnsureMatchingTerms(struct raft *r, raft_term term);

/* If different from the current one, update information about the current
 * leader. Must be called only by followers.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     A copy of @address could not be made
 */
int recvUpdateLeader(struct raft *r, raft_id id, const char *address);

#endif /* RECV_H_ */
