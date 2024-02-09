/* Membership-related APIs. */

#ifndef MEMBERSHIP_H_
#define MEMBERSHIP_H_

#include "../include/raft.h"

/* XXX Internal code for transfer request objects. Used by the legacy layer to
 * differentiate between items in the legacy.requests queue. */
#define RAFT_TRANSFER_ (RAFT_CHANGE + 1)

/* Helper returning an error if the configuration can't be changed,
 *
 * Errors:
 *
 * RAFT_CANTCHANGE
 *     A configuration change or a promotion are in progress.
 */
int membershipCanChangeConfiguration(struct raft *r);

/* Update the information about the progress that the non-voting server
 * currently being promoted is making in catching with logs.
 *
 * Return false if the server being promoted did not yet catch-up with logs, and
 * true if it did.
 *
 * This function must be called only by leaders after a @raft_assign request
 * has been submitted. */
bool membershipUpdateCatchUpRound(struct raft *r);

/* Update the local configuration replacing it with the content of the given
 * RAFT_CHANGE entry, which has just been received in as part of a RAFT_SUBMIT
 * event on a leader or a RAFT_RECEIVE event of an AppendEntries message on a
 * follower. The uncommitted configuration index will be updated accordingly.
 *
 * It must be called only by followers or leaders.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     A new raft_configuration object to hold the decoded configuration could
 *     not be allocated.
 *
 * RAFT_MALFORMED
 *     The entry data does not contain a valid encoded configuration.
 */
int membershipUncommittedChange(struct raft *r,
                                const raft_index index,
                                const struct raft_entry *entry);

/* Rollback an uncommitted configuration change that was applied locally, but
 * failed to be committed. It must be called by followers after they receive an
 * AppendEntries RPC request that instructs them to evict the uncommitted entry
 * from their log.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     A copy of the last committed configuration to rollback to could not be
 *     made.
 */
int membershipRollback(struct raft *r);

/* Start the leadership transfer by sending a TimeoutNow message to the target
 * server.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     The TimeoutNow message could not be enqueued.
 */
int membershipLeadershipTransferStart(struct raft *r);

#endif /* MEMBERSHIP_H_ */
