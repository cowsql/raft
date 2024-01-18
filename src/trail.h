#ifndef RAFT_TRAIL_H_
#define RAFT_TRAIL_H_

#include "../include/raft.h"

#endif /* RAFT_TRAIL_H_ */

/* Initialize an empty trail of raft log entries. */
void TrailInit(struct raft_trail *t);

/* Release all memory used by the given trail object. */
void TrailClose(struct raft_trail *t);

/* Called at startup when populating the trail with data of entries loaded from
 * disk. It sets the starting state of the trail. The start index must be lower
 * or equal than snapshot_index + 1. */
void TrailStart(struct raft_trail *t,
                raft_index snapshot_index,
                raft_term snapshot_term,
                raft_index start_index);

/* Get the current number of entries in the log (either already persisted or
 * being persisted). */
unsigned TrailNumEntries(const struct raft_trail *t);

/* Get the index of the last entry in the log. Return 0 if the log is empty. */
raft_index TrailLastIndex(const struct raft_trail *t);

/* Get the term of the last entry in the log. Return 0 if the log is empty. */
raft_term TrailLastTerm(const struct raft_trail *t);

/* Get the term of the entry with the given index. Return 0 if index is greater
 * than the last index of the log, or if it's lower than oldest index we know
 * the term of (either because it's outstanding or because it's the last entry
 * in the most recent snapshot). */
raft_term TrailTermOf(const struct raft_trail *t, raft_index index);

/* Record a new entry at TrailLastIndex() + 1, with the given term.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     Memory for the records array could not be allocated.
 */
int TrailAppend(struct raft_trail *t, raft_term term);

/* Delete all entries from the given index (included) onwards. If the log is
 * empty this is a no-op. If @index is lower than or equal to the index of the
 * first entry in the log, then the log will become empty. */
void TrailTruncate(struct raft_trail *t, raft_index index);

/* To be called when taking a new snapshot. The log must contain an entry at
 * last_index, which is the index of the last entry included in the
 * snapshot. The function will update the last snapshot information and delete
 * all entries up to last_index - trailing (included). If the log contains no
 * entry at last_index - trailing, then no entry will be deleted. */
void TrailSnapshot(struct raft_trail *t,
                   raft_index last_index,
                   unsigned trailing);

/* Get the index of the last entry in the most recent snapshot. Return #0 if
 * there are no snapshots. */
raft_index TrailSnapshotIndex(const struct raft_trail *t);

/* Get the term of the last entry of the most recent snapshot. Return #0 if
 * there are no snapshots. */
raft_term TrailSnapshotTerm(const struct raft_trail *t);

/* To be called when installing a snapshot.
 *
 * The log can be in any state. All outstanding entries will be discarded, the
 * last index and last term of the most recent snapshot will be set to the given
 * values, and the offset adjusted accordingly. */
void TrailRestore(struct raft_trail *t,
                  raft_index last_index,
                  raft_term last_term);

/* Return true if there is an entry at the given index. */
bool TrailHasEntry(const struct raft_trail *t, raft_index index);
