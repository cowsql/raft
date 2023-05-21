/* Restore the in-memory raft state with data loaded from persistent storage. */

#ifndef RAFT_RESTORE_H_
#define RAFT_RESTORE_H_

#include "../include/raft.h"

/* Restore the entries that were loaded from persistent storage. The most recent
 * configuration entry will be restored as well, if any. */
int RestoreEntries(struct raft *r,
                   raft_index snapshot_index,
                   raft_term snapshot_term,
                   raft_index start_index,
                   struct raft_entry *entries,
                   unsigned n);

/* Function to be called when restoring a snapshot.
 *
 * This will reset the current state of the raft object as if the last entry
 * contained in the snapshot with the given metadata had just been persisted,
 * committed and applied.
 *
 * The in-memory log must be empty when calling this function. */
int RestoreSnapshot(struct raft *r, struct raft_snapshot_metadata *metadata);

#endif /* RAFT_RESTORE_H_ */
