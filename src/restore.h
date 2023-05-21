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

#endif /* RAFT_RESTORE_H_ */
