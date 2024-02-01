#include "trail.h"
#include "assert.h"

void TrailInit(struct raft_trail *t)
{
    t->records = NULL;
    t->size = 0;
    t->front = t->back = 0;
    t->offset = 0;
    t->snapshot.index = 0;
    t->snapshot.term = 0;
}

void TrailClose(struct raft_trail *t)
{
    if (t->records != NULL) {
        raft_free(t->records);
    }
}

void TrailStart(struct raft_trail *t,
                raft_index snapshot_index,
                raft_term snapshot_term,
                raft_index start_index)
{
    assert(TrailNumEntries(t) == 0);
    assert(start_index > 0);
    assert(start_index <= snapshot_index + 1);
    assert(snapshot_index == 0 || snapshot_term != 0);

    t->snapshot.index = snapshot_index;
    t->snapshot.term = snapshot_term;
    t->offset = start_index - 1;
}

/* Get the current number of records in the trail. */
static unsigned trailNumRecords(const struct raft_trail *t)
{
    /* The circular buffer is not wrapped. */
    if (t->front <= t->back) {
        return t->back - t->front;
    }

    /* The circular buffer is wrapped. */
    return t->size - t->front + t->back;
}

/* Return the circular buffer position of the i'th record in the log. */
static unsigned trailPositionAt(const struct raft_trail *t, unsigned i)
{
    return (t->front + i) % t->size;
}

unsigned TrailNumEntries(const struct raft_trail *t)
{
    unsigned n = trailNumRecords(t);
    unsigned i;

    if (n == 0) {
        return 0;
    }

    i = trailPositionAt(t, n - 1);

    assert(t->records[i].index > 0);
    assert(t->records[i].index > t->offset);

    return (unsigned)(t->records[i].index - t->offset);
}

raft_index TrailLastIndex(const struct raft_trail *t)
{
    unsigned n = TrailNumEntries(t);

    /* If there are no entries in the log, but there is a snapshot available
     * check that it's last index is consistent with the offset. */
    if (n == 0) {
        if (t->snapshot.index != 0) {
            assert(t->offset <= t->snapshot.index);
            return t->snapshot.index;
        }
        return 0;
    }
    return t->offset + n;
}

raft_term TrailLastTerm(const struct raft_trail *t)
{
    raft_index last_index;
    last_index = TrailLastIndex(t);
    return last_index > 0 ? TrailTermOf(t, last_index) : 0;
}

raft_term TrailTermOf(const struct raft_trail *t, raft_index index)
{
    unsigned n;
    unsigned i;
    unsigned j;

    assert(index > 0);
    assert(t->offset <= t->snapshot.index);

    /* If the given index is lower than the first index, or higher than the last
     * index, return 0, unless there is a matching snapshot. */
    if (index < t->offset + 1 || index > t->offset + TrailNumEntries(t)) {
        if (index == t->snapshot.index) {
            return t->snapshot.term;
        }
        return 0;
    }

    /* Go through all records, starting from the last one, looking for a record
     * whose previous record has an index lower than the given one. Stop when we
     * find such a record, or when we reach the first record, which has no
     * previous record. */
    n = trailNumRecords(t);
    assert(n > 0);
    do {
        i = trailPositionAt(t, n - 1);
        assert(index <= t->records[i].index);

        if (n == 1) {
            break;
        }

        j = trailPositionAt(t, n - 2);
        if (index > t->records[j].index) {
            break;
        }

        n -= 1;
    } while (1);

    return t->records[i].term;
}

/* Ensure that the last record in the circular buffer is at the given term,
 * creating a new record if necessary.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     Memory for the records array could not be allocated.
 */
static int trailEnsureRecord(struct raft_trail *t, raft_term term)
{
    unsigned size;
    unsigned n;
    unsigned i;
    /* clang-format off */
    struct
    {
        raft_index index;
        raft_term term;
    } *records;
    /* clang-format on */

    n = trailNumRecords(t);

    /* If there are already some records, and the term of the last record
     * matches the given one, then there's nothing to do, as we already have a
     * record for the given term. */
    if (n > 0) {
        i = trailPositionAt(t, n - 1);
        assert(t->records[i].term <= term);
        if (t->records[i].term == term) {
            return 0;
        }
    }

    /* If the circular buffer is big enough to hold an additional record, just
     * append it to the back. */
    if (n + 1 < t->size) {
        t->back += 1;
        t->back = t->back % t->size;
        goto out;
    }

    /* Otherwise we need to resize the circular buffer.
     *
     * Make the new size twice the current size plus one (for the record
     * associated with the new term). Over-allocating now avoids smaller
     * allocations later. */
    size = (t->size + 1) * 2;
    records = raft_calloc(size, sizeof *t->records);

    if (records == NULL) {
        return RAFT_NOMEM;
    }

    /* Copy all active old records to the beginning of the newly allocated
     * array. */
    for (i = 0; i < n; i++) {
        unsigned j = trailPositionAt(t, i);
        records[i].index = t->records[j].index;
        records[i].term = t->records[j].term;
    }

    /* Release the old records array. */
    if (t->records != NULL) {
        raft_free(t->records);
    }

    t->records = (void *)records;
    t->size = size;
    t->front = 0;
    t->back = n + 1;

out:
    i = trailPositionAt(t, n);

    t->records[i].index = 0;
    t->records[i].term = term;

    return 0;
}

int TrailAppend(struct raft_trail *t, raft_term term)
{
    unsigned n;
    unsigned i;
    unsigned j;
    int rv;

    rv = trailEnsureRecord(t, term);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        return rv;
    }

    n = trailNumRecords(t);
    i = trailPositionAt(t, n - 1);

    /* If we have already recorded an index for this term, then the next index
     * is just that index plus 1. */
    if (t->records[i].index > 0) {
        t->records[i].index += 1;
        goto out;
    }

    assert(t->records[i].index == 0);

    /* Otherwise, if there is a previous record, then the next index is the
     * index of the previous record plus 1. */
    if (n > 1) {
        j = trailPositionAt(t, n - 2);
        assert(t->records[j].index > 0);
        t->records[i].index = t->records[j].index + 1;
        goto out;
    }

    /* Otherwise, we're appending the very first entry to an empty trail. */
    t->records[i].index = t->offset + 1;

out:
    return 0;
}

void TrailTruncate(struct raft_trail *t, const raft_index index)
{
    raft_index last;
    unsigned n;
    unsigned i;
    unsigned j;

    assert(index > t->offset);
    assert(index <= TrailLastIndex(t));

    /* Delete all entries, starting from the last, down to the given index
     * included. */
    for (last = TrailLastIndex(t); last >= index; last--) {
        n = trailNumRecords(t);
        i = trailPositionAt(t, n - 1);

        /* The record must be valid */
        assert(t->records[i].index > 0);
        assert(t->records[i].term > 0);

        /* The record must refer to the entry being deleted. */
        assert(t->records[i].index == last);

        if (n == 1) {
            /* If we're deleting the very last entry of the trail, clear
             * everything. */
            if (last == t->offset + 1) {
                t->records[i].index = 0;
                t->records[i].term = 0;
                t->front = t->back = 0;
                return;
            }

            /* Otherwise just delete this entry. */
            t->records[i].index -= 1;
            assert(t->records[i].index > 0);
            continue;
        }

        j = trailPositionAt(t, n - 2);

        /* If the preceding index belongs to the previous record, clear the
         * current record. */
        if (last - 1 == t->records[j].index) {
            t->records[i].index = 0;
            t->records[i].term = 0;
            if (t->back == 0) {
                t->back = t->size - 1;
            } else {
                t->back -= 1;
            }
            continue;
        }

        /* Otherwise just delete this entry. */
        t->records[i].index -= 1;
        assert(t->records[i].index > 0);
    }
}

/* Delete all entries up to the given index (included). */
static void trailRemovePrefix(struct raft_trail *t, const raft_index index)
{
    unsigned n = trailNumRecords(t);
    unsigned i;
    unsigned j;
    unsigned front = t->front;
    raft_index record_index;

    assert(index > 0);
    assert(index <= TrailLastIndex(t));

    for (i = 0; i < n; i++) {
        j = trailPositionAt(t, i);

        record_index = t->records[j].index;

        /* If the index belongs to this record, and there are larger indexes
         * too, just update the starting offset, but leave the record. */
        if (record_index > index) {
            break;
        }

        /* Drop the entire record. */
        t->records[j].index = 0;
        t->records[j].term = 0;
        front += 1;

        /* If the index is the last one of the record, stop here, the next
         * record will be unchanged. */
        if (record_index == index) {
            break;
        }
    }

    t->front = front % t->size;
    t->offset = index;
}

void TrailSnapshot(struct raft_trail *t,
                   const raft_index last_index,
                   const unsigned trailing)
{
    raft_term last_term;

    assert(last_index > 0);

    /* We must not already have a snapshot at this index */
    assert(t->snapshot.index != last_index);

    /* We must have an entry at this index */
    last_term = TrailTermOf(t, last_index);
    assert(last_term != 0);

    t->snapshot.index = last_index;
    t->snapshot.term = last_term;

    /* If we have not at least trailing entries preceeding the given last index,
     * then there's nothing to remove and we're done. */
    if (last_index <= trailing || TrailTermOf(t, last_index - trailing) == 0) {
        return;
    }

    trailRemovePrefix(t, last_index - trailing);
}

raft_index TrailSnapshotIndex(const struct raft_trail *t)
{
    return t->snapshot.index;
}

raft_term TrailSnapshotTerm(const struct raft_trail *t)
{
    return t->snapshot.term;
}

void TrailRestore(struct raft_trail *t,
                  raft_index last_index,
                  raft_term last_term)
{
    size_t n = TrailNumEntries(t);
    assert(last_index > 0);
    assert(last_term > 0);
    if (n > 0) {
        TrailTruncate(t, TrailLastIndex(t) - n + 1);
    }
    t->snapshot.index = last_index;
    t->snapshot.term = last_term;
    t->offset = last_index;
}

bool TrailHasEntry(const struct raft_trail *t, raft_index index)
{
    unsigned n = trailNumRecords(t);
    unsigned i;

    /* If there are no records, there are no entries. */
    if (n == 0) {
        return false;
    }

    /* If the index is lower than the offset, then it's not in the log.*/
    if (index <= t->offset) {
        return false;
    }

    /* If the index is greater than the last record, then it's not in the log.*/
    i = trailPositionAt(t, n - 1);
    assert(t->records[i].index > 0);
    if (index > t->records[i].index) {
        return false;
    }

    return true;
}
