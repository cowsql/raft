#include "../../src/trail.h"
#include "../lib/heap.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    struct raft_trail trail;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SET_UP_HEAP;
    TrailInit(&f->trail);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TrailClose(&f->trail);
    TEAR_DOWN_HEAP;
    free(f);
}

SUITE(trail)

/* An empty trail has recorded any entry yet. */
TEST(trail, Empty, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    munit_assert_uint(TrailNumEntries(&f->trail), ==, 0);
    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 0);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 0);
    munit_assert_false(TrailHasEntry(&f->trail, 1));
    return MUNIT_OK;
}

/* Anchor the trail at a certain index and snapshot. */
TEST(trail, Start, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailStart(&f->trail, 3 /* snapshot index */, 4 /* snapshot term */,
               2 /* start index */);

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 0);
    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 3);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 4);

    TrailAppend(&f->trail, 3); /* index 2, term 3 */
    TrailAppend(&f->trail, 4); /* index 3, term 4 */
    TrailAppend(&f->trail, 4); /* index 4, term 4 */

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 3);
    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 4);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 4);

    munit_assert_ullong(TrailTermOf(&f->trail, 1), ==, 0); /* non existing */

    munit_assert_ullong(TrailTermOf(&f->trail, 2), ==, 3);
    munit_assert_ullong(TrailTermOf(&f->trail, 3), ==, 4);
    munit_assert_ullong(TrailTermOf(&f->trail, 4), ==, 4);

    munit_assert_ullong(TrailTermOf(&f->trail, 5), ==, 0); /* non existing */

    return MUNIT_OK;
}

/* Append to an empty trail information about a new entry. */
TEST(trail, AppendEmpty, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailAppend(&f->trail, 1); /* index 1, term 1 */

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 1);

    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 1);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 1);

    munit_assert_ullong(TrailTermOf(&f->trail, 1), ==, 1);

    return MUNIT_OK;
}

/* Append information about a new entry with the same term as the last one. */
TEST(trail, AppendSameTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailAppend(&f->trail, 1); /* index 1, term 1 */
    TrailAppend(&f->trail, 1); /* index 2, term 1 */

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 2);

    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 2);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 1);

    munit_assert_ullong(TrailTermOf(&f->trail, 1), ==, 1);
    munit_assert_ullong(TrailTermOf(&f->trail, 2), ==, 1);

    return MUNIT_OK;
}

/* Append information about a new entry with a newer term than the last one. */
TEST(trail, AppendNewerTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailAppend(&f->trail, 1); /* index 1, term 1 */
    TrailAppend(&f->trail, 2); /* index 2, term 2 */

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 2);

    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 2);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 2);

    munit_assert_ullong(TrailTermOf(&f->trail, 1), ==, 1);
    munit_assert_ullong(TrailTermOf(&f->trail, 2), ==, 2);

    return MUNIT_OK;
}

/* Get the term of an entry. */
TEST(trail, TermOf, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailAppend(&f->trail, 1); /* index 1, term 1 */
    TrailAppend(&f->trail, 1); /* index 2, term 1 */
    TrailAppend(&f->trail, 2); /* index 3, term 2 */
    TrailAppend(&f->trail, 3); /* index 4, term 3 */

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 4);

    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 4);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 3);

    munit_assert_ullong(TrailTermOf(&f->trail, 1), ==, 1);
    munit_assert_ullong(TrailTermOf(&f->trail, 2), ==, 1);
    munit_assert_ullong(TrailTermOf(&f->trail, 3), ==, 2);
    munit_assert_ullong(TrailTermOf(&f->trail, 4), ==, 3);

    return MUNIT_OK;
}

/* Truncate the trail removing information about all entries past the given
 * index (included). */
TEST(trail, Truncate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailAppend(&f->trail, 1); /* index 1, term 1 */
    TrailAppend(&f->trail, 1); /* index 2, term 1 */
    TrailAppend(&f->trail, 2); /* index 3, term 2 */
    TrailAppend(&f->trail, 3); /* index 4, term 3 */
    TrailAppend(&f->trail, 3); /* index 5, term 3 */

    TrailTruncate(&f->trail, 3);

    munit_assert_true(TrailHasEntry(&f->trail, 2));
    munit_assert_false(TrailHasEntry(&f->trail, 3));

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 2);

    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 2);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 1);

    TrailTruncate(&f->trail, 1);

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 0);

    return MUNIT_OK;
}

/* Truncate a trail that was wrapped. */
TEST(trail, TruncateWrapped, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailAppend(&f->trail, 1); /* index 1, term 1 */
    TrailAppend(&f->trail, 1); /* index 2, term 1 */
    TrailAppend(&f->trail, 2); /* index 3, term 2 */
    TrailAppend(&f->trail, 3); /* index 4, term 3 */
    TrailAppend(&f->trail, 3); /* index 5, term 3 */
    TrailAppend(&f->trail, 4); /* index 6, term 4 */
    TrailAppend(&f->trail, 4); /* index 7, term 4 */
    TrailAppend(&f->trail, 5); /* index 8, term 5 */
    TrailAppend(&f->trail, 5); /* index 9, term 5 */

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 9);

    TrailSnapshot(&f->trail, 7 /* snapshot index */, 0 /* trailing */);
    munit_assert_uint(TrailNumEntries(&f->trail), ==, 2);

    TrailAppend(&f->trail, 6); /* index 10, term 6 */
    TrailAppend(&f->trail, 6); /* index 11, term 6 */
    TrailAppend(&f->trail, 7); /* index 12, term 7 */
    TrailAppend(&f->trail, 8); /* index 13, term 8 */
    TrailAppend(&f->trail, 8); /* index 14, term 8 */
    TrailAppend(&f->trail, 8); /* index 15, term 8 */
    TrailAppend(&f->trail, 9); /* index 16, term 9 */

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 9);

    TrailTruncate(&f->trail, 8);

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 0);

    return MUNIT_OK;
}

/* Remove a prefix of the log. */
TEST(trail, Snapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailAppend(&f->trail, 1); /* index 1, term 1 */
    TrailAppend(&f->trail, 1); /* index 2, term 1 */
    TrailAppend(&f->trail, 2); /* index 3, term 2 */
    TrailAppend(&f->trail, 3); /* index 4, term 3 */
    TrailAppend(&f->trail, 3); /* index 5, term 3 */

    TrailSnapshot(&f->trail, 3 /* snapshot index */, 2 /* trailing */);

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 4);

    munit_assert_ullong(TrailTermOf(&f->trail, 1), ==, 0);
    munit_assert_ullong(TrailTermOf(&f->trail, 2), ==, 1);

    munit_assert_false(TrailHasEntry(&f->trail, 1));
    munit_assert_true(TrailHasEntry(&f->trail, 2));

    TrailAppend(&f->trail, 4); /* index 6, term 4 */
    TrailAppend(&f->trail, 4); /* index 7, term 4 */

    TrailSnapshot(&f->trail, 6 /* snapshot index */, 1 /* trailing */);

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 2);

    munit_assert_ullong(TrailTermOf(&f->trail, 5), ==, 0);
    munit_assert_ullong(TrailTermOf(&f->trail, 6), ==, 4);

    return MUNIT_OK;
}

/* Restore a snapshot erasing the whole log. */
TEST(trail, Restore, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    TrailAppend(&f->trail, 1); /* index 1, term 1 */
    TrailAppend(&f->trail, 1); /* index 2, term 1 */
    TrailAppend(&f->trail, 2); /* index 3, term 2 */
    TrailAppend(&f->trail, 3); /* index 4, term 3 */

    TrailRestore(&f->trail, 3 /* snapshot index */, 2 /* snapshot term */);

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 0);

    munit_assert_ullong(TrailLastIndex(&f->trail), ==, 3);
    munit_assert_ullong(TrailLastTerm(&f->trail), ==, 2);

    TrailAppend(&f->trail, 3); /* index 4, term 3 */

    munit_assert_uint(TrailNumEntries(&f->trail), ==, 1);
    munit_assert_ullong(TrailTermOf(&f->trail, 4), ==, 3);

    return MUNIT_OK;
}
