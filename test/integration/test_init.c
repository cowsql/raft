#include "../../include/raft.h"
#include "../lib/heap.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture holding an unitialized raft object.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    struct raft raft;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SET_UP_HEAP;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_HEAP;
    free(f);
}

/******************************************************************************
 *
 * raft_init
 *
 *****************************************************************************/

SUITE(raft_init)

/* The io and fsm objects can be set to NULL */
TEST(raft_init, nullFsmAndIo, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int rv;
    rv = raft_init(&f->raft, NULL, NULL, 1, "1");
    munit_assert_int(rv, ==, 0);
    raft_close(&f->raft, NULL);
    return MUNIT_OK;
}

static char *oom_heap_fault_delay[] = {"0", NULL};
static char *oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory failures. */
TEST(raft_init, oom, setUp, tearDown, 0, oom_params)
{
    struct fixture *f = data;
    int rv;
    HEAP_FAULT_ENABLE;
    rv = raft_init(&f->raft, NULL, NULL, 1, "1");
    munit_assert_int(rv, ==, RAFT_NOMEM);
    munit_assert_string_equal(raft_errmsg(&f->raft), "out of memory");
    return MUNIT_OK;
}
