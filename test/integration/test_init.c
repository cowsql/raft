#include "../../include/raft.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * raft_init
 *
 *****************************************************************************/

SUITE(raft_init)

TEST(raft_init, ioVersionNotSet, NULL, NULL, 0, NULL)
{
    struct raft r = {0};
    struct raft_io io = {0};
    struct raft_fsm fsm = {0};
    io.version = 0;
    fsm.version = 3;

    int rc;
    rc = raft_init(&r, &io, &fsm, 1, "1");
    munit_assert_int(rc, ==, -1);
    munit_assert_string_equal(r.errmsg, "io->version must be set");
    return MUNIT_OK;
}

TEST(raft_init, fsmVersionNotSet, NULL, NULL, 0, NULL)
{
    struct raft r = {0};
    struct raft_io io = {0};
    struct raft_fsm fsm = {0};
    io.version = 2;
    fsm.version = 0;

    int rc;
    rc = raft_init(&r, &io, &fsm, 1, "1");
    munit_assert_int(rc, ==, -1);
    munit_assert_string_equal(r.errmsg, "fsm->version must be set");
    return MUNIT_OK;
}
