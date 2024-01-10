/* Raft tracer that emits messages to stderr. */

#ifndef TEST_TRACER_H
#define TEST_TRACER_H

#include "../../include/raft.h"

#define FIXTURE_TRACER struct raft_tracer tracer
#define SET_UP_TRACER                \
    do {                             \
        f->tracer.emit = TracerEmit; \
        f->tracer.version = 2;       \
    } while (0)
#define TEAR_DOWN_TRACER

void TracerEmit(struct raft_tracer *t, int type, const void *info);

#endif /* TEST_TRACER_H */
