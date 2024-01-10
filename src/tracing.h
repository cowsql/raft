/* Tracing functions and helpers. */

#ifndef TRACING_H_
#define TRACING_H_

#include <stdio.h>

#include "../include/raft.h"
#include "utils.h"

/* If an env var with this name is found, tracing can be enabled */
#define LIBRAFT_TRACE "LIBRAFT_TRACE"

extern struct raft_tracer NoopTracer;

/* Default stderr tracer. */
extern struct raft_tracer StderrTracer;

/* Legacy raft tracer */
typedef void (*raft_tracer_emit)(struct raft_tracer *t,
                                 const char *file,
                                 int line,
                                 const char *message);

/* Use TRACER to trace an event of type TYPE with the given INFO. */
#define Trace(TRACER, TYPE, INFO)             \
    do {                                      \
        if (LIKELY(TRACER == NULL)) {         \
            break;                            \
        }                                     \
        if (LIKELY(TRACER->version == 2)) {   \
            TRACER->emit(TRACER, TYPE, INFO); \
        }                                     \
    } while (0)

/* Emit a diagnostic message with the given tracer at level 3. */
#define Infof(TRACER, ...) Logf(TRACER, 3, __VA_ARGS__)

/* Emit diagnostic message with the given tracer at level 5. */
#define Tracef(TRACER, ...) Logf(TRACER, 5, __VA_ARGS__)

/* Use the tracer to log an event at the given level.
 *
 * The LEVEL parameter should be one of:
 *
 * - 1: error
 * - 2: warning
 * - 3: info
 * - 4: debug
 * - 5: trace
 */
#define Logf(TRACER, LEVEL, ...)                             \
    do {                                                     \
        int _type;                                           \
        struct raft_tracer_info _info;                       \
        static char _msg[1024];                              \
                                                             \
        if (LIKELY(TRACER == NULL)) {                        \
            break;                                           \
        }                                                    \
                                                             \
        snprintf(_msg, sizeof _msg, __VA_ARGS__);            \
                                                             \
        if (LIKELY(TRACER->version == 2)) {                  \
            _type = RAFT_TRACER_DIAGNOSTIC;                  \
            _info.version = 1;                               \
            _info.diagnostic.level = LEVEL;                  \
            _info.diagnostic.message = _msg;                 \
            _info.diagnostic.file = __FILE__;                \
            _info.diagnostic.line = __LINE__;                \
            TRACER->emit(TRACER, _type, &_info);             \
        } else if (UNLIKELY((bool)TRACER->version)) {        \
            raft_tracer_emit emit =                          \
                (raft_tracer_emit)((uintptr_t)TRACER->emit); \
            emit(TRACER, __FILE__, __LINE__, _msg);          \
        }                                                    \
    } while (0)

/* Enable the tracer if the env variable is set or disable the tracer */
void raft_tracer_maybe_enable(struct raft_tracer *tracer, bool enabled);

#endif /* TRACING_H_ */
