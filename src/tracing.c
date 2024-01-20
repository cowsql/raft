#include <inttypes.h>
#include <stdlib.h>
#include <time.h>

#include "tracing.h"

static inline void noopEmit(struct raft_tracer *t, int type, const void *info)
{
    (void)t;
    (void)type;
    (void)info;
}
struct raft_tracer NoopTracer = {.impl = NULL, .version = 2, .emit = noopEmit};

static bool stderrTraceEnabled = false;

static inline void stderrTracerEmit(struct raft_tracer *t,
                                    int type,
                                    const void *data)
{
    const struct raft_tracer_info *info = data;
    struct timespec ts = {0};
    int64_t ns;

    (void)t;

    if (!stderrTraceEnabled) {
        return;
    }
    if (type != RAFT_TRACER_DIAGNOSTIC) {
        return;
    }

    /* ignore errors */
    clock_gettime(CLOCK_REALTIME, &ts);
    ns = ts.tv_sec * 1000000000 + ts.tv_nsec;
    fprintf(stderr, "LIBRAFT   %" PRId64 " %s:%d %s\n", ns,
            info->diagnostic.file, info->diagnostic.line,
            info->diagnostic.message);
}
struct raft_tracer StderrTracer = {.impl = NULL,
                                   .version = 2,
                                   .emit = stderrTracerEmit};

void raft_tracer_maybe_enable(struct raft_tracer *tracer, bool enabled)
{
    (void)tracer;
    if (getenv(LIBRAFT_TRACE) != NULL) {
        stderrTraceEnabled = enabled;
    }
}
