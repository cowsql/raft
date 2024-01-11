#include "tracer.h"

#include "munit.h"

static void traceDiagnostic(const struct raft_tracer_info *info)
{
    fprintf(stderr, "%20s:%*d - %s\n", info->diagnostic.file, 3,
            info->diagnostic.line, info->diagnostic.message);
}

void TracerEmit(struct raft_tracer *t, int type, const void *info)
{
    (void)t;
    switch (type) {
        case RAFT_TRACER_DIAGNOSTIC:
            traceDiagnostic(info);
            break;
    };
}
