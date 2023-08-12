#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "disk.h"

enum { BENCHMARK_DISK = 0 };

static const char *doc =
    "benchmarks:\n"
    " - disk: Sequential disk writes\n";

static const char *benchmarks[] = {[BENCHMARK_DISK] = "disk", NULL};

int benchmarkCode(const char *name)
{
    int i = 0;
    while (benchmarks[i] != NULL) {
        if (strcmp(benchmarks[i], name) == 0) {
            return i;
        }
        i++;
    }
    return -1;
}

int main(int argc, char *argv[])
{
    int cmd;
    int rv;

    assert(argc >= 1);

    if (argc < 2) {
        printf("usage: %s <benchmark> [--help | <args>]\n\n%s", argv[0], doc);
        return -1;
    }

    cmd = benchmarkCode(argv[1]);
    if (cmd == -1) {
        printf("unknown: benchmark '%s'\n", argv[1]);
        return -1;
    }

    switch (cmd) {
        case BENCHMARK_DISK:
            rv = DiskRun(argc - 1, &argv[1]);
            break;
        default:
            assert(0);
            rv = -1;
            break;
    }

    return rv;
}
