#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "disk.h"
#include "report.h"
#include "submit.h"

enum {
    BENCHMARK_DISK = 0,
    BENCHMARK_SUBMIT,
};

static const char *doc =
    "benchmarks:\n"
    " - disk: Sequential disk writes\n"
    " - submit: Sequential submission of entries\n";

static const char *benchmarks[] =
    {[BENCHMARK_DISK] = "disk", [BENCHMARK_SUBMIT] = "submit", NULL};

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
    struct report report;
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

    ReportInit(&report);

    switch (cmd) {
        case BENCHMARK_DISK:
            rv = DiskRun(argc - 1, &argv[1], &report);
            break;
        case BENCHMARK_SUBMIT:
            rv = SubmitRun(argc - 1, &argv[1], &report);
            break;
        default:
            assert(0);
            rv = -1;
            break;
    }

    ReportPrint(&report);
    ReportClose(&report);

    return rv;
}
