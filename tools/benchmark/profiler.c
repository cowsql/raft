#include <assert.h>
#include <stdio.h>
#include <sys/resource.h>

#include "profiler.h"

#define PATH_TEMPLATE "/sys/kernel/tracing/events/%s/enable"

void ProfilerInit(struct Profiler *p)
{
    p->n_traces = 0;
    p->switches = 0;
}

void ProfilerTrace(struct Profiler *p, char *name)
{
    p->traces[p->n_traces] = name;
    p->n_traces++;
}

static int profilerTraceFsWrite(const char *name, const char *text)
{
    char path[1024];
    FILE *file;
    sprintf(path, PATH_TEMPLATE, name);
    file = fopen(path, "w");

    if (file == NULL) {
        perror("fopen tracefs");
        return -1;
    }
    fprintf(file, "%s", text);
    fclose(file);

    return 0;
}

static int profilerTraceFsEnable(const char *name)
{
    return profilerTraceFsWrite(name, "1");
}

static int profilerTraceFsDisable(const char *name)
{
    return profilerTraceFsWrite(name, "0");
}

static int contextSwitchCounterStart(unsigned *counter)
{
    struct rusage usage;
    int rv;

    rv = getrusage(RUSAGE_SELF, &usage);
    if (rv != 0) {
        return -1;
    }

    *counter = (unsigned)usage.ru_nvcsw;

    return 0;
}

static int contextSwitchCounterStop(unsigned *counter)
{
    struct rusage usage;
    int rv;

    rv = getrusage(RUSAGE_SELF, &usage);
    if (rv != 0) {
        return -1;
    }

    *counter = (unsigned)usage.ru_nvcsw - *counter;

    return 0;
}

int ProfilerStart(struct Profiler *p)
{
    unsigned i;
    int rv;

    for (i = 0; i < p->n_traces; i++) {
        rv = profilerTraceFsEnable(p->traces[i]);
        if (rv != 0) {
            return rv;
        }
    }

    rv = contextSwitchCounterStart(&p->switches);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int ProfilerStop(struct Profiler *p)
{
    unsigned i;
    int rv;

    rv = contextSwitchCounterStop(&p->switches);
    if (rv != 0) {
        return rv;
    }

    for (i = 0; i < p->n_traces; i++) {
        rv = profilerTraceFsDisable(p->traces[i]);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;
}
