/* Kernel profiling and tracing helpers. */

#ifndef PROFILER_H_
#define PROFILER_H_

#include "fs.h"

struct Profiler;

/* Group of per-CPU perf events. */
struct ProfilerEventGroup
{
    int cpu;           /* The group runs on this CPU */
    int *events;       /* All perf_event_open FDs created for CPU */
    unsigned n_events; /* N. of perf events in the group */
    void *data;        /* Perf event group leader mmap */
    struct Profiler *p;
};

/* Performance data collected from a specific kernel sub-system. */
struct ProfilerDataSource
{
    unsigned n_commands;
    struct
    {
        unsigned long long id;       /* Command ID */
        unsigned long long start;    /* Start time */
        unsigned long long duration; /* Command duration */
    } commands[16348];
    unsigned types[3]; /* Tracepoint type codes for this sub-system */
};

struct Profiler
{
    struct FsFileInfo *device; /* Information about the underlying device. */
    const char *traces[10];    /* Names of the kernel sub-systems to trace. */
    unsigned n_traces;         /* Number of sub-systems to trace. */
    unsigned switches;         /* Number of context switches performed. */
    struct ProfilerEventGroup *groups; /* Groups of per-CPU events.. */
    unsigned n_groups;
    struct ProfilerDataSource nvme;
    struct ProfilerDataSource block;
};

void ProfilerInit(struct Profiler *p, struct FsFileInfo *device);

void ProfilerClose(struct Profiler *p);

/* Turn on performance profiling. */
int ProfilerPerf(struct Profiler *p);

/* Trace the given kernel sub-system using tracefs. The output will be available
 * under /sys/kernel/tracing/trace. */
void ProfilerTrace(struct Profiler *p, const char *system);

/* Enable on profiler the given subsystem. */
int ProfilerStart(struct Profiler *p);

int ProfilerStop(struct Profiler *p);

#endif /* PROFILER_H_ */
