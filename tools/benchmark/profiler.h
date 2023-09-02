/* Kernel profiling and tracing helpers. */

#ifndef PROFILER_H_
#define PROFILER_H_

struct Profiler
{
    char *traces[10];  /* Names of the kernel sub-systems to trace. */
    unsigned n_traces; /* Number of sub-systems to trace. */
    unsigned switches; /* Number of context switches performed. */
};

void ProfilerInit(struct Profiler *t);

/* Trace the given kernel sub-system using tracefs. The output will be available
 * under /sys/kernel/tracing/trace. */
void ProfilerTrace(struct Profiler *t, char *system);

/* Enable on profiler the given subsystem. */
int ProfilerStart(struct Profiler *t);

int ProfilerStop(struct Profiler *t);

#endif /* PROFILER_H_ */
