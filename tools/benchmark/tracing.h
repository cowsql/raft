/* Enable and disable tracing via debugfs. */

#ifndef TRACING_H_
#define TRACING_H_

struct tracing
{
    char *systems[10]; /* Names of the sub-systems to trace. */
    unsigned n;        /* Number of sub-systems to trace. */
};

void TracingInit(struct tracing *t);

/* Add a new sub-system to trace. */
void TracingAdd(struct tracing *t, char *system);

/* Enable on tracing the given subsystem. */
int TracingStart(struct tracing *t);

int TracingStop(struct tracing *t);

#endif /* TRACING_H_ */
