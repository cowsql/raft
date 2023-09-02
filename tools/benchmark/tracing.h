/* Enable and disable tracing via debugfs. */

#ifndef TRACING_H_
#define TRACING_H_

struct Tracing
{
    char *systems[10]; /* Names of the sub-systems to trace. */
    unsigned n;        /* Number of sub-systems to trace. */
};

void TracingInit(struct Tracing *t);

/* Add a new sub-system to trace. */
void TracingAdd(struct Tracing *t, char *system);

/* Enable on tracing the given subsystem. */
int TracingStart(struct Tracing *t);

int TracingStop(struct Tracing *t);

#endif /* TRACING_H_ */
