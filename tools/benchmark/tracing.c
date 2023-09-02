#include <stdio.h>

#include "tracing.h"

#define PATH_TEMPLATE "/sys/kernel/debug/tracing/events/%s/enable"

void TracingInit(struct Tracing *t)
{
    t->n = 0;
}

void TracingAdd(struct Tracing *t, char *system)
{
    t->systems[t->n] = system;
    t->n++;
}

static int tracingWrite(const char *name, const char *op)
{
    char path[1024];
    FILE *file;
    sprintf(path, PATH_TEMPLATE, name);
    file = fopen(path, "w");

    if (file == NULL) {
        perror("fopen debugfs");
        return -1;
    }
    fprintf(file, "%s", op);
    fclose(file);

    return 0;
}

static int tracingEnable(const char *name)
{
    return tracingWrite(name, "1");
}

static int tracingDisable(const char *name)
{
    return tracingWrite(name, "0");
}

int TracingStart(struct Tracing *t)
{
    unsigned i;
    int rv;
    for (i = 0; i < t->n; i++) {
        rv = tracingEnable(t->systems[i]);
        if (rv != 0) {
            return rv;
        }
    }
    return 0;
}

int TracingStop(struct Tracing *t)
{
    unsigned i;
    int rv;
    for (i = 0; i < t->n; i++) {
        rv = tracingDisable(t->systems[i]);
        if (rv != 0) {
            return rv;
        }
    }
    return 0;
}
