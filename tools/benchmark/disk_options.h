/* Options for the disk benchmark. */

#ifndef DISK_OPTIONS_H_
#define DISK_OPTIONS_H_

#include <stddef.h>

#include "profiler.h"

/* Options for the disk benchmark */
struct diskOptions
{
    char *dir;     /* Directory to use for creating temporary files */
    size_t buf;    /* Write buffer size */
    unsigned size; /* Size of the file to write, must be a multiple of buf */
    bool perf;     /* Turn on or off kernel performance measuring */
    const char *traces[8]; /* Kernel sub-systems to trace */
    unsigned n_traces;
};

#endif /* DISK_ARGS_H_ */
