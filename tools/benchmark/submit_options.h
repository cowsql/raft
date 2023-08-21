/* Options for the submit benchmark. */

#ifndef SUBMIT_OPTIONS_H_
#define SUBMIT_OPTIONS_H_

#include <stddef.h>

/* Options for the submit benchmark */
struct submitOptions
{
    char *dir;   /* Directory to use for creating temporary files */
    size_t size; /* Size of each submitted entry */
    unsigned n;  /* Number of submissions */
};

#endif /* SUBMIT_ARGS_H_ */
