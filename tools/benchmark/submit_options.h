/* Options for the submit benchmark. */

#ifndef SUBMIT_OPTIONS_H_
#define SUBMIT_OPTIONS_H_

#include <stddef.h>

/* Options for the submit benchmark */
struct submitOptions
{
    char *dir;     /* Directory to use for creating temporary files */
    size_t buf;    /* Buffer size of each raft entry to submit */
    unsigned size; /* Total number of bytes to submit */
};

#endif /* SUBMIT_ARGS_H_ */
