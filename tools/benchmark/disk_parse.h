/* Parse command line arguments for the disk benchmark. */

#ifndef DISK_ARGS_H_
#define DISK_ARGS_H_

#include "disk_options.h"
#include "tracing.h"

/* Array of all options combination to run. */
struct diskMatrix
{
    struct diskOptions *opts;
    unsigned n_opts;
    struct Tracing tracing;
};

/* Parse the given command line arguments. */
void DiskParse(int argc, char *argv[], struct diskMatrix *matrix);

#endif /* DISK_ARGS_H_ */
