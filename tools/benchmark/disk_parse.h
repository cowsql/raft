/* Parse command line arguments for the disk benchmark. */

#ifndef DISK_ARGS_H_
#define DISK_ARGS_H_

#include "disk_options.h"

/* Parse the given command line arguments. */
void DiskParse(int argc, char *argv[], struct diskOptions *opts);

#endif /* DISK_ARGS_H_ */
