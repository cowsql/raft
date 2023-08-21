/* Parse command line arguments for the submit benchmark. */

#ifndef SUBMIT_ARGS_H_
#define SUBMIT_ARGS_H_

#include "submit_options.h"

/* Parse the given command line arguments. */
void SubmitParse(int argc, char *argv[], struct submitOptions *opts);

#endif /* SUBMIT_ARGS_H_ */
