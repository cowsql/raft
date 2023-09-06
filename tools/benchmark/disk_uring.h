/* Sequential writes using io_uring. */

#ifndef DISK_URING_H_
#define DISK_URING_H_

#include <stddef.h>
#include <sys/uio.h>

#include "fs.h"
#include "profiler.h"
#include "report.h"

int DiskWriteUsingUring(int fd,
                        struct iovec *iov,
                        unsigned n,
                        struct Profiler *profiler,
                        struct histogram *histogram);

#endif /* DISK_URING_H_ */
