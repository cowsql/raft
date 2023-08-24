/* Sequential writes using pwritev2. */

#ifndef DISK_PWRITE_H_
#define DISK_PWRITE_H_

#include <stddef.h>
#include <sys/uio.h>

#include "report.h"

int DiskWriteUsingPwrite(int fd,
                         struct iovec *iov,
                         unsigned n,
                         struct histogram *histogram);

#endif /* DISK_PWRITE_H_ */
