/* Sequential writes using pwritev2. */

#ifndef DISK_PWRITEV2_H_
#define DISK_PWRITEV2_H_

#include <stddef.h>
#include <sys/uio.h>

int DiskWriteUsingPwritev2(int fd,
                           struct iovec *iov,
                           unsigned n,
                           time_t *latencies);

#endif /* DISK_PWRITEV2_H_ */
