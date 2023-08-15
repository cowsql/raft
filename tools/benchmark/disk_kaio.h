/* Sequential writes using the Kernel AIO sub-system. */

#ifndef DISK_KAIO_H_
#define DISK_KAIO_H_

#include <stddef.h>
#include <sys/uio.h>

int DiskWriteUsingKaio(int fd,
                       struct iovec *iov,
                       unsigned n,
                       time_t *latencies);

#endif /* DISK_URING_H_ */
