#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "disk_pwrite.h"
#include "timer.h"

static int writeWithPwrite(int fd, struct iovec *iov, unsigned i)
{
    ssize_t rv;
    rv = pwritev2(fd, iov, 1, (off_t)(i * iov->iov_len), RWF_DSYNC);
    if (rv == -1) {
        printf("pwritev2 '%d': %s\n", i, strerror(errno));
        return -1;
    }
    assert(rv == (int)iov->iov_len);
    return 0;
}

int DiskWriteUsingPwrite(int fd,
                         struct iovec *iov,
                         unsigned n,
                         time_t *latencies)
{
    struct timer timer;
    unsigned i;
    int rv;

    for (i = 0; i < n; i++) {
        TimerStart(&timer);
        rv = writeWithPwrite(fd, iov, i);
        if (rv != 0) {
            return -1;
        }
        latencies[i] = TimerStop(&timer);
    }

    return 0;
}
