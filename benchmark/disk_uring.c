#include <assert.h>
#include <errno.h>
#include <liburing.h>
#include <stdio.h>
#include <string.h>

#include "disk_uring.h"
#include "timer.h"

static struct io_uring uring;

static void initUring(int fd, struct iovec *iov)
{
    int rv;
    rv = io_uring_queue_init(4, &uring, 0);
    assert(rv == 0);
    rv = io_uring_register_files(&uring, &fd, 1);
    assert(rv == 0);

    rv = io_uring_register_buffers(&uring, iov, 1);
    assert(rv == 0);
}

static int writeWithUring(struct iovec *iov, unsigned i)
{
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int rv;

    sqe = io_uring_get_sqe(&uring);
    io_uring_prep_write_fixed(sqe, 0, iov->iov_base, (unsigned)iov->iov_len,
                              i * iov->iov_len, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    sqe->rw_flags = RWF_DSYNC;

    sqe = io_uring_get_sqe(&uring);
    io_uring_prep_fsync(sqe, 0, IORING_FSYNC_DATASYNC);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);

    rv = io_uring_submit(&uring);
    assert(rv == 2);

    io_uring_wait_cqe(&uring, &cqe);
    if (cqe->res < 0) {
        printf("sqe failed: %s\n", strerror(-cqe->res));
        return -1;
    }
    io_uring_cqe_seen(&uring, cqe);

    io_uring_wait_cqe(&uring, &cqe);
    if (cqe->res < 0) {
        printf("sqe failed: %s\n", strerror(-cqe->res));
        return -1;
    }
    io_uring_cqe_seen(&uring, cqe);

    return 0;
}

int DiskWriteUsingUring(int fd,
                        struct iovec *iov,
                        unsigned n,
                        time_t *latencies)
{
    struct timer timer;
    unsigned i;
    int rv;

    initUring(fd, iov);

    for (i = 0; i < n; i++) {
        TimerStart(&timer);
        rv = writeWithUring(iov, i);
        if (rv != 0) {
            return -1;
        }
        latencies[i] = TimerStop(&timer);
    }

    io_uring_queue_exit(&uring);

    return 0;
}
