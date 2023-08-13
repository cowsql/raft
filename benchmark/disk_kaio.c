#include <assert.h>
#include <errno.h>
#include <linux/aio_abi.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "disk_uring.h"
#include "timer.h"

int io_setup(unsigned nr_events, aio_context_t *ctx_idp)
{
    return (int)syscall(__NR_io_setup, nr_events, ctx_idp);
}

int io_destroy(aio_context_t ctx_id)
{
    return (int)syscall(__NR_io_destroy, ctx_id);
}

int io_submit(aio_context_t ctx_id, long nr, struct iocb **iocbpp)
{
    return (int)syscall(__NR_io_submit, ctx_id, nr, iocbpp);
}

int io_getevents(aio_context_t ctx_id,
                 long min_nr,
                 long nr,
                 struct io_event *events,
                 struct timespec *timeout)
{
    return (int)syscall(__NR_io_getevents, ctx_id, min_nr, nr, events, timeout);
}

static aio_context_t ctx; /* KAIO handle */

static int writeWithKaio(int fd, struct iovec *iov, unsigned i)
{
    struct iocb iocb1;
    struct iocb iocb2;
    struct iocb *iocbs[2];
    struct io_event events[2];
    int rv;

    memset(&iocb1, 0, sizeof iocb1);
    memset(&iocb2, 0, sizeof iocb2);

    iocb1.aio_fildes = (uint32_t)fd;
    iocb1.aio_lio_opcode = IOCB_CMD_PWRITEV;
    iocb1.aio_reqprio = 0;
    *((void **)(&iocb1.aio_buf)) = (void *)(&iov->iov_base);
    iocb1.aio_nbytes = 1;
    iocb1.aio_offset = (int64_t)(i * iov->iov_len);

    iocb2.aio_fildes = (uint32_t)fd;
    iocb2.aio_lio_opcode = IOCB_CMD_FDSYNC;

    iocbs[0] = &iocb1;
    iocbs[1] = &iocb2;

    rv = io_submit(ctx, 2, iocbs);
    if (rv != 2) {
        fprintf(stderr, "io_submit: %s\n", strerror(rv));
        return -1;
    }

    rv = io_getevents(ctx, 2, 2, events, NULL);
    if (rv != 2) {
        fprintf(stderr, "io_getevents: %s\n", strerror(rv));
        return -1;
    }

    return 0;
}

int DiskWriteUsingKaio(int fd, struct iovec *iov, unsigned n, time_t *latencies)
{
    struct timer timer;
    unsigned i;
    int rv;

    rv = io_setup(1, &ctx);
    if (rv != 0) {
        fprintf(stderr, "io_setup: %s\n", strerror(rv));
        return -1;
    }

    for (i = 0; i < n; i++) {
        TimerStart(&timer);
        rv = writeWithKaio(fd, iov, i);
        if (rv != 0) {
            return -1;
        }
        latencies[i] = TimerStop(&timer);
    }

    rv = io_destroy(ctx);
    if (rv != 0) {
        fprintf(stderr, "io_destroy: %s\n", strerror(rv));
        return -1;
    }

    return 0;
}
