#include <stdio.h>

#include "disk_uring.h"

#if defined(HAVE_LINUX_IO_URING_H)

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <linux/io_uring.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#include "timer.h"

#define QUEUE_DEPTH 1
#define MAX_CEQ_POLL 5ULL * 1000 * 1000 * 1000 /* At most a few seconds */

/* Macros for barriers needed by io_uring */
#define _io_uring_smp_store_release(p, v)                       \
    atomic_store_explicit((_Atomic __typeof__(*(p)) *)(p), (v), \
                          memory_order_release)

#define _io_uring_smp_load_acquire(p) \
    atomic_load_explicit((_Atomic __typeof__(*(p)) *)(p), memory_order_acquire)

static int _ring_fd;
static unsigned *sring_tail, *sring_mask, *sring_array, *cring_head,
    *cring_tail, *cring_mask;

struct io_uring_sqe *sqes;
struct io_uring_cqe *cqes;

/*
 * System call wrappers provided since glibc does not yet
 * provide wrappers for io_uring system calls.
 * */

static int _io_uring_setup(unsigned entries, struct io_uring_params *p)
{
    return (int)syscall(__NR_io_uring_setup, entries, p);
}

static int _io_uring_enter(int ring_fd,
                           unsigned int to_submit,
                           unsigned int min_complete,
                           unsigned int flags)
{
    return (int)syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete,
                        flags, NULL, 0);
}

static int _io_uring_register(int fd,
                              unsigned int opcode,
                              const void *arg,
                              unsigned int nr_args)
{
    return (int)syscall(__NR_io_uring_register, fd, opcode, arg, nr_args);
}

static int initUring(int fd, struct iovec *iov)
{
    struct io_uring_params p;
    void *sq_ptr, *cq_ptr;
    size_t sring_sz;
    size_t cring_sz;
    int rv;

    /* See io_uring_setup(2) for io_uring_params.flags you can set */
    memset(&p, 0, sizeof(p));
#if defined(IORING_SETUP_SINGLE_ISSUER)
    p.flags = IORING_SETUP_SINGLE_ISSUER;
#endif
    _ring_fd = _io_uring_setup(QUEUE_DEPTH, &p);
    if (_ring_fd < 0) {
        perror("io_uring_setup");
        return -1;
    }
    /*
     * io_uring communication happens via 2 shared kernel‐user space ring
     * buffers, which can be jointly mapped with a single mmap() call in
     * kernels >= 5.4.
     */

    sring_sz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    cring_sz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);

    /* Rather than check for kernel version, the recommended way is to
     * check the features field of the io_uring_params structure, which is a
     * bitmask. If IORING_FEAT_SINGLE_MMAP is set, we can do away with the
     * second mmap() call to map in the completion ring separately.
     */
    if (p.features & IORING_FEAT_SINGLE_MMAP) {
        if (cring_sz > sring_sz)
            sring_sz = cring_sz;
        cring_sz = sring_sz;
    }

    /* Map in the submission and completion queue ring buffers.
     *  Kernels < 5.4 only map in the submission queue, though.
     */
    sq_ptr = mmap(0, sring_sz, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, _ring_fd, IORING_OFF_SQ_RING);
    if (sq_ptr == MAP_FAILED) {
        perror("mmap");
        return -1;
    }

    if (p.features & IORING_FEAT_SINGLE_MMAP) {
        cq_ptr = sq_ptr;
    } else {
        return -1;
    }

    /* Save useful fields for later easy reference */
    sring_tail = (void *)((char *)sq_ptr + p.sq_off.tail);
    sring_mask = (void *)((char *)sq_ptr + p.sq_off.ring_mask);
    sring_array = (void *)((char *)sq_ptr + p.sq_off.array);

    /* Map in the submission queue entries array */
    sqes = mmap(0, p.sq_entries * sizeof(struct io_uring_sqe),
                PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, _ring_fd,
                IORING_OFF_SQES);
    if (sqes == MAP_FAILED) {
        perror("mmap");
        return 1;
    }

    /* Save useful fields for later easy reference */
    cring_head = (void *)((char *)cq_ptr + p.cq_off.head);
    cring_tail = (void *)((char *)cq_ptr + p.cq_off.tail);
    cring_mask = (void *)((char *)cq_ptr + p.cq_off.ring_mask);
    cqes = (struct io_uring_cqe *)((char *)cq_ptr + p.cq_off.cqes);

    rv = _io_uring_register(_ring_fd, IORING_REGISTER_FILES, &fd, 1);
    if (rv != 0) {
        fprintf(stderr, "Unable to register file!\n");
        return -1;
    }
    rv = _io_uring_register(_ring_fd, IORING_REGISTER_BUFFERS, iov, 1);
    if (rv != 0) {
        fprintf(stderr, "Unable to register buffer!\n");
        return -1;
    }

    return 0;
}

static int writeWithUring(struct iovec *iov, unsigned i)
{
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    unsigned index, tail;
    unsigned head;
    unsigned long long n;

    int rv;

    /* Add our submission queue entry to the tail of the SQE ring buffer */
    tail = *sring_tail;
    index = tail & *sring_mask;
    sqe = &sqes[index];

    /* Fill in the parameters required for the read or write operation */
    sqe->opcode = IORING_OP_WRITE_FIXED;
    sqe->flags = IOSQE_FIXED_FILE;
    sqe->fd = 0;
    sqe->addr = (unsigned long)iov->iov_base;
    sqe->len = (unsigned)iov->iov_len;
    sqe->off = i * iov->iov_len;
    sqe->rw_flags = RWF_DSYNC;

    sring_array[index] = index;
    tail++;

    /* Update the tail */
    _io_uring_smp_store_release(sring_tail, tail);

    rv = _io_uring_enter(_ring_fd, 1, 0, IORING_ENTER_GETEVENTS);
    if (rv != 1) {
        perror("io_uring_enter");
        return -1;
    }

    /* Poll the SQE ring */
    n = 0;
retry:
    /* Read barrier */
    head = _io_uring_smp_load_acquire(cring_head);
    /*
     * Remember, this is a ring buffer. If head == tail, it means that the
     * buffer is empty.
     * */
    if (head == *cring_tail) {
        if (n == MAX_CEQ_POLL) {
            printf("no cqe\n");
            return -1;
        }
        n++;
        goto retry;
    }

    /* Get the entry */
    cqe = &cqes[head & (*cring_mask)];
    if (cqe->res < 0) {
        fprintf(stderr, "Error: %s\n", strerror(abs(cqe->res)));
        return -1;
    }

    head++;

    /* Write barrier so that update to the head are made visible */
    _io_uring_smp_store_release(cring_head, head);

    return 0;
}

#endif /* HAVE_LINUX_IO_URING_H */

int DiskWriteUsingUring(int fd,
                        struct iovec *iov,
                        unsigned n,
                        struct Profiler *profiler,
                        struct histogram *histogram)
{
#if defined(HAVE_LINUX_IO_URING_H)
    struct timer timer;
    unsigned i;
    int rv;

    rv = initUring(fd, iov);
    if (rv != 0) {
        return -1;
    }

    /* Perform a first write to trigger initialization and warm up caches. */
    rv = writeWithUring(iov, 0);
    if (rv != 0) {
        return -1;
    }

    rv = ProfilerStart(profiler);
    if (rv != 0) {
        return rv;
    }

    for (i = 0; i < n; i++) {
        TimerStart(&timer);
        rv = writeWithUring(iov, i);
        if (rv != 0) {
            return -1;
        }
        HistogramCount(histogram, TimerStop(&timer));
    }

    rv = ProfilerStop(profiler);
    if (rv != 0) {
        return rv;
    }

    rv = _io_uring_register(_ring_fd, IORING_UNREGISTER_FILES, NULL, 0);
    if (rv != 0) {
        fprintf(stderr, "Unable to unregister file!\n");
        return -1;
    }
    rv = _io_uring_register(_ring_fd, IORING_UNREGISTER_BUFFERS, NULL, 0);
    if (rv != 0) {
        fprintf(stderr, "Unable to unregister buffer: %d %d\n", rv, errno);
        perror("io_uring_register");
        return -1;
    }

    close(_ring_fd);

    return 0;

#else /* HAVE_LINUX_IO_URING_H */

    (void)fd;
    (void)iov;
    (void)n;
    (void)profiler;
    (void)histogram;
    fprintf(stderr, "io_uring not available\n");
    return -1;

#endif /* HAVE_LINUX_IO_URING_H */
}
