#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "disk.h"
#include "disk_fs.h"
#include "disk_options.h"
#include "disk_parse.h"
#include "disk_pwritev2.h"
#include "disk_uring.h"
#include "timer.h"

/* Allocate a buffer of the given size. */
static void allocBuffer(struct iovec *iov, size_t size)
{
    iov->iov_len = size;
    iov->iov_base = aligned_alloc(iov->iov_len, iov->iov_len);
    assert(iov->iov_base != NULL);
}

/* Benchmark the performance of a single disk write. */
static int benchmarkWritePerformance(const char *dir,
                                     size_t buf,
                                     unsigned size,
                                     int engine,
                                     int mode)
{
    char *path;
    int fd;
    struct iovec iov;
    time_t *latencies;
    int rv;
    unsigned n = size / (unsigned)buf;

    assert(size % buf == 0);

    rv = DiskFsCreateTempFile(dir, n * buf, &path, &fd);
    if (rv != 0) {
        return -1;
    }

    if (mode == DISK_MODE_DIRECT) {
        rv = DiskFsSetDirectIO(fd);
        if (rv != 0) {
            return -1;
        }
    }

    allocBuffer(&iov, buf);
    latencies = malloc(n * sizeof *latencies);
    assert(latencies != NULL);

    switch (engine) {
        case DISK_ENGINE_PWRITEV2:
            rv = DiskWriteUsingPwritev2(fd, &iov, n, latencies);
            break;
        case DISK_ENGINE_URING:
            rv = DiskWriteUsingUring(fd, &iov, n, latencies);
            break;
        default:
            assert(0);
    }

    free(iov.iov_base);

    if (rv != 0) {
        return -1;
    }

    printf("%-8s:  %8s writes of %4zu bytes take %4zu microsecs on average\n",
           DiskEngineName(engine), DiskModeName(mode), buf,
           latencies[0] / 1000);

    free(latencies);

    rv = DiskFsRemoveTempFile(path, fd);
    if (rv != 0) {
        return -1;
    }

    return 0;
}

int DiskRun(int argc, char *argv[])
{
    struct diskOptions opts;
    struct stat st;
    int rv;

    DiskParse(argc, argv, &opts);

    rv = stat(opts.dir, &st);
    if (rv != 0) {
        printf("stat '%s': %s\n", opts.dir, strerror(errno));
        goto err;
    }

    assert(opts.buf != 0);
    assert(opts.mode >= 0);

    if (opts.mode == DISK_MODE_DIRECT) {
        rv = DiskFsCheckDirectIO(opts.dir, opts.buf);
        if (rv != 0) {
            goto err;
        }
    }

    rv = benchmarkWritePerformance(opts.dir, opts.buf, opts.size, opts.engine,
                                   opts.mode);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    return -1;
}
