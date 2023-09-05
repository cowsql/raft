#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "disk.h"
#include "disk_options.h"
#include "disk_parse.h"
#include "disk_uring.h"
#include "fs.h"
#include "timer.h"

/* Histgram resolution when the underlying block driver is NVMe. */
#define RESOLUTION_NVME 500        /* buckets are 0.5 microseconds apart */
#define BUCKETS_NVME 20 * 1000 * 2 /* buckets up to 20,000 microseconds */

/* Histgram resolution when the underlying block driver is nullb. */
#define RESOLUTION_NULLB 50     /* buckets are 50 nanoseconds apart */
#define BUCKETS_NULLB 1000 * 20 /* buckets up to 1,000 microseconds */

/* Histgram resolution when the underlying block driver is unspecified. */
#define RESOLUTION_GENERIC 1000   /* buckets are 1 microsecond apart */
#define BUCKETS_GENERIC 20 * 1000 /* buckets up to 20,000 microseconds */

/* Allocate a buffer of the given size. */
static void allocBuffer(struct iovec *iov, size_t size)
{
    unsigned i;

    iov->iov_len = size;
    iov->iov_base = aligned_alloc(iov->iov_len, iov->iov_len);
    assert(iov->iov_base != NULL);

    /* Populate the buffer with some fixed data. */
    for (i = 0; i < size; i++) {
        *(((uint8_t *)iov->iov_base) + i) = i % 128;
    }
}

static void reportLatency(struct benchmark *benchmark,
                          struct histogram *histogram)
{
    struct metric *m;
    m = BenchmarkGrow(benchmark, METRIC_KIND_LATENCY);
    MetricFillHistogram(m, histogram);
}

static void reportThroughput(struct benchmark *benchmark,
                             unsigned long duration,
                             unsigned size)
{
    struct metric *m;
    unsigned megabytes = size / (1024 * 1024); /* N megabytes written */
    m = BenchmarkGrow(benchmark, METRIC_KIND_THROUGHPUT);
    MetricFillThroughput(m, megabytes, duration);
}

/* Init the given histogram using a resolution and buckets count appropriate for
 * the given driver type. */
static void initHistogramForDriverType(struct histogram *histogram,
                                       unsigned type)
{
    unsigned buckets;
    unsigned resolution;

    switch (type) {
        case FS_DRIVER_NVME:
            buckets = BUCKETS_NVME;
            resolution = RESOLUTION_NVME;
            break;
        case FS_DRIVER_NULLB:
            buckets = BUCKETS_NULLB;
            resolution = RESOLUTION_NULLB;
            break;
        case FS_DRIVER_GENERIC:
            buckets = BUCKETS_GENERIC;
            resolution = RESOLUTION_GENERIC;
            break;
        default:
            assert(0);
            break;
    }
    HistogramInit(histogram, buckets, resolution, resolution);
}

/* Benchmark sequential write performance. */
static int writeFile(struct diskOptions *opts, struct benchmark *benchmark)
{
    struct FsFileInfo info;
    struct timer timer;
    struct histogram histogram;
    struct iovec iov;
    char *path;
    int fd;
    unsigned long duration;
    unsigned n = opts->size / (unsigned)opts->buf;
    bool raw;
    int rv;

    rv = FsFileInfo(opts->dir, &info);
    if (rv != 0) {
        printf("file info '%s': %s\n", opts->dir, strerror(errno));
        return -1;
    }
    raw = info.type == FS_TYPE_DEVICE;

    assert(opts->size % opts->buf == 0);

    if (raw) {
        rv = FsOpenBlockDevice(opts->dir, &fd);
    } else {
        rv = FsCreateTempFile(opts->dir, n * opts->buf, &path, &fd);
        if (rv == 0) {
            rv = FsFileInfo(path, &info);
        }
    }
    if (rv != 0) {
        return -1;
    }

    rv = FsCheckDirectIO(fd, opts->buf);
    if (rv != 0) {
        return -1;
    }

    allocBuffer(&iov, opts->buf);

    initHistogramForDriverType(&histogram, info.driver);

    TimerStart(&timer);

    rv = DiskWriteUsingUring(fd, &iov, n, &info, &opts->tracing, &histogram);

    duration = TimerStop(&timer);

    free(iov.iov_base);

    if (rv != 0) {
        return -1;
    }

    reportLatency(benchmark, &histogram);
    reportThroughput(benchmark, duration, opts->size);

    HistogramClose(&histogram);

    if (!raw) {
        rv = FsRemoveTempFile(path, fd);
        if (rv != 0) {
            return -1;
        }
    }

    return 0;
}

int DiskRun(int argc, char *argv[], struct report *report)
{
    struct diskOptions opts;
    struct benchmark *benchmark;
    char *name;
    int rv;

    DiskParse(argc, argv, &opts);

    assert(opts.buf != 0);

    rv = asprintf(&name, "disk:%zu", opts.buf);
    assert(rv > 0);
    assert(name != NULL);

    benchmark = ReportGrow(report, name);

    rv = writeFile(&opts, benchmark);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    return -1;
}
