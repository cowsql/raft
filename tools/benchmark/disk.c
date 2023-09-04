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

#define RESOLUTION 100         /* buckets are 100 nanoseconds apart */
#define BUCKETS 20 * 1000 * 10 /* buckets up to 20,000 microseconds */

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

/* Benchmark sequential write performance. */
static int writeFile(struct diskOptions *opts, struct benchmark *benchmark)
{
    struct timer timer;
    struct histogram histogram;
    struct iovec iov;
    char *path;
    int fd;
    unsigned long duration;
    unsigned n = opts->size / (unsigned)opts->buf;
    struct stat st;
    bool raw = false;
    int rv;

    rv = stat(opts->dir, &st);
    if (rv != 0) {
        printf("stat '%s': %s\n", opts->dir, strerror(errno));
        return -1;
    }

    if ((st.st_mode & S_IFMT) == S_IFBLK) {
        raw = true;
    }

    assert(opts->size % opts->buf == 0);

    if (raw) {
        rv = FsOpenBlockDevice(opts->dir, &fd);
    } else {
        rv = FsCreateTempFile(opts->dir, n * opts->buf, &path, &fd);
    }
    if (rv != 0) {
        return -1;
    }

    rv = FsCheckDirectIO(fd, opts->buf);
    if (rv != 0) {
        return -1;
    }

    allocBuffer(&iov, opts->buf);

    HistogramInit(&histogram, BUCKETS, RESOLUTION, RESOLUTION);

    TimerStart(&timer);

    rv = DiskWriteUsingUring(fd, &iov, n, &opts->tracing, &histogram);

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
