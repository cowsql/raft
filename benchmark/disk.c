#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "disk.h"
#include "disk_fs.h"
#include "disk_kaio.h"
#include "disk_options.h"
#include "disk_parse.h"
#include "disk_pwritev2.h"
#include "disk_uring.h"
#include "timer.h"

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
                          time_t *latencies,
                          unsigned n)
{
    struct metric *m;
    double total = 0;
    unsigned i;

    m = BenchmarkGrow(benchmark, METRIC_KIND_LATENCY);

    for (i = 0; i < n; i++) {
        double value = (double)latencies[i];

        if (i == 0) {
            m->lower_bound = value;
            m->upper_bound = value;
        }

        if (value < m->lower_bound) {
            m->lower_bound = value;
        }

        if (value > m->upper_bound) {
            m->upper_bound = value;
        }

        total += value;
    }

    m->value = total / n; /* Average latency */
}

static void reportThroughput(struct benchmark *benchmark,
                             time_t duration,
                             unsigned size)
{
    struct metric *m;
    double megabytes = (double)size / (1024 * 1024); /* N megabytes written */
    double seconds = (double)duration / (1024 * 1024 * 1024);
    m = BenchmarkGrow(benchmark, METRIC_KIND_THROUGHPUT);
    m->value = megabytes / seconds; /* Megabytes per second */
}

/* Benchmark sequential write performance. */
static int writeFile(struct diskOptions *opts, struct benchmark *benchmark)
{
    struct timer timer;
    struct iovec iov;
    char *path;
    int fd;
    time_t *latencies;
    time_t duration;
    unsigned n = opts->size / (unsigned)opts->buf;
    int rv;

    assert(opts->size % opts->buf == 0);

    rv = DiskFsCreateTempFile(opts->dir, n * opts->buf, &path, &fd);
    if (rv != 0) {
        return -1;
    }

    if (opts->mode == DISK_MODE_DIRECT) {
        rv = DiskFsSetDirectIO(fd);
        if (rv != 0) {
            return -1;
        }
    }

    allocBuffer(&iov, opts->buf);
    latencies = malloc(n * sizeof *latencies);
    assert(latencies != NULL);

    TimerStart(&timer);

    switch (opts->engine) {
        case DISK_ENGINE_PWRITEV2:
            rv = DiskWriteUsingPwritev2(fd, &iov, n, latencies);
            break;
        case DISK_ENGINE_URING:
            rv = DiskWriteUsingUring(fd, &iov, n, latencies);
            break;
        case DISK_ENGINE_KAIO:
            rv = DiskWriteUsingKaio(fd, &iov, n, latencies);
            break;
        default:
            assert(0);
    }

    duration = TimerStop(&timer);

    free(iov.iov_base);

    if (rv != 0) {
        return -1;
    }

    reportLatency(benchmark, latencies, n);
    reportThroughput(benchmark, duration, opts->size);
    free(latencies);

    rv = DiskFsRemoveTempFile(path, fd);
    if (rv != 0) {
        return -1;
    }

    return 0;
}

int DiskRun(int argc, char *argv[], struct report *report)
{
    struct diskOptions opts;
    char *name;
    struct benchmark *benchmark;
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

    asprintf(&name, "raft::disk::%s::%s::%zu", DiskEngineName(opts.engine),
             DiskModeName(opts.mode), opts.buf);
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
