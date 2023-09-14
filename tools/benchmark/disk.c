#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "disk.h"
#include "disk_options.h"
#include "disk_parse.h"
#include "disk_uring.h"
#include "fs.h"
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

/* Prepare the file or device to write to. */
static int openFile(struct diskOptions *opts,
                    struct FsFileInfo *info,
                    int *fd,
                    char **path)
{
    unsigned n = opts->size / (unsigned)opts->buf;
    int rv;

    rv = FsFileInfo(opts->dir, info);
    if (rv != 0) {
        printf("file info '%s': %s\n", opts->dir, strerror(errno));
        return -1;
    }

    if (info->type == FS_TYPE_DEVICE) {
        rv = FsOpenBlockDevice(opts->dir, fd);
    } else {
        rv = FsCreateTempFile(opts->dir, n * opts->buf, path, fd);
        if (rv == 0) {
            rv = FsFileInfo(*path, info);
        }
    }
    if (rv != 0) {
        return -1;
    }

    return 0;
}

static int closeFile(struct FsFileInfo *info, int fd, char *path)
{
    int rv;

    if (info->type != FS_TYPE_DEVICE) {
        rv = FsRemoveTempFile(path, fd);
    } else {
        rv = close(fd);
    }
    if (rv != 0) {
        return -1;
    }

    return 0;
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

int DiskRun(int argc, char *argv[], struct report *report)
{
    struct diskOptions opts;
    struct Profiler profiler;
    struct FsFileInfo info;
    struct benchmark *benchmark;
    struct timer timer;
    struct histogram histogram;
    struct iovec iov;
    char *name;
    char *path;
    int fd;
    unsigned long duration;
    unsigned i;
    unsigned n;

    int rv;

    DiskParse(argc, argv, &opts);

    rv = openFile(&opts, &info, &fd, &path);
    if (rv != 0) {
        return -1;
    }

    ProfilerInit(&profiler, &info);

    if (opts.perf) {
        rv = ProfilerPerf(&profiler);
        if (rv != 0) {
            return -1;
        }
    }

    for (i = 0; i < opts.n_traces; i++) {
        ProfilerTrace(&profiler, opts.traces[i]);
    }

    allocBuffer(&iov, opts.buf);

    HistogramInit(&histogram, info.buckets, info.resolution);

    TimerStart(&timer);

    n = opts.size / (unsigned)opts.buf;
    rv = DiskWriteUsingUring(fd, &iov, n, &profiler, &histogram);

    duration = TimerStop(&timer);

    free(iov.iov_base);

    if (rv != 0) {
        return -1;
    }

    rv = closeFile(&info, fd, path);
    if (rv != 0) {
        return -1;
    }

    /* 262144 is the maximum buffer size where no context switches happen,
     * presumably because io_uring inlines smaller requests and uses the
     * threadpool for larger ones. */
    if (opts.perf && profiler.switches != 0 &&
        info.driver != FS_DRIVER_GENERIC) {
        printf("Error: unexpected context switches: %u\n", profiler.switches);
        return -1;
    }

    /* Only report disk benchmarks if kernel sub-systems performance measurement
     * is disabled.
     *
     * In CI we run the "raft-benchmark disk" command twice: once with kernel
     * sub-systems performance measurement enabled, to report raw block/nvme
     * metrics, and once with kernel sub-systems performance measurement
     * disabled, to report the actual end-to-end metrics. That's because
     * enabling kernel sub-systems performance measurement has a noticeable
     * (albeit low) overhead.
     */
    if (!opts.perf) {
        rv = asprintf(&name, "disk:%zu", opts.buf);
        assert(rv > 0);
        assert(name != NULL);

        benchmark = ReportGrow(report, name);
        reportLatency(benchmark, &histogram);
        reportThroughput(benchmark, duration, opts.size);
    }

    HistogramClose(&histogram);

    if (opts.perf && info.driver != FS_DRIVER_GENERIC) {
        struct ProfilerDataSource *data;
        const char *system;

        system = "block";
        data = &profiler.block;
        rv = asprintf(&name, "disk:%s:%zu", system, opts.buf);
        assert(rv > 0);
        assert(name != NULL);

        HistogramInit(&histogram, info.buckets, info.resolution);

        if (data->n_commands != n && info.driver != FS_DRIVER_GENERIC) {
            printf("Error: unexpected commands: %u\n", data->n_commands);
            return -1;
        }
        for (i = 0; i < data->n_commands; i++) {
            assert(data->commands[i].duration > 0);
            HistogramCount(&histogram, data->commands[i].duration);
        }

        benchmark = ReportGrow(report, name);
        reportLatency(benchmark, &histogram);
        HistogramClose(&histogram);

        if (info.driver == FS_DRIVER_NVME) {
            system = "nvme";
            data = &profiler.block;
            rv = asprintf(&name, "disk:%s:%zu", system, opts.buf);
            assert(rv > 0);
            assert(name != NULL);

            HistogramInit(&histogram, info.buckets, info.resolution);

            if (data->n_commands != n) {
                printf("Error: unexpected commands: %u\n", data->n_commands);
                return -1;
            }
            for (i = 0; i < data->n_commands; i++) {
                assert(data->commands[i].duration > 0);
                HistogramCount(&histogram, data->commands[i].duration);
            }

            benchmark = ReportGrow(report, name);
            reportLatency(benchmark, &histogram);
            HistogramClose(&histogram);
        }
    }

    ProfilerClose(&profiler);

    return 0;
}
