/* Utilities for producing https://bencher.dev reports. */

#ifndef REPORT_H_
#define REPORT_H_

#include <time.h>

enum { METRIC_KIND_LATENCY = 0, METRIC_KIND_THROUGHPUT };

struct histogram
{
    unsigned *buckets;        /* buckets */
    unsigned n;               /* number of buckets */
    unsigned long long first; /* value of the first bucket */
    unsigned gap;             /* gap between subsequent buckets */
};

struct metric
{
    int kind;
    double value;
    double lower_bound;
    double upper_bound;
};

struct benchmark
{
    char *name;
    struct metric **metrics;
    unsigned n_metrics;
};

struct report
{
    struct benchmark **benchmarks;
    unsigned n_benchmarks;
};

/* Initialize a histogram object. */
void HistogramInit(struct histogram *h, unsigned n, unsigned gap);

void HistogramClose(struct histogram *h);

/* Update the counter of the bucket associated with the given value. */
void HistogramCount(struct histogram *h, unsigned long value);

/* Fill a metric object with a histogram-based measurement, calculating the 50th
 * percentile over the buckets. */
void MetricFillHistogram(struct metric *m, struct histogram *h);

/* Fill a metric object with a throughput measurement, given the number of total
 * operations and their total duration. */
void MetricFillThroughput(struct metric *m,
                          unsigned n_ops,
                          unsigned long duration);

/* Add a new metric to a benchmark. */
struct metric *BenchmarkGrow(struct benchmark *b, int kind);

/* Initialize an empty report. */
void ReportInit(struct report *r);

/* Free all allocated memory. */
void ReportClose(struct report *r);

/* Add a new benchmark to a report. */
struct benchmark *ReportGrow(struct report *r, char *name);

/* Write the given report to stdout, using the JSON format. */
void ReportPrint(struct report *r);

#endif /* REPORT_H_ */
