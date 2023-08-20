/* Utilities for producing https://bencher.dev reports. */

#ifndef REPORT_H_
#define REPORT_H_

#include <time.h>

enum { METRIC_KIND_LATENCY = 0, METRIC_KIND_THROUGHPUT };

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

/* Fill a metric object with a latency measurement, calculating the 50th
 * percentile over the given samples. */
void MetricFillLatency(struct metric *m, time_t *samples, unsigned n_samples);

/* Fill a metric object with a throughput measurement, given the number of total
 * operations and their total duration. */
void MetricFillThroughput(struct metric *m, unsigned n_ops, time_t duration);

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
