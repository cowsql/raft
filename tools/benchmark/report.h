/* Utilities for producing https://bencher.dev reports. */

#ifndef REPORT_H_
#define REPORT_H_

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
