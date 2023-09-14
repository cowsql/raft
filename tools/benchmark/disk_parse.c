#include <argp.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "disk.h"
#include "disk_parse.h"

#define MEGABYTE (1024 * 1024)

static char doc[] = "Benchmark sequential write performance\n";

/* Order of fields: {NAME, KEY, ARG, FLAGS, DOC, GROUP}.*/
static struct argp_option options[] = {
    {"dir", 'd', "DIR", 0, "Directory to use for temp files (default '.')", 0},
    {"buf", 'b', "BUF", 0, "Write buffer size (default 4096)", 0},
    {"size", 's', "S", 0, "Size of the file to write (default 8M)", 0},
    {"perf", 'p', NULL, 0, "Report kernel subsystems performance metrics", 0},
    {"trace", 't', "TRACE", 0, "Comma-separated kernel subsystems to trace", 0},
    {0}};

static error_t argpParser(int key, char *arg, struct argp_state *state);

static struct argp argp = {
    .options = options,
    .parser = argpParser,
    .doc = doc,
};

/* Parse a comma-separated list of kernels subsystem names. */
static void parseTracing(struct diskOptions *opts, char *arg)
{
    char *token;
    unsigned n_tokens = 1;
    unsigned i;

    /* Count the number of comma-separated tokens in this argument. */
    for (i = 0; i < strlen(arg); i++) {
        if (arg[i] == ',')
            n_tokens++;
    }

    for (i = 0; i < n_tokens; i++) {
        if (i == 0) {
            token = strtok(arg, ",");
        } else {
            token = strtok(NULL, ",");
        }
        assert(token != NULL);
        opts->traces[opts->n_traces] = token;
        opts->n_traces++;
    }
}

static error_t argpParser(int key, char *arg, struct argp_state *state)
{
    struct diskOptions *opts = state->input;

    switch (key) {
        case 'd':
            opts->dir = arg;
            break;
        case 'b':
            opts->buf = (size_t)atoi(arg);
            break;
        case 's':
            opts->size = (unsigned)atoi(arg);
            break;
        case 'p':
            opts->perf = true;
            break;
        case 't':
            parseTracing(opts, arg);
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }

    return 0;
}

static void optionsInit(struct diskOptions *opts)
{
    opts->dir = ".";
    opts->buf = 4096;
    opts->size = 8 * MEGABYTE;
    opts->perf = false;
    opts->n_traces = 0;
}

static void optionsCheck(struct diskOptions *opts)
{
    if (opts->buf == 0 || opts->buf > MEGABYTE) {
        printf("Invalid buffer size %zu\n", opts->buf);
        exit(1);
    }
    if (opts->size == 0 || opts->size % 4096 != 0) {
        printf("Invalid file size %u\n", opts->size);
        exit(1);
    }
    if (opts->perf && getuid() != 0) {
        printf("Performance measurment requires root\n");
        exit(1);
    }
    if (opts->n_traces > 0 && getuid() != 0) {
        printf("Tracing requires root\n");
        exit(1);
    }
}

void DiskParse(int argc, char *argv[], struct diskOptions *opts)
{
    optionsInit(opts);

    argv[0] = "benchmark/run disk";
    argp_parse(&argp, argc, argv, 0, 0, opts);

    optionsCheck(opts);
}
