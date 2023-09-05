#include <argp.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "submit.h"
#include "submit_parse.h"

#define MEGABYTE (1024 * 1024)

static char doc[] = "Benchmark sequential submit requests\n";

/* Order of fields: {NAME, KEY, ARG, FLAGS, DOC, GROUP}.*/
static struct argp_option options[] = {
    {"dir", 'd', "DIR", 0, "Directory to use for temp files (default '.')", 0},
    {"buf", 'b', "BUF", 0, "Size of each entry to submit (default 4096)", 0},
    {"size", 's', "S", 0, "Total number of bytes to submit (default 8M)", 0},
    {0}};

static error_t argpParser(int key, char *arg, struct argp_state *state);

static struct argp argp = {
    .options = options,
    .parser = argpParser,
    .doc = doc,
};

static error_t argpParser(int key, char *arg, struct argp_state *state)
{
    struct submitOptions *opts = state->input;

    switch (key) {
        case 'd':
            opts->dir = arg;
            break;
        case 'b':
            opts->buf = (unsigned)atoi(arg);
            break;
        case 's':
            opts->size = (unsigned)atoi(arg);
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }

    return 0;
}

static void optionsInit(struct submitOptions *opts)
{
    opts->dir = ".";
    opts->buf = 2048;
    opts->size = 8 * MEGABYTE;
}

static void optionsCheck(struct submitOptions *opts)
{
    if (opts->buf == 0 || opts->buf > MEGABYTE) {
        printf("Invalid buffer entry size %zu\n", opts->buf);
        exit(1);
    }
    if (opts->size == 0 || opts->size % 4096 != 0) {
        printf("Invalid total submission size %u\n", opts->size);
        exit(1);
    }
}

void SubmitParse(int argc, char *argv[], struct submitOptions *opts)
{
    optionsInit(opts);

    argv[0] = "benchmark/run submit";
    argp_parse(&argp, argc, argv, 0, 0, opts);

    optionsCheck(opts);
}
