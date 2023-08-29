#include <argp.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "disk.h"
#include "disk_parse.h"

#define MEGABYTE (1024 * 1024)

static char doc[] =
    "Benchmark sequential write performance\n\n"
    "Each option can be passed multiple comma-separated arguments.\n";

/* Order of fields: {NAME, KEY, ARG, FLAGS, DOC, GROUP}.*/
static struct argp_option options[] = {
    {"dir", 'd', "DIR", 0, "Directory to use for temp files (default '.')", 0},
    {"buf", 'b', "BUF", 0, "Write buffer size (default 4096)", 0},
    {"size", 's', "S", 0, "Size of the file to write (default 8M)", 0},
    {"engine", 'e', "ENGINE", 0, "I/O engine to use: pwrite, kaio or uring", 0},
    {"tracing", 't', "TRACING", 0, "Enable tracing using debugfs", 0},
    {0}};

static error_t argpParser(int key, char *arg, struct argp_state *state);

static struct argp argp = {
    .options = options,
    .parser = argpParser,
    .doc = doc,
};

/* Expand the run options in the given matrix by multiplying them by the given
 * factor. */
static void expandMatrix(struct diskMatrix *matrix, unsigned factor)
{
    unsigned n_opts = matrix->n_opts * factor;
    unsigned i;
    unsigned j;
    assert(factor > 1);
    matrix->opts = realloc(matrix->opts, n_opts * sizeof *matrix->opts);
    assert(matrix->opts != NULL);

    /* Create copies of the original options. */
    for (i = 1; i < factor; i++) {
        for (j = 0; j < matrix->n_opts; j++) {
            matrix->opts[i * matrix->n_opts + j] = matrix->opts[j];
        }
    }

    matrix->n_opts = n_opts;
}

static error_t argpParser(int key, char *arg, struct argp_state *state)
{
    struct diskMatrix *matrix = state->input;
    struct diskOptions *opts;
    char *token;
    unsigned n_opts = matrix->n_opts; /* Original size of the matrix */
    unsigned n_tokens = 1;
    unsigned i;
    unsigned j;
    unsigned k;
    bool expanded = false;

    /* All our flags require and argument. So if there's no argument, this is
     * not a supported flag. */
    if (arg == NULL) {
        return ARGP_ERR_UNKNOWN;
    }

    /* Count the number of comma-separated tokens in this argument. */
    for (i = 0; i < strlen(arg); i++) {
        if (arg[i] == ',')
            n_tokens++;
    }

    k = 0;
    for (i = 0; i < n_tokens; i++) {
        if (i == 0) {
            token = strtok(arg, ",");
        } else {
            token = strtok(NULL, ",");
        }
        assert(token != NULL);

        switch (key) {
            case 't':
                TracingAdd(&matrix->tracing, token);
                continue;
        }

        /* Multiply the matrix by the number of tokens. */
        if (!expanded && n_tokens > 1) {
            expandMatrix(matrix, n_tokens);
            expanded = true;
        }

        for (j = 0; j < n_opts; j++) {
            opts = &matrix->opts[k];
            k++;
            switch (key) {
                case 'd':
                    opts->dir = token;
                    break;
                case 'b':
                    opts->buf = (size_t)atoi(token);
                    break;
                case 's':
                    opts->size = (unsigned)atoi(token);
                    break;
                case 'e':
                    opts->engine = DiskEngineCode(token);
                    break;
                default:
                    return ARGP_ERR_UNKNOWN;
            }
        }
    }

    return 0;
}

static void optionsInit(struct diskOptions *opts)
{
    opts->dir = ".";
    opts->buf = 4096;
    opts->size = 8 * MEGABYTE;
    opts->engine = DISK_ENGINE_URING;
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
    if (opts->engine == -1) {
        printf("Invalid engine\n");
        exit(1);
    }
}

void DiskParse(int argc, char *argv[], struct diskMatrix *matrix)
{
    unsigned i;

    matrix->opts = malloc(sizeof *matrix->opts);
    assert(matrix->opts != NULL);
    matrix->n_opts = 1;

    optionsInit(&matrix->opts[0]);
    TracingInit(&matrix->tracing);

    argv[0] = "benchmark/run disk";
    argp_parse(&argp, argc, argv, 0, 0, matrix);

    for (i = 0; i < matrix->n_opts; i++) {
        optionsCheck(&matrix->opts[i]);
    }
}
