#include <argp.h>
#include <stdlib.h>

#include "disk.h"
#include "disk_options.h"

static char doc[] = "Benchmark sequential write performance";

/* Order of fields: {NAME, KEY, ARG, FLAGS, DOC, GROUP}.*/
static struct argp_option options[] = {
    {"dir", 'd', "DIR", 0, "Directory to use for temp files (default '.')", 0},
    {"buf", 'b', "BUF", 0, "Write buffer size (default 4096)", 0},
    {"size", 's', "S", 0, "Size of the file to write (default 8M)", 0},
    {"engine", 'e', "ENGINE", 0, "I/O engine to use: pwrite2 or uring", 0},
    {"mode", 'm', "MODE", 0, "I/O mode: buffered or direct", 0},
    {0}};

static error_t argpParser(int key, char *arg, struct argp_state *state);

static struct argp argp = {
    .options = options,
    .parser = argpParser,
    .doc = doc,
};

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
        case 'e':
            opts->engine = DiskEngineCode(arg);
            break;
        case 'm':
            opts->mode = DiskModeCode(arg);
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
    opts->size = 8 * 1024 * 1024;
    opts->engine = DISK_ENGINE_URING;
    opts->mode = DISK_MODE_DIRECT;
}

void DiskParse(int argc, char *argv[], struct diskOptions *opts)
{
    optionsInit(opts);
    argv[0] = "benchmark/run disk";
    argp_parse(&argp, argc, argv, 0, 0, opts);

    if (opts->buf == 0) {
        printf("Invalid buffer size %zu\n", opts->buf);
        exit(1);
    }
    if (opts->size == 0 || opts->size % opts->buf != 0) {
        printf("Invalid file size %u\n", opts->size);
        exit(1);
    }
    if (opts->engine == -1) {
        printf("Invalid engine\n");
        exit(1);
    }
    if (opts->mode == -1) {
        printf("Invalid mode\n");
        exit(1);
    }
}
