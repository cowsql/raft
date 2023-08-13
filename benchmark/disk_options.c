#include <string.h>

#include "disk_options.h"

static const char *engines[] = {[DISK_ENGINE_PWRITEV2] = "pwritev2",
                                [DISK_ENGINE_URING] = "uring",
                                [DISK_ENGINE_KAIO] = "kaio",
                                NULL};

static const char *modes[] =
    {[DISK_MODE_BUFFERED] = "buffered", [DISK_MODE_DIRECT] = "direct", NULL};

int DiskEngineCode(const char *name)
{
    int i = 0;
    while (engines[i] != NULL) {
        if (strcmp(engines[i], name) == 0) {
            return i;
        }
        i++;
    }
    return -1;
}

const char *DiskEngineName(int code)
{
    return engines[code];
}

int DiskModeCode(const char *mode)
{
    int i = 0;
    while (modes[i] != NULL) {
        if (strcmp(modes[i], mode) == 0) {
            return i;
        }
        i++;
    }
    return -1;
}

const char *DiskModeName(int code)
{
    return modes[code];
}
