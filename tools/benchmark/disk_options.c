#include <string.h>

#include "disk_options.h"

static const char *engines[] = {[DISK_ENGINE_PWRITE] = "pwrite",
                                [DISK_ENGINE_URING] = "uring",
                                [DISK_ENGINE_KAIO] = "kaio",
                                NULL};

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
