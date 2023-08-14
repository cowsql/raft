/* Options for the disk benchmark. */

#ifndef DISK_OPTIONS_H_
#define DISK_OPTIONS_H_

#include <stddef.h>

/* Disk I/O engines. */
enum { DISK_ENGINE_PWRITE = 0, DISK_ENGINE_URING, DISK_ENGINE_KAIO };

/* Disk I/O modes */
enum { DISK_MODE_BUFFERED = 0, DISK_MODE_DIRECT };

/* Options for the disk benchmark */
struct diskOptions
{
    char *dir;     /* Directory to use for creating temporary files */
    size_t buf;    /* Write buffer size */
    unsigned size; /* Size of the file to write, must be a multiple of buf */
    int engine;    /* OS write interface to use */
    int mode;      /* Direct or buffered */
};

/* Translate a disk engine name to the associated code. Return -1 if unknown. */
int DiskEngineCode(const char *name);

/* Translate a disk engine code to its associated name. */
const char *DiskEngineName(int code);

/* Translate a disk mode name to the associated code. Return -1 if unknown. */
int DiskModeCode(const char *mode);

/* Translate a disk mode code to its associated name. */
const char *DiskModeName(int code);

#endif /* DISK_ARGS_H_ */
