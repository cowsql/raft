/* File-system utilities. */

#ifndef FS_H_
#define FS_H_

#include <stdbool.h>
#include <stdlib.h>

enum {
    FS_TYPE_REGULAR, /* Regular file or directory */
    FS_TYPE_DEVICE,  /* Block device */
};

enum {
    FS_DRIVER_NVME = 0, /* NVMe driver */
    FS_DRIVER_NULLB,    /* nullb driver */
    FS_DRIVER_GENERIC,  /* Unspecified underlying driver */
};

/* Hold information about a file. */
struct FsFileInfo
{
    unsigned type;            /* Regular file or device. */
    unsigned driver;          /* Underlying block device driver */
    unsigned buckets;         /* N. of buckets to use for histograms */
    unsigned resolution;      /* Resolution to use for histograms */
    unsigned block_dev_start; /* First sector of the underlying block device */
    unsigned block_dev_end;   /* End sector of the underlying block device */
    bool block_dev_write_through; /* True if device has power-loss protection */
};

/* Detect file information. */
int FsFileInfo(const char *path, struct FsFileInfo *info);

/* Create a temporary file of the given size. */
int FsCreateTempFile(const char *dir, size_t size, char **path, int *fd);

/* Remove a temporary file. */
int FsRemoveTempFile(char *path, int fd);

/* Create a temporary directory under the given dir. */
int FsCreateTempDir(const char *dir, char **path);

/* Recursively remove a temporary dir. */
int FsRemoveTempDir(char *path);

/* Open a block device for raw I/O. */
int FsOpenBlockDevice(const char *dir, int *fd);

/* Check if a file exists in the given dir. */
int FsFileExists(const char *dir, const char *name, bool *exists);

#endif /* FS_H_ */
