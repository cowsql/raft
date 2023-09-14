#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <libgen.h>
#include <stdio.h>
#include <string.h>
#include <sys/sysmacros.h>
#include <sys/uio.h>
#include <unistd.h>

#include "fs.h"

/* Minimum physical block size for direct I/O that we expect to detect. */
#define MIN_BLOCK_SIZE 512

/* Maximum physical block size for direct I/O that we expect to detect. */
#define MAX_BLOCK_SIZE (1024 * 1024) /* 1M */

#define SYS_BLOCK_PATH "/sys/dev/block/%d:%d"

/* Histgram resolution when the underlying block driver is NVMe. */
#define RESOLUTION_NVME 500        /* buckets are 0.5 microseconds apart */
#define BUCKETS_NVME 20 * 1000 * 2 /* buckets up to 20,000 microseconds */

/* Histgram resolution when the underlying block driver is nullb. */
#define RESOLUTION_NULLB 50     /* buckets are 50 nanoseconds apart */
#define BUCKETS_NULLB 1000 * 20 /* buckets up to 1,000 microseconds */

/* Histgram resolution when the underlying block driver is unspecified. */
#define RESOLUTION_GENERIC 1000   /* buckets are 1 microsecond apart */
#define BUCKETS_GENERIC 20 * 1000 /* buckets up to 20,000 microseconds */

/* Read an integer value from the file /sys/dev/block/<maj>:<min>/name . */
static int readBlockDeviceInfo(unsigned maj,
                               unsigned min,
                               const char *name,
                               unsigned *value)
{
    char path[1024];
    char buf[256];
    FILE *file;
    int rv;
    sprintf(path, SYS_BLOCK_PATH "/%s", maj, min, name);

    file = fopen(path, "r");
    if (file == NULL) {
        /* Special case null_blk devices which don't create a "start" file. */
        if (strcmp(name, "start") == 0 && errno == ENOENT) {
            *value = 0;
            return 0;
        }

        fprintf(stderr, "fopen device info at %s: %s\n", path, strerror(errno));
        return -1;
    }

    rv = (int)fread(buf, sizeof buf, 1, file);
    if (rv < 0) {
        fprintf(stderr, "fread device info at %s: %s\n", path, strerror(errno));
        return -1;
    }

    *value = (unsigned)atoi(buf);

    fclose(file);

    return 0;
}

/* Read the start sector of the given block device. */
static int readBlockDeviceStart(unsigned maj, unsigned min, unsigned *start)
{
    return readBlockDeviceInfo(maj, min, "start", start);
}

/* Read the size in sectors of the given block device. */
static int readBlockDeviceSize(unsigned maj, unsigned min, unsigned *size)
{
    return readBlockDeviceInfo(maj, min, "size", size);
}

/* Check if a block device has power-loss protection via write-through.
 *
 * https://stackoverflow.com/questions/72353566/when-should-i-use-req-op-flush-in-a-kernel-blockdev-driver-do-req-op-flush-bio
 */
static int readBlockDeviceWriteCache(const char *path, bool *write_through)
{
    char buf[256];
    FILE *file;
    int rv;

    file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, "fopen write_cache at %s: %s\n", path, strerror(errno));
        return -1;
    }

    rv = (int)fread(buf, sizeof buf, 1, file);
    if (rv < 0) {
        fprintf(stderr, "fread write_cache at %s: %s\n", path, strerror(errno));
        return -1;
    }

    fclose(file);

    *write_through = strcmp(buf, "write through\n") == 0;

    return 0;
}

int FsFileInfo(const char *path, struct FsFileInfo *info)
{
    struct stat st;
    unsigned maj;
    unsigned min;
    unsigned dev_size;
    char block[1024];
    char link[1024];
    char parent[8192]; /* TODO: suppress -Werror=format-overflow */
    const char *name;
    int rv;

    rv = stat(path, &st);
    if (rv != 0) {
        perror("stat");
        return -1;
    }

    if ((st.st_mode & S_IFMT) == S_IFBLK) {
        info->type = FS_TYPE_DEVICE;
        maj = major(st.st_rdev);
        min = minor(st.st_rdev);
    } else {
        info->type = FS_TYPE_REGULAR;
        maj = major(st.st_dev);
        min = minor(st.st_dev);
    }

    sprintf(block, SYS_BLOCK_PATH, maj, min);

    memset(link, 0, sizeof link);
    rv = (int)readlink(block, link, sizeof link);

    if (rv < 0) {
        if (errno != ENOENT) {
            return -1;
        }
        info->driver = FS_DRIVER_GENERIC;
        goto out;
    }

    rv = readBlockDeviceStart(maj, min, &info->block_dev_start);
    if (rv != 0) {
        return -1;
    }
    rv = readBlockDeviceSize(maj, min, &dev_size);
    if (rv != 0) {
        return -1;
    }
    info->block_dev_end = info->block_dev_start + dev_size;

    name = basename(link);

    if (strcmp(name, "nullb0") == 0) {
        info->driver = FS_DRIVER_NULLB;
        sprintf(parent, "%s/%s/queue/write_cache", dirname(block), link);
    } else if (strncmp(name, "nvme", strlen("nvme")) == 0) {
        info->driver = FS_DRIVER_NVME;
        sprintf(parent, "%s/%s/queue/write_cache", dirname(block),
                dirname(link));
    } else {
        info->driver = FS_DRIVER_GENERIC;
        info->block_dev_write_through = false;
        goto out;
    }

    rv = readBlockDeviceWriteCache(parent, &info->block_dev_write_through);
    if (rv != 0) {
        return rv;
    }

out:
    switch (info->driver) {
        case FS_DRIVER_NVME:
            info->buckets = BUCKETS_NVME;
            info->resolution = RESOLUTION_NVME;
            break;
        case FS_DRIVER_NULLB:
            info->buckets = BUCKETS_NULLB;
            info->resolution = RESOLUTION_NULLB;
            break;
        case FS_DRIVER_GENERIC:
            info->buckets = BUCKETS_GENERIC;
            info->resolution = RESOLUTION_GENERIC;
            break;
        default:
            assert(0);
            break;
    }

    return 0;
}

static char *makeTempTemplate(const char *dir)
{
    char *path;
    path = malloc(strlen(dir) + strlen("/bench-XXXXXX") + 1);
    assert(path != NULL);
    sprintf(path, "%s/bench-XXXXXX", dir);
    return path;
}

int FsCreateTempFile(const char *dir, size_t size, char **path, int *fd)
{
    int flags = O_WRONLY | O_CREAT | O_EXCL | O_DIRECT;
    void *buf;
    int dirfd;
    int rv;

    *path = makeTempTemplate(dir);
    *fd = mkostemp(*path, flags);
    if (*fd == -1) {
        printf("mstemp '%s': %s\n", *path, strerror(errno));
        return -1;
    }

    rv = posix_fallocate(*fd, 0, (off_t)size);
    if (rv != 0) {
        errno = rv;
        printf("posix_fallocate: %s\n", strerror(errno));
        unlink(*path);
        return -1;
    }

    buf = aligned_alloc(size, size);
    assert(buf != NULL);

    rv = (int)pwrite(*fd, buf, size, 0);

    free(buf);

    if (rv != (int)size) {
        printf("pwrite '%s': %d\n", *path, rv);
        return -1;
    }

    /* Sync the file and its directory. */
    rv = fsync(*fd);
    assert(rv == 0);

    dirfd = open(dir, O_RDONLY | O_DIRECTORY);
    assert(dirfd != -1);

    rv = fsync(dirfd);
    assert(rv == 0);

    close(dirfd);

    return 0;
}

int FsRemoveTempFile(char *path, int fd)
{
    int rv;

    rv = close(fd);
    if (rv != 0) {
        printf("close '%s': %s\n", path, strerror(errno));
        goto out;
    }
    rv = unlink(path);
    if (rv != 0) {
        printf("unlink '%s': %s\n", path, strerror(errno));
        goto out;
    }

out:
    free(path);
    return rv;
}

int FsCreateTempDir(const char *dir, char **path)
{
    *path = makeTempTemplate(dir);
    if (*path == NULL) {
        return -1;
    }
    *path = mkdtemp(*path);
    if (*path == NULL) {
        return -1;
    }
    return 0;
}

/* Wrapper around remove(), compatible with ntfw. */
static int dirRemoveFn(const char *path,
                       const struct stat *sbuf,
                       int type,
                       struct FTW *ftwb)
{
    (void)sbuf;
    (void)type;
    (void)ftwb;
    return remove(path);
}

int FsRemoveTempDir(char *path)
{
    int rv;
    rv = nftw(path, dirRemoveFn, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
    if (rv != 0) {
        return -1;
    }
    free(path);
    return 0;
}

int FsOpenBlockDevice(const char *dir, int *fd)
{
    *fd = open(dir, O_WRONLY | O_DIRECT);
    if (*fd == -1) {
        printf("open '%s': %s\n", dir, strerror(errno));
        return -1;
    }
    return 0;
}

int FsFileExists(const char *dir, const char *name, bool *exists)
{
    char *path;
    struct stat st;
    int rv;

    path = malloc(strlen(dir) + strlen(name) + 1 + 1);
    assert(path != NULL);
    sprintf(path, "%s/%s", dir, name);

    rv = stat(path, &st);

    free(path);

    if (rv != 0) {
        if (errno != ENOENT) {
            return -1;
        }
        *exists = false;
    } else {
        *exists = true;
    }

    return 0;
}
