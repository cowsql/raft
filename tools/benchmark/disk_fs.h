/* File-system utilities. */

#ifndef DISK_FS_H_
#define DISK_FS_H_

#include <stdlib.h>

/* Create a temporary file of the given size. */
int DiskFsCreateTempFile(const char *dir, size_t size, char **path, int *fd);

/* Remove a temporary file. */
int DiskFsRemoveTempFile(char *path, int fd);

/* Check if direct I/O is available when writing to files in the given dir using
 * the given buffer size. */
int DiskFsCheckDirectIO(const char *dir, size_t buf);

/* Set direct I/O on the given file descriptor. */
int DiskFsSetDirectIO(int fd);

#endif /* DISK_FS_H_ */
