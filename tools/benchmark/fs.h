/* File-system utilities. */

#ifndef FS_H_
#define FS_H_

#include <stdlib.h>

/* Create a temporary file of the given size. */
int FsCreateTempFile(const char *dir, size_t size, char **path, int *fd);

/* Remove a temporary file. */
int FsRemoveTempFile(char *path, int fd);

/* Create a temporary directory under the given dir. */
int FsCreateTempDir(const char *dir, char **path);

/* Check if direct I/O is available when writing to files in the given dir using
 * the given buffer size. */
int FsCheckDirectIO(const char *dir, size_t buf);

/* Set direct I/O on the given file descriptor. */
int FsSetDirectIO(int fd);

#endif /* FS_H_ */
