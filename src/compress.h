#ifndef COMPRESS_H_
#define COMPRESS_H_

#include "../include/raft.h"

/*
 * Decompresses the content of `buf` into a newly allocated buffer that is
 * returned to the caller through `decompressed`. Returns a non-0 value upon
 * failure.
 */
int Decompress(struct raft_buffer buf,
               struct raft_buffer *decompressed,
               char *errmsg);

/* Returns `true` if `data` is compressed, `false` otherwise. */
bool IsCompressed(const void *data, size_t sz);

#endif /* COMPRESS_H_ */
