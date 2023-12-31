#include "compress.h"

#ifdef LZ4_AVAILABLE
#include <lz4frame.h>
#endif
#include <limits.h>
#include <string.h>

#include "assert.h"
#include "byte.h"
#include "err.h"

#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))

int Decompress(struct raft_buffer buf,
               struct raft_buffer *decompressed,
               char *errmsg)
{
#ifndef LZ4_AVAILABLE
    (void)buf;
    (void)decompressed;
    ErrMsgPrintf(errmsg, "LZ4 not available");
    return RAFT_INVALID;
#else
    assert(decompressed != NULL);

    int rv = RAFT_IOERR;
    size_t src_offset = 0;
    size_t dst_offset = 0;
    size_t src_size = 0;
    size_t dst_size = 0;
    size_t ret = 0;

    LZ4F_decompressionContext_t ctx;
    if (LZ4F_isError(LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION))) {
        ErrMsgPrintf(errmsg, "LZ4F_createDecompressionContext");
        rv = RAFT_NOMEM;
        goto err;
    }

    src_size = buf.len;
    LZ4F_frameInfo_t frameInfo = {0};
    /* `src_size` will contain the size of the LZ4 Frame Header after the call,
     * decompression must resume at that offset. */
    ret = LZ4F_getFrameInfo(ctx, &frameInfo, buf.base, &src_size);
    if (LZ4F_isError(ret)) {
        ErrMsgPrintf(errmsg, "LZ4F_getFrameInfo %s", LZ4F_getErrorName(ret));
        rv = RAFT_IOERR;
        goto err_after_ctx_alloc;
    }
    src_offset = src_size;

    decompressed->base = raft_malloc((size_t)frameInfo.contentSize);
    decompressed->len = (size_t)frameInfo.contentSize;
    if (decompressed->base == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_ctx_alloc;
    }

    ret = 1;
    while (ret != 0) {
        src_size = buf.len - src_offset;
        /* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         * The next line works around a bug in an older lz4 lib where the
         * `size_t` dst_size parameter would overflow an `int`.
         * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! */
        dst_size = min(decompressed->len - dst_offset, (size_t)INT_MAX);
        /* `dst_size` will contain the number of bytes written to
         * decompressed->base, while `src_size` will contain the number of bytes
         * consumed from buf.base */
        ret = LZ4F_decompress(ctx, (char *)decompressed->base + dst_offset,
                              &dst_size, (char *)buf.base + src_offset,
                              &src_size, NULL);
        if (LZ4F_isError(ret)) {
            ErrMsgPrintf(errmsg, "LZ4F_decompress %s", LZ4F_getErrorName(ret));
            rv = RAFT_IOERR;
            goto err_after_buff_alloc;
        }
        src_offset += src_size;
        dst_offset += dst_size;
    }

    if (LZ4F_freeDecompressionContext(ctx) != 0) {
        raft_free(decompressed->base);
        decompressed->base = NULL;
        return RAFT_IOERR;
    }

    return 0;

err_after_buff_alloc:
    raft_free(decompressed->base);
    decompressed->base = NULL;
err_after_ctx_alloc:
    LZ4F_freeDecompressionContext(ctx);
err:
    return rv;
#endif /* LZ4_AVAILABLE */
}

bool IsCompressed(const void *data, size_t sz)
{
    if (data == NULL || sz < 4) {
        return false;
    }
    const uint8_t *cursor = data;
#ifdef LZ4F_MAGICNUMBER
#define RAFT_LZ4F_MAGICNUMBER LZ4F_MAGICNUMBER
#else
#define RAFT_LZ4F_MAGICNUMBER 0x184D2204U
#endif
    return byteGet32(&cursor) == RAFT_LZ4F_MAGICNUMBER;
}
