#include "../../src/byte.h"
#include "../../src/compress.h"
#include "../lib/munit.h"
#include "../lib/runner.h"

SUITE(Compress)

#ifdef LZ4_AVAILABLE

static unsigned char lz4_data[] = {
    0x4,  0x22, 0x4d, 0x18, 0x4c, 0x40, 0xd,  0x0,  0x0,  0x0,
    0x0,  0x0,  0x0,  0x0,  0x39, 0xd,  0x0,  0x0,  0x80, 0x68,
    0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64,
    0xa,  0x0,  0x0,  0x0,  0x0,  0x0,  0xf0, 0xb4, 0x59, 0x85};

TEST(Compress, decompress, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed;
    struct raft_buffer decompressed;
    int rv;

    compressed.base = lz4_data;
    compressed.len = sizeof lz4_data;

    rv = Decompress(compressed, &decompressed, errmsg);
    munit_assert_int(rv, ==, 0);

    munit_assert_string_equal(decompressed.base, "hello world\n");

    raft_free(decompressed.base);

    return MUNIT_OK;
}

#else

TEST(Compress, lz4Disabled, NULL, NULL, 0, NULL)
{
    struct raft_buffer buf = {0};
    struct raft_buffer decompressed;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];

    munit_assert_int(Decompress(buf, &decompressed, errmsg), ==, RAFT_INVALID);
    munit_assert_string_equal(errmsg, "LZ4 not available");

    return MUNIT_OK;
}

#endif /* LZ4_AVAILABLE */

static const char LZ4_MAGIC[4] = {0x04, 0x22, 0x4d, 0x18};

TEST(Compress, isCompressedTooSmall, NULL, NULL, 0, NULL)
{
    munit_assert_false(IsCompressed(&LZ4_MAGIC[1], sizeof(LZ4_MAGIC) - 1));
    return MUNIT_OK;
}

TEST(Compress, isCompressedNull, NULL, NULL, 0, NULL)
{
    munit_assert_false(IsCompressed(NULL, sizeof(LZ4_MAGIC)));
    return MUNIT_OK;
}

TEST(Compress, isCompressed, NULL, NULL, 0, NULL)
{
    munit_assert_true(IsCompressed(LZ4_MAGIC, sizeof(LZ4_MAGIC)));
    return MUNIT_OK;
}

TEST(Compress, notCompressed, NULL, NULL, 0, NULL)
{
    char not_compressed[4] = {0x18, 0x4d, 0x22, 0x04};
    munit_assert_false(IsCompressed(not_compressed, sizeof(not_compressed)));
    return MUNIT_OK;
}
