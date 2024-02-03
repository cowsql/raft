#include "../../src/uv_os.h"
#include "../lib/dir.h"
#include "../lib/runner.h"

SUITE(UvOsJoin)

/* dir and filename have sensible lengths */
TEST(UvOsJoin, basic, NULL, NULL, 0, NULL)
{
    int rv;
    const char *dir = "/home";
    const char *filename = "testfile";
    char path[UV__PATH_SZ];
    rv = UvOsJoin(dir, filename, path);
    munit_assert_int(rv, ==, 0);
    munit_assert_string_equal(path, "/home/testfile");
    return MUNIT_OK;
}

TEST(UvOsJoin, dirTooLong, NULL, NULL, 0, NULL)
{
    int rv;
    char path[UV__PATH_SZ];
    char dir[UV__DIR_LEN + 2]; /* Room for '\0' and then 1 char over limit. */
    memset((char *)dir, '/', sizeof(dir));
    dir[sizeof(dir) - 1] = '\0';
    const char *filename = "testfile";

    rv = UvOsJoin(dir, filename, path);
    munit_assert_int(rv, !=, 0);
    return MUNIT_OK;
}

TEST(UvOsJoin, filenameTooLong, NULL, NULL, 0, NULL)
{
    int rv;
    char path[UV__PATH_SZ];
    const char *dir = "testdir";
    char filename[UV__FILENAME_LEN + 2];
    memset((char *)filename, 'a', sizeof(filename));
    filename[sizeof(filename) - 1] = '\0';

    rv = UvOsJoin(dir, filename, path);
    munit_assert_int(rv, !=, 0);
    return MUNIT_OK;
}

TEST(UvOsJoin, dirAndFilenameTooLong, NULL, NULL, 0, NULL)
{
    int rv;
    char path[UV__PATH_SZ];
    char dir[UV__DIR_LEN + 2];
    memset((char *)dir, '/', sizeof(dir));
    dir[sizeof(dir) - 1] = '\0';

    char filename[UV__FILENAME_LEN + 2];
    memset((char *)filename, 'a', sizeof(filename));
    filename[sizeof(filename) - 1] = '\0';

    rv = UvOsJoin(dir, filename, path);
    munit_assert_int(rv, !=, 0);
    return MUNIT_OK;
}

TEST(UvOsJoin, dirAndFilenameMax, NULL, NULL, 0, NULL)
{
    int rv;
    char path[UV__PATH_SZ];
    char dir[UV__DIR_LEN + 1];
    memset((char *)dir, '/', sizeof(dir));
    dir[sizeof(dir) - 1] = '\0';

    char filename[UV__FILENAME_LEN + 1];
    memset((char *)filename, 'a', sizeof(filename));
    filename[sizeof(filename) - 1] = '\0';

    rv = UvOsJoin(dir, filename, path);
    munit_assert_int(rv, ==, 0);
    char cmp_path[UV__DIR_LEN + UV__FILENAME_LEN + 1 + 1];
    snprintf(cmp_path, UV__DIR_LEN + UV__FILENAME_LEN + 1 + 1, "%s/%s", dir,
             filename);
    munit_assert_string_equal(path, cmp_path);
    return MUNIT_OK;
}

SUITE(UvOsOpen)

TEST(UvOsOpen, Tmpfile, DirSetUp, DirTearDown, 0, DirAllParams)
{
    const char *dir = data;
    uv_file fd;
    int rv;

    if (dir == NULL) {
        return MUNIT_SKIP;
    }

    rv = UvOsOpen(dir, O_TMPFILE | O_WRONLY, S_IRUSR | S_IWUSR, &fd);
    munit_assert_int(rv, ==, 0);

    rv = UvOsClose(fd);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}
