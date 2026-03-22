#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include "cpe453fs.h"

struct hello_args
{
	int fd;
};

static const char *hello_str = "Hello World!\n";
static const char *hello_path = "/hello";

static void hello_set_file_descriptor(void *args, int fd)
{
	struct hello_args *hello = (struct hello_args*)args;
	hello->fd = fd;
}

static int hello_getattr(void *args, uint32_t block_num, struct stat *stbuf)
{
    int res = 0;

	printf("GETATTR %d\n", block_num);
    if(block_num == 1) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
    }
    else if(block_num == 2) {
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = strlen(hello_str);
    }
    else
        res = -ENOENT;

    return res;
}

static int hello_readdir(void *args, uint32_t block_num, void *buf, CPE453_readdir_callback_t cb)
{
	printf("READDIR %d\n", block_num);
    if(block_num != 1)
        return -ENOENT;

    cb(buf, hello_path + 1, 2);

    return 0;
}

static int hello_open(void *args, uint32_t block_num)
{
    if(block_num != 2)
        return -ENOENT;

    return 0;
}

static int hello_read(void *args, uint32_t block_num, char *buf, size_t size, off_t offset)
{
    off_t len;
    if(block_num != 2)
        return -ENOENT;

    len = strlen(hello_str);
    if (offset < len) {
        if ( (off_t)(offset + size) > len)
            size = len - offset;
        memcpy(buf, hello_str + offset, size);
    } else
        size = 0;

    return size;
}

static uint32_t hello_root_node(void *args)
{
	return 1;
}

#ifdef  __cplusplus
extern "C" {
#endif

struct cpe453fs_ops *CPE453_get_operations(void)
{
	static struct cpe453fs_ops ops;
	static struct hello_args args;
	memset(&ops, 0, sizeof(ops));
	ops.arg = &args;

	ops.getattr = hello_getattr;
	ops.readdir = hello_readdir;
	ops.open = hello_open;
	ops.read = hello_read;
	ops.root_node = hello_root_node;
	ops.set_file_descriptor = hello_set_file_descriptor;

	return &ops;
}

#ifdef  __cplusplus
}
#endif

