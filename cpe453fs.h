#ifndef MACOSX
#ifndef LINUX
#define LINUX
#endif
#endif

#ifndef CPE453FS_H
#define CPE453FS_h

#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#ifndef MACOSX
#include <sys/epoll.h>
#endif

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif

#ifdef LINUX
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif
#endif
#include <fuse.h>

#ifdef  __cplusplus
extern "C" {
#endif

typedef void (*CPE453_readdir_callback_t)(void *, const char*, uint32_t);

struct cpe453fs_ops
{
	// This void * is passed as the first argument to all function calls
	void *arg;


	// Functions necessary for the first version of the file system assignment,
	// a read-only file system
	int (*getattr)(void*, uint32_t block_num, struct stat *stbuf);
	int (*readdir)(void*, uint32_t block_num, void *buf, CPE453_readdir_callback_t cb);
	int (*open)(void*, uint32_t block_num);
	int (*read)(void*, uint32_t block_num, char *buff, size_t size, off_t offset);
	int (*readlink)(void*, uint32_t block_num, char *buff, size_t buff_size);
	uint32_t (*root_node)(void*);
	void (*set_file_descriptor)(void*, int);

	// Functions necessary for a read-write file system.  I suggest implementing
	// them in the order found in this header file.
	int (*chmod)(void*, uint32_t block_num, mode_t new_mode);
	int (*chown)(void*, uint32_t block_num, uid_t new_uid, gid_t new_gid);
	int (*utimens)(void*, uint32_t block_num, const struct timespec tv[2]);
	int (*rmdir)(void*, uint32_t block_num, const char *name);
	int (*unlink)(void*, uint32_t block_num, const char *name);
	int (*mknod)(void*, uint32_t parent_block, const char *name, mode_t new_mode, dev_t new_dev);
	int (*symlink)(void*, uint32_t parent_block, const char *name, const char *link_dest);
	int (*mkdir)(void*, uint32_t parent_block, const char *name, mode_t new_mode);
	int (*link)(void*, uint32_t parent_block, const char *name, uint32_t dest_block);
	int(*rename)(void*, uint32_t old_parent, const char *old_name, uint32_t new_parent, const char *new_name);
	int (*truncate)(void*, uint32_t block_num, off_t new_size);
	int (*write)(void*, uint32_t block_num, const char *buff, size_t wr_len, off_t wr_offset);

	// Optional functions

	// Called when the file system is first initialized by FUSE
	void (*init)(void);
	// Called when the file system is unmounted, but before the program exits
	void (*destroy)(void);
};

extern struct cpe453fs_ops *CPE453_get_operations(void);

extern void readblock(int fd, unsigned char *p, uint32_t bnum);
extern void writeblock(int fd, unsigned char *p, uint32_t bnum);

#ifdef  __cplusplus
}
#endif

#endif
