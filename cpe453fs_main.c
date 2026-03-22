#ifndef MACOSX
#ifndef LINUX
#define LINUX
#endif
#endif

#define FUSE_USE_VERSION 26
#ifdef LINUX
#define _XOPEN_SOURCE 500
#endif

#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/param.h>
#include <sys/statvfs.h>

#include "cpe453fs.h"

static struct cpe453fs_ops *fs_ops = NULL;
static int fd = 0;

void readblock(int fd, unsigned char *p, uint32_t bnum)
{
	unsigned char buff[10240];
	int len;
		
	
        if ((len = pread(fd, buff, 4096, 4096*bnum)) < 0)
	{
		perror("Error reading form file");
      printf("   Block %u offset %u\n", bnum, 4096*bnum);
		exit(0);
	}
	memcpy(p, buff, len);
}

void writeblock(int fd, unsigned char *p, uint32_t bnum)
{
	unsigned char buff[10240];

	memcpy(buff, p, 4096);
	if (pwrite(fd, buff, 4096, 4096*bnum) < 4096)
	{
		perror("Error writing to file");
      printf("   Block %u offset %u\n", bnum, 4096*bnum);
		exit(0);
	}
}

struct lookup_info
{
	char name[MAXPATHLEN+1];
	uint32_t block;
};

static void lookup_readdir_cb(void *a, const char *n, uint32_t block)
{
	struct lookup_info *args;

	args = (struct lookup_info*)a;
	if (0 == strcmp(n, args->name))
		args->block = block;
}

static int lookup_block_num(const char *str, uint32_t *num, const char **rem, uint32_t *pnum)
{
	const char *curr;
	uint32_t curr_block;
	struct lookup_info args;
	*num = 0;

	if (NULL == num || NULL == str || '/' != str[0])
		return -ENOENT;
	if (NULL == fs_ops->readdir || NULL == fs_ops->root_node)
		return -EACCES;

	curr = str + 1;
	curr_block = (*fs_ops->root_node)(fs_ops->arg);
	*num = 0;
	if (0 == curr_block)
		return -ENOENT;
	while(NULL != curr && '\0' != curr[0])
	{
		const char *last = strchr(curr, '/');
		if (NULL == last)
		{
			if (NULL != rem)
			{
				*rem = curr;
				if (pnum == NULL)
					break;
				*pnum = curr_block;
			}
			last = curr + strlen(curr);
		}

		args.name[ last - curr ] = 0;
		memcpy(args.name, curr, last - curr);
		args.block = 0;
		(*fs_ops->readdir)(fs_ops->arg, curr_block, (void*)&args, lookup_readdir_cb);

		if (0 == args.block)
			return -ENOENT;

		curr_block = args.block;
		curr = last;
		if ('/' == last[0])
			curr++;
	}
	*num = curr_block;
	return 0;
}

static int cpe453fs_getattr(const char *path, struct stat *stbuf)
{
    int res = 0;
	uint32_t bn;

	if (NULL == fs_ops->getattr)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("GETATTR %s (%u)\n", path, bn);
#endif
    memset(stbuf, 0, sizeof(struct stat));
	res = (*fs_ops->getattr)(fs_ops->arg, bn, stbuf);

    return res;
}

struct readdir_info
{
	fuse_fill_dir_t filler;
	void *arg;
};

static void readdir_cb(void *a, const char *n, uint32_t block_num)
{
	struct readdir_info *info;

	info = (struct readdir_info*)a;
#ifdef DEBUG
	printf("\t%s\n", n);
#endif

	(*info->filler)(info->arg, n, NULL, 0);
}

static int cpe453fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi)
{
    int res = 0;
	uint32_t bn;
	struct readdir_info info;

	if (NULL == fs_ops->readdir)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("READDIR %s (%u)\n", path, bn);
#endif

	info.filler = filler;
	info.arg = buf;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
	res = (*fs_ops->readdir)(fs_ops->arg, bn, (void*)&info, readdir_cb);

    return res;
}

static int cpe453fs_open(const char *path, struct fuse_file_info *fi)
{
    int res = 0;
	uint32_t bn;

	if (NULL == fs_ops->open)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("OPEN %s (%u)\n", path, bn);
#endif

	if (0 == fs_ops->write)
	{
    	if((fi->flags & 3) != O_RDONLY)
        	return -EACCES;
	}
	res = (*fs_ops->open)(fs_ops->arg, bn);

    return res;
}

static int cpe453fs_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
    int res = 0;
	uint32_t bn;

	if (NULL == fs_ops->read)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("READ %s (%u)\n", path, bn);
#endif

	res = (*fs_ops->read)(fs_ops->arg, bn, buf, size, offset);

    return res;
}

static int cpe453fs_readlink(const char* path, char *buf, size_t buf_len)
{
    int res = 0;
	uint32_t bn;

	if (NULL == fs_ops->readlink)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("READLINK %s (%u)\n", path, bn);
#endif

	res = (*fs_ops->readlink)(fs_ops->arg, bn, buf, buf_len);

    return res;
}

static int cpe453fs_statfs(const char *path, struct statvfs *stat)
{
	struct statvfs disk;
	struct stat sb;
	// int sz;

#ifdef DEBUG
	printf("STATFS %s\n", path);
#endif

	if (0 > fstatvfs(fd, &disk))
	{
		perror("Error calling fstatvfs");
		exit(0);
	}

	if (0 > fstat(fd, &sb))
	{
		perror("Error calling fstat");
		exit(0);
	}
	// sz = sb.st_size / 4096;

	stat->f_bsize = 4096;
	stat->f_frsize = 4096;
	stat->f_blocks = (disk.f_frsize * disk.f_blocks ) / 4096;
	stat->f_bfree = (disk.f_frsize * disk.f_bfree ) / 4096;
	stat->f_bavail = (disk.f_frsize * disk.f_bavail ) / 4096;
	stat->f_files = (disk.f_frsize * disk.f_blocks ) / 4096;
	stat->f_ffree = (disk.f_frsize * disk.f_bfree ) / 4096;
	stat->f_favail = (disk.f_frsize * disk.f_bavail ) / 4096;

	return 0;
}

static int cpe453fs_chmod(const char *path, mode_t new_mode)
{
	uint32_t bn;
	int res = 0;

	if (NULL == fs_ops->chmod)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("CHMOD %s (%u)\n", path, bn);
#endif

	res = (*fs_ops->chmod)(fs_ops->arg, bn, new_mode);

    return res;
}

static int cpe453fs_chown(const char *path, uid_t new_uid, gid_t new_gid)
{
	uint32_t bn;
	int res = 0;

	if (NULL == fs_ops->chown)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("CHOWN %s (%u)\n", path, bn);
#endif

	res = (*fs_ops->chown)(fs_ops->arg, bn, new_uid, new_gid);

    return res;
}

static int cpe453fs_utimens(const char *path, const struct timespec tv[2])
{
	uint32_t bn;
	int res = 0;

	if (NULL == fs_ops->utimens)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("UTIMENS %s (%u)\n", path, bn);
#endif

	res = (*fs_ops->utimens)(fs_ops->arg, bn, tv);

    return res;
}

static int cpe453fs_rmdir(const char *path)
{
	uint32_t bn;
	const char *rem = NULL;
	int res = 0;

	if (NULL == fs_ops->rmdir)
		return -EACCES;

	res = lookup_block_num(path, &bn, &rem, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("RMDIR %s (%u/%s)\n", path, bn, rem);
#endif

	res = (*fs_ops->rmdir)(fs_ops->arg, bn, rem);

    return res;
}

static int cpe453fs_unlink(const char *path)
{
	uint32_t bn;
	const char *rem = NULL;
	int res = 0;

	if (NULL == fs_ops->unlink)
		return -EACCES;

	res = lookup_block_num(path, &bn, &rem, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("UNLINK %s (%u/%s)\n", path, bn, rem);
#endif

	res = (*fs_ops->unlink)(fs_ops->arg, bn, rem);

    return res;
}

static int cpe453fs_mknod(const char *path, mode_t new_mode, dev_t new_dev)
{
	uint32_t bn, par;
	const char *rem = NULL;
	int res = 0;

	if (NULL == fs_ops->mknod)
		return -EACCES;

	res = lookup_block_num(path, &bn, &rem, &par);
	if (res == 0)
		return -EEXIST;
	if (res != -ENOENT)
		return res;
#ifdef DEBUG
	printf("MKNOD %s (%u/%s)\n", path, par, rem);
#endif

	res = (*fs_ops->mknod)(fs_ops->arg, par, rem, new_mode, new_dev);

    return res;
}

static int cpe453fs_symlink(const char *path, const char *dest)
{
	uint32_t bn, par;
	const char *rem = NULL;
	int res = 0;

	if (NULL == fs_ops->symlink)
		return -EACCES;

	res = lookup_block_num(dest, &bn, &rem, &par);
	if (res == 0)
		return -EEXIST;
	if (res != -ENOENT)
		return res;
#ifdef DEBUG
	printf("SYMLINK %s (%u/%s)\n", dest, par, rem);
#endif

	res = (*fs_ops->symlink)(fs_ops->arg, par, rem, path);

    return res;
}

static int cpe453fs_mkdir(const char *path, mode_t new_mode)
{
	uint32_t bn, par;
	const char *rem = NULL;
	int res = 0;

	if (NULL == fs_ops->mkdir)
		return -EACCES;

	res = lookup_block_num(path, &bn, &rem, &par);
	if (res == 0)
		return -EEXIST;
	if (res != -ENOENT)
		return res;
#ifdef DEBUG
	printf("MKDIR %s (%u/%s)\n", path, par, rem);
#endif

	res = (*fs_ops->mkdir)(fs_ops->arg, par, rem, new_mode);

    return res;
}

static int cpe453fs_link(const char *from, const char *to)
{
	uint32_t bn, to_bn, to_par;
	const char *rem = NULL;
	int res = 0;

	if (NULL == fs_ops->link)
		return -EACCES;

	res = lookup_block_num(to, &to_bn, &rem, &to_par);
	if (res == 0)
		return -EEXIST;
	if (res != -ENOENT)
		return res;
	res = lookup_block_num(from, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("LINK %s->%s (%u)->(%u/%s)\n", from, to, bn, to_par, rem);
#endif

	res = (*fs_ops->link)(fs_ops->arg, to_par, rem, bn);

    return res;
}

static int cpe453fs_rename(const char *from, const char *to)
{
	uint32_t from_bn, to_bn, to_par;
	const char *to_rem = NULL, *from_rem = NULL;
	int res = 0;

	if (NULL == fs_ops->rename)
		return -EACCES;
	if (NULL == fs_ops->unlink)
		return -EACCES;

	res = lookup_block_num(to, &to_bn, &to_rem, &to_par);
	if (res == 0) {
	   res = (*fs_ops->unlink)(fs_ops->arg, to_par, to_rem);
      if (res < 0)
         return res;
   }
   else
      if (res != -ENOENT)
		   return res;
	res = lookup_block_num(from, &from_bn, &from_rem, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("RENAME %s->%s (%u/%s)->(%u/%s)\n", from, to, from_bn, from_rem, to_par, to_rem);
#endif

	res = (*fs_ops->rename)(fs_ops->arg, from_bn, from_rem, to_par, to_rem);

    return res;
}

static int cpe453fs_truncate(const char *path, off_t size)
{
    int res = 0;
	uint32_t bn;

	if (NULL == fs_ops->truncate)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("TRUNCATE %s (%u)\n", path, bn);
#endif

	res = (*fs_ops->truncate)(fs_ops->arg, bn, size);

    return res;
}

static int cpe453fs_write(const char *path, const char *buff, size_t buf_len,
off_t offset, struct fuse_file_info *unused)
{
    int res = 0;
	uint32_t bn;

	if (NULL == fs_ops->write)
		return -EACCES;

	res = lookup_block_num(path, &bn, NULL, NULL);
	if (res < 0)
		return res;
#ifdef DEBUG
	printf("WRITE %s (%u)\n", path, bn);
#endif

	res = (*fs_ops->write)(fs_ops->arg, bn, buff, buf_len, offset);

    return res;
}

static void *cpe453fs_init(struct fuse_conn_info *conn)
{
	if (fs_ops->init)
		(*fs_ops->init)();
	return NULL;
}

static void cpe453fs_destroy(void *unused)
{
	if (fs_ops->destroy)
		(*fs_ops->destroy)();
}

static void init_ops(struct fuse_operations *ops)
{
	memset(ops, 0, sizeof(*ops));
	if (NULL != fs_ops->getattr)
		ops->getattr	= cpe453fs_getattr;
	if (NULL != fs_ops->readdir)
		ops->readdir	= cpe453fs_readdir;
	if (NULL != fs_ops->open)
		ops->open		= cpe453fs_open;
	if (NULL != fs_ops->read)
		ops->read		= cpe453fs_read;
	if (NULL != fs_ops->readlink)
		ops->readlink 	= cpe453fs_readlink;
	ops->statfs		= cpe453fs_statfs;
	if (NULL != fs_ops->chmod)
		ops->chmod		= cpe453fs_chmod;
	if (NULL != fs_ops->chown)
		ops->chown		= cpe453fs_chown;
	if (NULL != fs_ops->utimens)
		ops->utimens		= cpe453fs_utimens;
	if (NULL != fs_ops->rmdir)
		ops->rmdir		= cpe453fs_rmdir;
	if (NULL != fs_ops->unlink)
		ops->unlink		= cpe453fs_unlink;
	if (NULL != fs_ops->mknod)
		ops->mknod		= cpe453fs_mknod;
	if (NULL != fs_ops->symlink)
		ops->symlink	= cpe453fs_symlink;
	if (NULL != fs_ops->mkdir)
		ops->mkdir		= cpe453fs_mkdir;
	if (NULL != fs_ops->link)
		ops->link		= cpe453fs_link;
	if (NULL != fs_ops->rename && NULL != fs_ops->unlink)
		ops->rename		= cpe453fs_rename;
	if (NULL != fs_ops->truncate)
		ops->truncate	= cpe453fs_truncate;
	if (NULL != fs_ops->write)
		ops->write	= cpe453fs_write;
	ops->init = cpe453fs_init;
	ops->destroy = cpe453fs_destroy;
}

int main(int argc, char *argv[])
{
	int res;
	struct fuse_operations cpe453fs_ops;

	fs_ops = CPE453_get_operations();

	init_ops(&cpe453fs_ops);

	if (argc < 2)
	{
		fprintf(stderr, "Usage: %s [fuse options] <FS File>\n", argv[0]);
		exit(1);
	}

	fd = open(argv[argc-1], O_RDWR
#ifdef LINUX
//		| O_DIRECT
//		| 040000
#endif
	);

	if (fd < 0)
	{
		perror("Error opening filesystem file");
		exit(1);
	}

   printf("Using fd %d\n", fd);

#ifdef MACOSX
	// fcntl(fd, F_NOCACHE, 1);
#endif

	if (NULL != fs_ops->set_file_descriptor)
		(*fs_ops->set_file_descriptor)(fs_ops->arg, fd);

    res = fuse_main(argc - 1, argv, &cpe453fs_ops, NULL);

	close(fd);
	return res;
}
