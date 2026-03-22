#include <stddef.h>
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

#include "cpe453fs.h"

#define BLOCKSIZE 4096

#define TYPE_SUPERBLOCK 1
#define TYPE_INODE 2
#define TYPE_DIR_EXTENT 3
#define TYPE_FILE_EXTENT 4
#define TYPE_FREE 5

#define MAX_DIRNAME_LENGTH 1023
#define DIRNAME_BUFFER_SIZE (MAX_DIRNAME_LENGTH + 1)

#pragma pack(push, 1)
struct Superblock {
	uint32_t type;
	uint32_t magic[1024 - 3];
	uint32_t root_inode;
	uint32_t free_list_head;
};
#pragma pack(pop)

_Static_assert(sizeof(struct Superblock) == BLOCKSIZE, "");

#pragma pack(push, 1)
struct Inode {
	uint32_t type;
	uint16_t mode;
	uint16_t nlink;
	uint32_t uid;
	uint32_t gid;
	uint32_t rdev;
	uint32_t user_flags;
	uint32_t access_time_secs;
	uint32_t access_time_nsecs;
	uint32_t modification_time_secs;
	uint32_t modification_time_nsecs;
	uint32_t status_time_secs;
	uint32_t status_time_nsecs;
	uint64_t actual_size;
	uint64_t n_allocated_blocks;
	uint8_t contents[BLOCKSIZE - 17 * sizeof(uint32_t)];
	uint32_t next_extents_block;
};
#pragma pack(pop)

_Static_assert(sizeof(struct Inode) == BLOCKSIZE, "");

#pragma pack(push, 1)
struct FileExtents {
	uint32_t type;
	uint32_t inode;
	uint8_t contents[BLOCKSIZE - 3 * sizeof(uint32_t)];
	uint32_t next_extents_block;
};
#pragma pack(pop)

_Static_assert(sizeof(struct FileExtents) == BLOCKSIZE, "");

#pragma pack(push, 1)
struct DirectoryExtents {
	uint32_t type;
	uint8_t contents[BLOCKSIZE - 2 * sizeof(uint32_t)];
	uint32_t next_extents_block;
};
#pragma pack(pop)

_Static_assert(sizeof(struct DirectoryExtents) == BLOCKSIZE, "");

#pragma pack(push, 1)
struct DirectoryEntryHeader {
	uint16_t length;
	uint32_t inode;
};
#pragma pack(pop)

#pragma pack(push, 1)
struct EmptyBlock {
	uint32_t type;
	uint32_t next_empty_block;
	uint32_t padding[1024 - 2];
};
#pragma pack(pop)
_Static_assert(sizeof(struct EmptyBlock) == BLOCKSIZE, "");

// union Block {
//         struct Superblock superblock;
//         struct Inode inode;
//         struct Extents extents;
// };
//
//_Static_assert(sizeof(union Block) == BLOCKSIZE, "");

struct Args {
	int fd;
};

static void set_file_descriptor(void *args, int fd)
{
	struct Args *fs = (struct Args *)args;
	fs->fd = fd;
}

static int read_block_struct(void *block, int fd, uint32_t block_num)
{
	readblock(fd, (unsigned char *)block, block_num);

	uint32_t type;
	memcpy(&type, block, sizeof(type));

	if (type < 1 || type > 5)
		return -EINVAL;

	return 0;
}

static int mygetattr(void *args, uint32_t block_num, struct stat *stbuf)
{
	struct Args *fs = (struct Args *)args;

	struct Inode inode;
	int result = read_block_struct(&inode, fs->fd, block_num);

	if (result)
		return result;
	if (inode.type != TYPE_INODE)
		return -EINVAL;

	stbuf->st_mode = inode.mode | 0x0755;
	stbuf->st_nlink = inode.nlink;
	stbuf->st_uid = inode.uid;
	stbuf->st_gid = inode.gid;
	stbuf->st_rdev = inode.rdev;
	stbuf->st_size = inode.actual_size;
	stbuf->st_atime = inode.access_time_secs;
	stbuf->st_mtime = inode.modification_time_secs;
	stbuf->st_ctime = inode.status_time_secs;
	stbuf->st_blksize = BLOCKSIZE;
	stbuf->st_blocks = inode.n_allocated_blocks;

	return 0;
}

static int read_directory_entry(const uint8_t *data, char *name_buffer, uint32_t *entry_inode, size_t *bytes_read)
{
	struct DirectoryEntryHeader entry_header;
	memcpy(&entry_header, data, sizeof(entry_header));

	if (entry_header.length == 0) {
		*bytes_read = 0;
		return 0;
	}

	size_t header_offset = sizeof(entry_header);

	if (entry_header.length < sizeof(struct DirectoryEntryHeader) + 1)
		return -EINVAL;
	if (entry_header.length > sizeof(struct DirectoryEntryHeader) + MAX_DIRNAME_LENGTH)
		return -EINVAL;

	size_t name_length = entry_header.length - header_offset;
	memcpy(name_buffer, data + header_offset, name_length);
	name_buffer[name_length] = '\0';

	*bytes_read = entry_header.length;
	*entry_inode = entry_header.inode;
	return 0;
}

static int callback_directory_entry(const uint8_t *entry, void *buf, CPE453_readdir_callback_t cb, size_t *bytes_read)
{
	char name_buffer[DIRNAME_BUFFER_SIZE];
	uint32_t entry_inode;
	int result;

	result = read_directory_entry(entry, name_buffer, &entry_inode, bytes_read);
	if (result)
		return result;

	if (*bytes_read == 0)
		return 0;

	cb(buf, name_buffer, entry_inode);

	return 0;
}

struct DataRegion {
	uint8_t *data;
	size_t size;
};

static int myreaddir(void *args, uint32_t block_num, void *buf, CPE453_readdir_callback_t cb)
{
	struct Args *fs = (struct Args *)args;
	struct Inode inode;
	int result;

	result = read_block_struct(&inode, fs->fd, block_num);
	if (result)
		return result;
	if (inode.type != TYPE_INODE)
		return -EINVAL;
	if ((inode.mode & S_IFMT) != S_IFDIR)
		return -EINVAL;

	struct DataRegion region;
	struct DirectoryExtents ext;

	region.data = inode.contents;
	region.size = sizeof(inode.contents);

	size_t bytes_read_global = 0;
	size_t i_region = 0;
	uint32_t next_ext_num = inode.next_extents_block;
	while (bytes_read_global < inode.actual_size) {
		if (i_region >= region.size) {
			if (!next_ext_num)
				break;

			result = read_block_struct(&ext, fs->fd, next_ext_num);
			if (result)
				return result;
			if (ext.type != TYPE_DIR_EXTENT)
				return -EINVAL;

			next_ext_num = ext.next_extents_block;
			region.data = ext.contents;
			region.size = sizeof(ext.contents);

			i_region = 0;
		}

		if (region.size - i_region < sizeof(struct DirectoryEntryHeader) + 1) {
			i_region = region.size;
			continue;
		}

                struct DirectoryEntryHeader entry;
                memcpy(&entry, region.data + i_region, sizeof(entry));
                if (entry.inode == 0) {
                        i_region += entry.length;
                        continue;
                }

		size_t bytes_read = 0;
		result = callback_directory_entry(&region.data[i_region], buf, cb, &bytes_read);
		if (result)
			return result;

		if (bytes_read == 0) {
			// no more entries in this block
			i_region = region.size;
			continue;
		}

		if (bytes_read_global + bytes_read > inode.actual_size)
			return -EINVAL;

		i_region += bytes_read;
		bytes_read_global += bytes_read;
	}
	return 0;
}

static int myopen(void *args, uint32_t block_num)
{
	struct Args *fs = (struct Args *)args;
	struct Inode inode;
	int result;
	result = read_block_struct(&inode, fs->fd, block_num);
	if (result)
		return result;
	if (inode.type != TYPE_INODE)
		return -EINVAL;
	if ((inode.mode & S_IFMT) != S_IFREG)
		return -EINVAL;

	return 0;
}

static int read_file_inode(struct Inode *inode, int fd, uint32_t block_num)
{
	int result;
	result = read_block_struct(inode, fd, block_num);

	if (result)
		return result;
	if (inode->type != TYPE_INODE)
		return -EINVAL;
	if ((inode->mode & S_IFMT) != S_IFREG)
		return -EINVAL;

	return 0;
}

static int myread(void *args, uint32_t block_num, char *buf, size_t size, off_t offset)
{
	struct Args *fs = (struct Args *)args;
	int result;

	struct Inode inode;
	result = read_file_inode(&inode, fs->fd, block_num);

	if (result)
		return result;

	if (offset > inode.actual_size)
		return -EINVAL;

	struct DataRegion region;
	size_t i_region = offset;
	uint32_t next_ext_num = inode.next_extents_block;

	if (offset < sizeof(inode.contents)) {
		region.data = inode.contents;
		region.size = sizeof(inode.contents);
		i_region = offset;
	} else {
		i_region -= sizeof(inode.contents);

		struct FileExtents ext;
		result = read_block_struct(&ext, fs->fd, next_ext_num);
		if (result)
			return result;

		while (i_region >= sizeof(ext.contents)) {
			i_region -= sizeof(ext.contents);

			next_ext_num = ext.next_extents_block;
			result = read_block_struct(&ext, fs->fd, next_ext_num);
			if (result)
				return result;
		}

		region.data = ext.contents;
		region.size = sizeof(ext.contents);
		next_ext_num = ext.next_extents_block;
	}

	size_t i_buffer = 0;
	while (offset < inode.actual_size && i_buffer < size) {
		if (i_region >= region.size) {
			struct FileExtents ext;
			result = read_block_struct(&ext, fs->fd, next_ext_num);
			if (result)
				return result;

			next_ext_num = ext.next_extents_block;
			region.data = ext.contents;
			region.size = sizeof(ext.contents);

			i_region = 0;
		}
		buf[i_buffer] = region.data[i_region];

		i_buffer++;
		i_region++;
		offset++;
	}

	return i_buffer;
}

static int myreadlink(void *args, uint32_t block_num, char *buf, size_t size)
{
	struct Args *fs = (struct Args *)args;
	int result;

	struct Inode inode;
	result = read_block_struct(&inode, fs->fd, block_num);
	if (result)
		return result;
	if (inode.type != TYPE_INODE)
		return -EINVAL;
	if ((inode.mode & S_IFMT) != S_IFLNK)
		return -EINVAL;

	if (size == 0)
		return 0;

	size_t copy_len = inode.actual_size;
	if (copy_len > size - 1)
		copy_len = size - 1;

	memcpy(buf, inode.contents, copy_len);
	buf[copy_len] = '\0';

	return 0;
}

static uint32_t root_node(void *args)
{
	struct Args *fs = (struct Args *)args;
	struct Superblock sb;
	int result;
	result = read_block_struct(&sb, fs->fd, 0);
	if (result)
		return -result;

	if (sb.type != TYPE_SUPERBLOCK)
		return -EINVAL;

	return sb.root_inode;
}

static int read_superblock(struct Superblock *sb, void *args)
{
	struct Args *fs = (struct Args *)args;
	int result;
	result = read_block_struct(sb, fs->fd, 1);
	if (result)
		return result;
	if (sb->type != TYPE_SUPERBLOCK)
		return -EINVAL;

	return 0;
}

static int read_empty_block(struct EmptyBlock *b, void *args, uint32_t block_num)
{
	struct Args *fs = (struct Args *)args;
	int result;
	result = read_block_struct(b, fs->fd, block_num);
	if (result)
		return result;
	if (b->type != TYPE_FREE)
		return -EINVAL;

	return 0;
}

static int grow_image(void *args)
{
	struct Args *fs = (struct Args *)args;
	off_t current_size = lseek(fs->fd, 0, SEEK_END);
	if (current_size == -1)
		return -errno;

	int result = ftruncate(fs->fd, current_size + BLOCKSIZE);
	if (result == -1)
		return -errno;
	return 0;
}

static int write_superblock(void *args, const struct Superblock *sb)
{
	struct Args *fs = (struct Args *)args;
	writeblock(fs->fd, (unsigned char *)sb, 1);
	return 0;
}

static int allocate_block(void *args, uint32_t *block_num_out)
{
	struct Args *fs = (struct Args *)args;

	struct Superblock sb;
	int result;
	result = read_superblock(&sb, args);
	if (result)
		return result;

	if (sb.free_list_head == 0) {
		off_t old_size = lseek(fs->fd, 0, SEEK_END);
		if (old_size == -1)
			return -errno;

		result = grow_image(args);
		if (result)
			return result;

		*block_num_out = old_size / BLOCKSIZE;
		return 0;
	}

	uint32_t old_free_block = sb.free_list_head;

	struct EmptyBlock empty_block;
	result = read_empty_block(&empty_block, args, sb.free_list_head);
	if (result)
		return result;

	sb.free_list_head = empty_block.next_empty_block;

	// write updated superblock
	result = write_superblock(args, &sb);
	if (result)
		return result;

	*block_num_out = old_free_block;
	return 0;
}

static int free_block(void *args, uint32_t block_num)
{
	struct Args *fs = (struct Args *)args;

	struct Superblock sb;
	int result;
	result = read_superblock(&sb, args);
	if (result)
		return result;

	struct EmptyBlock empty_block;
	empty_block.type = TYPE_FREE;
	empty_block.next_empty_block = sb.free_list_head;

	writeblock(fs->fd, (unsigned char *)&empty_block, block_num);

	sb.free_list_head = block_num;

	result = write_superblock(args, &sb);
	if (result)
		return result;

	return 0;
}

static int read_directory_inode(struct Inode *inode, int fd, uint32_t block_num)
{
	int result;
	result = read_block_struct(inode, fd, block_num);

	if (result)
		return result;
	if (inode->type != TYPE_INODE)
		return -EINVAL;
	if ((inode->mode & S_IFMT) != S_IFDIR)
		return -EINVAL;

	return 0;
}

static int remove_dir_entry(void *args, uint32_t dir_inode_num, const char *name)
{
	struct Args *fs = (struct Args *)args;

	struct Inode dir_inode;
	int result;
	result = read_directory_inode(&dir_inode, fs->fd, dir_inode_num);
	if (result)
		return result;

	size_t global_bytes_read = 0;
	uint32_t curr_block_num = dir_inode_num;
	uint32_t next_block_num = dir_inode.next_extents_block;
	size_t i_region = 0;

	struct DataRegion region;
	struct DirectoryExtents ext;

	region.data = dir_inode.contents;
	region.size = sizeof(dir_inode.contents);

	while (global_bytes_read < dir_inode.actual_size) {
		if (i_region >= region.size) {
			if (!next_block_num)
				return -ENOENT;

			result = read_block_struct(&ext, fs->fd, next_block_num);
			if (result)
				return result;
			if (ext.type != TYPE_DIR_EXTENT)
				return -EINVAL;

			curr_block_num = next_block_num;
			next_block_num = ext.next_extents_block;

			region.data = ext.contents;
			region.size = sizeof(ext.contents);

			i_region = 0;
		}

		if (region.size - i_region < sizeof(struct DirectoryEntryHeader) + 1) {
			i_region = region.size;
			continue;
		}

		struct DirectoryEntryHeader entry;
		memcpy(&entry, region.data + i_region, sizeof(entry));

		if (entry.length == 0) {
			i_region = region.size;
			continue;
		}

		if (entry.length < sizeof(entry) + 1)
			return -EINVAL;

		char name_buffer[DIRNAME_BUFFER_SIZE];
		size_t name_offset = i_region + sizeof(entry);
		size_t name_length = entry.length - sizeof(entry);

		if (name_offset + name_length > region.size)
			return -EINVAL;
		if (name_length > MAX_DIRNAME_LENGTH)
			return -EINVAL;
		if (name_length < 1)
			return -EINVAL;

		if (entry.inode == 0) {
			i_region += entry.length;
			global_bytes_read += entry.length;
			continue;
		}

		memcpy(name_buffer, region.data + name_offset, name_length);
		name_buffer[name_length] = '\0';
		if (strcmp(name_buffer, name) == 0) {
			//struct Inode curr_block; // could be an extents
			//result = read_block_struct(&curr_block, fs->fd, curr_block_num);
			//if (result)
			//	return result;
			//if (curr_block.type == TYPE_INODE && !(S_ISDIR(curr_block.mode)))
			//	return -EINVAL;
			//if (curr_block.type != TYPE_INODE && curr_block.type != TYPE_DIR_EXTENT)
			//	return -EINVAL;

                        dir_inode.actual_size -= entry.length;

			entry.inode = 0;
			memcpy(region.data + i_region, &entry, sizeof(entry));

			if (curr_block_num == dir_inode_num)
				writeblock(fs->fd, (unsigned char *)&dir_inode, dir_inode_num);
			else
				writeblock(fs->fd, (unsigned char *)&ext, curr_block_num);

			return 0;
		}

		i_region += entry.length;
		global_bytes_read += entry.length;
		//
	}

	(void)curr_block_num;
	return -ENOENT;
}

#ifdef __cplusplus
extern "C" {
#endif

struct cpe453fs_ops *CPE453_get_operations(void)
{
        (void)allocate_block;
        (void)free_block;

	static struct cpe453fs_ops ops;
	static struct Args args;
	memset(&ops, 0, sizeof(ops));
	ops.arg = &args;

	ops.getattr = mygetattr;
	ops.readdir = myreaddir;
	ops.open = myopen;
	ops.read = myread;
	ops.readlink = myreadlink;
	ops.root_node = root_node;
	ops.set_file_descriptor = set_file_descriptor;
        ops.rmdir = remove_dir_entry;

	return &ops;
}

#ifdef __cplusplus
}
#endif
