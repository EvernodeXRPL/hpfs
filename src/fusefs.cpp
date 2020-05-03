/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPLv2.
  See the file COPYING.
*/

/** @file
 *
 * This file system mirrors the existing file system hierarchy of the
 * system, starting at the root file system. This is implemented by
 * just "passing through" all requests to the corresponding user-space
 * libc functions. This implementation is a little more sophisticated
 * than the one in passthrough.c, so performance is not quite as bad.
 *
 * Compile with:
 *
 *     gcc -Wall passthrough_fh.c `pkg-config fuse3 --cflags --libs` -lulockmgr -o passthrough_fh
 *
 * ## Source code ##
 * \include passthrough_fh.c
 */

#define FUSE_USE_VERSION 31

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <fuse3/fuse.h>

#ifdef HAVE_LIBULOCKMGR
#include <ulockmgr.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <sys/file.h> /* flock(2) */

#include <iostream>
#include <string>
#include "hpfs.hpp"
#include "vfs.hpp"

namespace fusefs
{

void *xmp_init(struct fuse_conn_info *conn,
			   struct fuse_config *cfg)
{
	(void)conn;
	cfg->use_ino = 1;
	cfg->nullpath_ok = 0;

	/* Pick up changes from lower filesystem right away. This is
	   also necessary for better hardlink support. When the kernel
	   calls the unlink() handler, it does not know the inode of
	   the to-be-removed entry and can therefore not invalidate
	   the cache of the associated inode - resulting in an
	   incorrect st_nlink value being reported for any remaining
	   hardlinks to this inode. */
	cfg->entry_timeout = 0;
	cfg->attr_timeout = 0;
	cfg->negative_timeout = 0;

	return NULL;
}

int xmp_getattr(const char *path, struct stat *stbuf,
				struct fuse_file_info *fi)
{
	(void)fi;
	return vfs::getattr(path, stbuf);
}

int xmp_access(const char *path, int mask)
{
	return 0;
}

int xmp_readlink(const char *path, char *buf, size_t size)
{
	return 0;
}

int xmp_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
				off_t offset, struct fuse_file_info *fi,
				enum fuse_readdir_flags flags)
{
	return 0;
}

int xmp_mkdir(const char *path, mode_t mode)
{
	return vfs::create(path, mode);
}

int xmp_rmdir(const char *path)
{
	return 0;
}

int xmp_symlink(const char *from, const char *to)
{
	return 0;
}

int xmp_rename(const char *from, const char *to, unsigned int flags)
{
	return 0;
}

int xmp_link(const char *from, const char *to)
{
	return 0;
}

int xmp_unlink(const char *path)
{
	return 0;
}

int xmp_chmod(const char *path, mode_t mode,
			  struct fuse_file_info *fi)
{
	return 0;
}

int xmp_chown(const char *path, uid_t uid, gid_t gid,
			  struct fuse_file_info *fi)
{
	return 0;
}

#ifdef HAVE_UTIMENSAT
int xmp_utimens(const char *path, const struct timespec ts[2],
				struct fuse_file_info *fi)
{
	return 0;
}
#endif

int xmp_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	return vfs::create(path, mode);
}

int xmp_open(const char *path, struct fuse_file_info *fi)
{
	return 0;
}

int xmp_read(const char *path, char *buf, size_t size, off_t offset,
			 struct fuse_file_info *fi)
{
	return vfs::read(path, buf, size, offset);
}

int xmp_write(const char *path, const char *buf, size_t size,
			  off_t offset, struct fuse_file_info *fi)
{
	std::cout << path << "\n";
	return vfs::write(path, buf, size, offset);
}

int xmp_statfs(const char *path, struct statvfs *stbuf)
{
	return 0;
}

int xmp_release(const char *path, struct fuse_file_info *fi)
{
	(void)path;
	close(fi->fh);
	return 0;
}

int xmp_truncate(const char *path, off_t size,
				 struct fuse_file_info *fi)
{
	return 0;
}

int xmp_fsync(const char *path, int isdatasync,
			  struct fuse_file_info *fi)
{
	int res;
	(void)path;

#ifndef HAVE_FDATASYNC
	(void)isdatasync;
#else
	if (isdatasync)
		res = fdatasync(fi->fh);
	else
#endif
	res = fsync(fi->fh);
	if (res == -1)
		return -errno;

	return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
int xmp_fallocate(const char *path, int mode,
				  off_t offset, off_t length, struct fuse_file_info *fi)
{
	return 0;
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
int xmp_setxattr(const char *path, const char *name, const char *value,
				 size_t size, int flags)
{
	return 0;
}

int xmp_getxattr(const char *path, const char *name, char *value,
				 size_t size)
{
	return 0;
}

int xmp_listxattr(const char *path, char *list, size_t size)
{
	return 0;
}

int xmp_removexattr(const char *path, const char *name)
{
	return 0;
}
#endif /* HAVE_SETXATTR */

#ifdef HAVE_LIBULOCKMGR
int xmp_lock(const char *path, struct fuse_file_info *fi, int cmd,
			 struct flock *lock)
{
	return 0;
}
#endif

int xmp_flock(const char *path, struct fuse_file_info *fi, int op)
{
	return 0;
}

#ifdef HAVE_COPY_FILE_RANGE
ssize_t xmp_copy_file_range(const char *path_in,
							struct fuse_file_info *fi_in,
							off_t off_in, const char *path_out,
							struct fuse_file_info *fi_out,
							off_t off_out, size_t len, int flags)
{
	return 0;
}
#endif

off_t xmp_lseek(const char *path, off_t off, int whence, struct fuse_file_info *fi)
{
	return 0;
}

void assign_operations(fuse_operations &xmp_oper)
{
	xmp_oper.getattr = xmp_getattr;
	xmp_oper.readlink = xmp_readlink;
	// xmp_oper.mknod = xmp_mknod;
	xmp_oper.mkdir = xmp_mkdir;
	xmp_oper.unlink = xmp_unlink;
	xmp_oper.rmdir = xmp_rmdir;
	xmp_oper.symlink = xmp_symlink;
	xmp_oper.rename = xmp_rename;
	xmp_oper.link = xmp_link;
	xmp_oper.chmod = xmp_chmod;
	xmp_oper.chown = xmp_chown;
	xmp_oper.truncate = xmp_truncate;
	xmp_oper.open = xmp_open;
	xmp_oper.read = xmp_read;
	xmp_oper.write = xmp_write;
	xmp_oper.statfs = xmp_statfs;
	//xmp_oper.flush = xmp_flush;
	xmp_oper.release = xmp_release;
	xmp_oper.fsync = xmp_fsync;
#ifdef HAVE_SETXATTR
	xmp_oper.setxattr = xmp_setxattr;
	xmp_oper.getxattr = xmp_getxattr;
	xmp_oper.listxattr = xmp_listxattr;
	xmp_oper.removexattr = xmp_removexattr;
#endif
	//xmp_oper.opendir = xmp_opendir;
	xmp_oper.readdir = xmp_readdir;
	//xmp_oper.releasedir = xmp_releasedir;
	//xmp_oper.fsyncdir = NULL;
	xmp_oper.init = xmp_init;
	//xmp_oper.destroy = NULL;
	xmp_oper.access = xmp_access;
	xmp_oper.create = xmp_create;
#ifdef HAVE_LIBULOCKMGR
	xmp_oper.lock = xmp_lock;
#endif
#ifdef HAVE_UTIMENSAT
	xmp_oper.utimens = xmp_utimens;
#endif
	//xmp_oper.bmap = NULL;
	//xmp_oper.ioctl = NULL;
	//xmp_oper.poll = NULL;
	//xmp_oper.write_buf = xmp_write_buf;
	//xmp_oper.read_buf = xmp_read_buf;
	xmp_oper.flock = xmp_flock;
#ifdef HAVE_POSIX_FALLOCATE
	xmp_oper.fallocate = xmp_fallocate;
#endif
#ifdef HAVE_COPY_FILE_RANGE
	xmp_oper.copy_file_range = xmp_copy_file_range;
#endif
	xmp_oper.lseek = xmp_lseek;
}

fuse_operations xmp_oper;

int init(char *arg0)
{
	fuse_args args = FUSE_ARGS_INIT(0, NULL);
	fuse_opt_add_arg(&args, arg0); // Mount dir
	fuse_opt_add_arg(&args, hpfs::ctx.mount_dir.c_str()); // Mount dir
	fuse_opt_add_arg(&args, "-f");						  // Foreground
	fuse_opt_add_arg(&args, "-s");						  // Single threaded
	fuse_opt_add_arg(&args, "-ofsname=hpfs");
	fuse_opt_add_arg(&args, "-odefault_permissions");
	// fuse_opt_add_arg(&args, "-d"); // Debug

	umask(0);
	assign_operations(xmp_oper);
	return fuse_main(args.argc, args.argv, &xmp_oper, NULL);
}

} // namespace fusefs