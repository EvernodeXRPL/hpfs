/**
 * Code adopted from libfuse examples.
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
#include "session.hpp"
#include "inodes.hpp"
#include "vfs/vfs.hpp"
#include "vfs/fuse_adapter.hpp"
#include "hmap/query.hpp"
#include "audit/logger_index.hpp"

/**
 * Sets the 'session' local variable if session exists. Otherwise returns error code.
 */
#define CHECK_SESSION(sess_name)                         \
    SESSION_READ_LOCK                                    \
    session::fs_session *sess = session::get(sess_name); \
    if (!sess)                                           \
        return -ENOENT;

namespace hpfs::fusefs
{
    void *fs_init(struct fuse_conn_info *conn,
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

    int fs_getattr(const char *full_path, struct stat *stbuf, struct fuse_file_info *fi)
    {
        (void)fi;

        // Treat root path as success so we will return dummy stat for root.
        // Fuse host will fail if we return error code for root.
        if (strcmp(full_path, "/") == 0)
        {
            *stbuf = ctx.default_stat;
            stbuf->st_ino = inodes::ROOT_INO;
            stbuf->st_mode |= S_IFDIR;
            return 0;
        }

        SESSION_READ_LOCK

        // 0 = Successfuly interpreted as a session control request.
        // 1 = Request should be handled by the virtual fs.
        // <0 = Error code needs to be returned.
        const int sess_check_result = session::session_check_getattr(full_path, stbuf);
        if (sess_check_result < 1)
            return sess_check_result;

        const auto &[sess_name, res_path] = session::split_path(full_path);
        session::fs_session *sess = session::get(sess_name);
        if (!sess)
            return -ENOENT;

        if (sess->hmap_query)
        {
            // Check whether this is a hash map query path.
            const hmap::query::hmap_query &hmap_query = sess->hmap_query.value();
            const hmap::query::request req = hmap_query.parse_request_path(res_path.data());
            if (req.mode != hmap::query::MODE::UNDEFINED)
                return hmap_query.getattr(req, stbuf);
        }

        return sess->fuse_adapter->getattr(res_path, stbuf);
    }

    int fs_access(const char *full_path, int mask)
    {
        return 0;
    }

    int fs_readlink(const char *full_path, char *buf, size_t size)
    {
        return 0;
    }

    int fs_readdir(const char *full_path, void *buf, fuse_fill_dir_t filler,
                   off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags)
    {
        if (strcmp(full_path, "/") == 0)
        {
            // Return listing of all sessions as child directories.
            SESSION_READ_LOCK
            for (const auto &[ino, sess_name] : session::get_sessions())
            {
                struct stat st;
                st = ctx.default_stat;
                st.st_ino = ino;
                st.st_mode |= S_IFDIR;
                filler(buf, sess_name.c_str(), &st, 0, (fuse_fill_dir_flags)0);
            }
        }
        else
        {
            const auto &[sess_name, res_path] = session::split_path(full_path);
            CHECK_SESSION(sess_name);

            vfs::vdir_children_map children;
            int res = sess->fuse_adapter->readdir(res_path, children);
            if (res < 0)
                return res;

            for (const auto &[child_name, stat] : children)
                filler(buf, child_name.c_str(), &stat, 0, (fuse_fill_dir_flags)0);
        }

        return 0;
    }

    int fs_mkdir(const char *full_path, mode_t mode)
    {
        const auto &[sess_name, res_path] = session::split_path(full_path);
        CHECK_SESSION(sess_name);
        return sess->fuse_adapter->mkdir(res_path, mode);
    }

    int fs_rmdir(const char *full_path)
    {
        const auto &[sess_name, res_path] = session::split_path(full_path);
        CHECK_SESSION(sess_name);
        return sess->fuse_adapter->rmdir(res_path);
    }

    int fs_symlink(const char *from, const char *to)
    {
        return 0;
    }

    int fs_rename(const char *from, const char *to, unsigned int flags)
    {
        if (flags)
            return -EINVAL;

        const auto &[from_sess_name, from_res_path] = session::split_path(from);
        const auto &[to_sess_name, to_res_path] = session::split_path(to);

        if (from_sess_name != to_sess_name)
            return -EINVAL;

        CHECK_SESSION(from_sess_name);

        return sess->fuse_adapter->rename(from_res_path, to_res_path);
    }

    int fs_link(const char *from, const char *to)
    {
        return 0;
    }

    int fs_unlink(const char *full_path)
    {
        // 0 = Successfuly interpreted as a session control request.
        // 1 = Request should be handled by the virtual fs.
        // <0 = Error code needs to be returned.
        const int sess_check_result = session::session_check_unlink(full_path);
        if (sess_check_result < 1)
            return sess_check_result;

        const auto &[sess_name, res_path] = session::split_path(full_path);
        {
            SESSION_READ_LOCK
            session::fs_session *sess = session::get(sess_name);
            if (!sess)
                return -ENOENT;

            return sess->fuse_adapter->unlink(res_path);
        }
    }

    int fs_chmod(const char *full_path, mode_t mode,
                 struct fuse_file_info *fi)
    {
        const auto &[sess_name, res_path] = session::split_path(full_path);
        CHECK_SESSION(sess_name);
        return sess->fuse_adapter->chmod(res_path, mode);
    }

    int fs_chown(const char *full_path, uid_t uid, gid_t gid,
                 struct fuse_file_info *fi)
    {
        return 0;
    }

    int fs_utimens(const char *full_path, const struct timespec ts[2],
                   struct fuse_file_info *fi)
    {
        return 0;
    }

    int fs_create(const char *full_path, mode_t mode, struct fuse_file_info *fi)
    {
        // 0 = Successfuly interpreted as a session control request.
        // 1 = Request should be handled by the virtual fs.
        // <0 = Error code needs to be returned.
        const int sess_check_result = session::session_check_create(full_path);
        if (sess_check_result < 1)
            return sess_check_result;

        // 0 = Successfuly interpreted as a log index control request.
        // 1 = Request should be handled by the virtual fs.
        // <0 = Error code needs to be returned.
        const int index_check_result = audit::logger_index::handle_log_index_control(full_path);
        if (index_check_result < 1)
        {
            if (index_check_result == 0)
            {
                hmap::hasher::h32 hash;
                audit::logger_index::read_last_root_hash(hash);
                std::cout << hash << " " << std::to_string(audit::logger_index::get_last_seq_no()) << "\n";
            }
            return index_check_result;
        }
        
        const auto &[sess_name, res_path] = session::split_path(full_path);
        {
            SESSION_READ_LOCK
            session::fs_session *sess = session::get(sess_name);
            if (!sess)
                return -ENOENT;

            return sess->fuse_adapter->create(res_path, mode);
        }
    }

    int fs_open(const char *full_path, struct fuse_file_info *fi)
    {
        const auto &[sess_name, res_path] = session::split_path(full_path);
        CHECK_SESSION(sess_name);

        if (sess->hmap_query)
        {
            // Check whether this is a hash map query path.
            const hmap::query::request req = sess->hmap_query->parse_request_path(res_path.data());
            if (req.mode != hmap::query::MODE::UNDEFINED)
                return 0;
        }

        // Check if file is being opened in truncate mode.
        if (fi->flags & O_TRUNC)
            sess->fuse_adapter->truncate(res_path, 0);

        return 0;
    }

    int fs_read(const char *full_path, char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi)
    {
        const auto &[sess_name, res_path] = session::split_path(full_path);
        CHECK_SESSION(sess_name);

        if (sess->hmap_query)
        {
            // Check whether this is a hash map query path.
            const hmap::query::hmap_query &hmap_query = sess->hmap_query.value();
            const hmap::query::request req = hmap_query.parse_request_path(res_path.data());
            if (req.mode != hmap::query::MODE::UNDEFINED)
                return hmap_query.read(req, buf, size);
        }

        return sess->fuse_adapter->read(res_path, buf, size, offset);
    }

    int fs_write(const char *full_path, const char *buf, size_t size,
                 off_t offset, struct fuse_file_info *fi)
    {
        const auto &[sess_name, res_path] = session::split_path(full_path);
        CHECK_SESSION(sess_name);
        return sess->fuse_adapter->write(res_path, buf, size, offset);
    }

    int fs_statfs(const char *full_path, struct statvfs *stbuf)
    {
        return 0;
    }

    static int fs_flush(const char *full_path, struct fuse_file_info *fi)
    {
        /* This is called from every close on an open file, so call the
	   close on the underlying filesystem.	But since flush may be
	   called multiple times for an open file, this must not really
	   close the file.  This is important if used on a network
	   filesystem like NFS which flush the data/metadata on close() */
        if (fi->fh > 0)
            close(dup(fi->fh));

        return 0;
    }

    int fs_release(const char *full_path, struct fuse_file_info *fi)
    {
        if (fi->fh > 0)
            close(fi->fh);
        return 0;
    }

    int fs_truncate(const char *full_path, off_t size, struct fuse_file_info *fi)
    {
        const auto &[sess_name, res_path] = session::split_path(full_path);
        CHECK_SESSION(sess_name);
        return sess->fuse_adapter->truncate(res_path, size);
    }

    int fs_fsync(const char *full_path, int isdatasync, struct fuse_file_info *fi)
    {
        return 0;
    }

#ifdef HAVE_POSIX_FALLOCATE
    int fs_fallocate(const char *full_path, int mode,
                     off_t offset, off_t length, struct fuse_file_info *fi)
    {
        return 0;
    }
#endif

#ifdef HAVE_SETXATTR
    /* xattr operations are optional and can safely be left unimplemented */
    int fs_setxattr(const char *full_path, const char *name, const char *value,
                    size_t size, int flags)
    {
        return 0;
    }

    int fs_getxattr(const char *full_path, const char *name, char *value,
                    size_t size)
    {
        return 0;
    }

    int fs_listxattr(const char *full_path, char *list, size_t size)
    {
        return 0;
    }

    int fs_removexattr(const char *full_path, const char *name)
    {
        return 0;
    }
#endif /* HAVE_SETXATTR */

#ifdef HAVE_LIBULOCKMGR
    int fs_lock(const char *full_path, struct fuse_file_info *fi, int cmd,
                struct flock *lock)
    {
        return 0;
    }
#endif

    int fs_flock(const char *full_path, struct fuse_file_info *fi, int op)
    {
        return 0;
    }

#ifdef HAVE_COPY_FILE_RANGE
    ssize_t fs_copy_file_range(const char *path_in,
                               struct fuse_file_info *fi_in,
                               off_t off_in, const char *path_out,
                               struct fuse_file_info *fi_out,
                               off_t off_out, size_t len, int flags)
    {
        return 0;
    }
#endif

    off_t fs_lseek(const char *full_path, off_t off, int whence, struct fuse_file_info *fi)
    {
        return 0;
    }

    void assign_operations(fuse_operations &fs_oper)
    {
        fs_oper.getattr = fs_getattr;
        fs_oper.readlink = fs_readlink;
        //fs_oper.mknod = fs_mknod;
        fs_oper.mkdir = fs_mkdir;
        fs_oper.unlink = fs_unlink;
        fs_oper.rmdir = fs_rmdir;
        fs_oper.symlink = fs_symlink;
        fs_oper.rename = fs_rename;
        fs_oper.link = fs_link;
        fs_oper.chmod = fs_chmod;
        fs_oper.chown = fs_chown;
        fs_oper.truncate = fs_truncate;
        fs_oper.open = fs_open;
        fs_oper.read = fs_read;
        fs_oper.write = fs_write;
        fs_oper.statfs = fs_statfs;
        fs_oper.flush = fs_flush;
        fs_oper.release = fs_release;
        fs_oper.fsync = fs_fsync;
#ifdef HAVE_SETXATTR
        fs_oper.setxattr = fs_setxattr;
        fs_oper.getxattr = fs_getxattr;
        fs_oper.listxattr = fs_listxattr;
        fs_oper.removexattr = fs_removexattr;
#endif
        //fs_oper.opendir = fs_opendir;
        fs_oper.readdir = fs_readdir;
        //fs_oper.releasedir = fs_releasedir;
        //fs_oper.fsyncdir = NULL;
        fs_oper.init = fs_init;
        //fs_oper.destroy = NULL;
        fs_oper.access = fs_access;
        fs_oper.create = fs_create;
#ifdef HAVE_LIBULOCKMGR
        fs_oper.lock = fs_lock;
#endif
        fs_oper.utimens = fs_utimens;
        //fs_oper.bmap = NULL;
        //fs_oper.ioctl = NULL;
        //fs_oper.poll = NULL;
        //fs_oper.write_buf = fs_write_buf;
        //fs_oper.read_buf = fs_read_buf;
        fs_oper.flock = fs_flock;
#ifdef HAVE_POSIX_FALLOCATE
        fs_oper.fallocate = fs_fallocate;
#endif
#ifdef HAVE_COPY_FILE_RANGE
        fs_oper.copy_file_range = fs_copy_file_range;
#endif
        fs_oper.lseek = fs_lseek;
    }

    fuse_operations fs_oper;

    int init(char *arg0)
    {
        fuse_args args = FUSE_ARGS_INIT(0, NULL);
        fuse_opt_add_arg(&args, arg0);
        fuse_opt_add_arg(&args, hpfs::ctx.mount_dir.c_str()); // Mount dir
        fuse_opt_add_arg(&args, "-f");                        // Foreground
        fuse_opt_add_arg(&args, "-ofsname=hpfs");
        fuse_opt_add_arg(&args, "-odefault_permissions");
        // fuse_opt_add_arg(&args, "-s"); // Single threaded
        // fuse_opt_add_arg(&args, "-d"); // Debug

        umask(0);
        assign_operations(fs_oper);
        return fuse_main(args.argc, args.argv, &fs_oper, NULL);
    }

} // namespace hpfs::fusefs