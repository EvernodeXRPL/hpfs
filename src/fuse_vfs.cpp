#include <iostream>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include "vfs.hpp"
#include "hpfs.hpp"
#include "util.hpp"
#include "hmap/hmap.hpp"

namespace fuse_vfs
{

    int getattr(const char *vpath, struct stat *stbuf)
    {
        vfs::vnode *vn;
        if (vfs::get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        *stbuf = vn->st;
        return 0;
    }

    int readdir(const char *vpath, vfs::vdir_children_map &children)
    {
        vfs::vnode *vn;
        if (vfs::get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;
        if (!S_ISDIR(vn->st.st_mode))
            return -ENOTDIR;

        return vfs::get_dir_children(vpath, children);
    }

    int mkdir(const char *vpath, mode_t mode)
    {
        if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
            return -EACCES;

        vfs::vnode *vn;
        if (vfs::get_vnode(vpath, &vn) == -1)
            return -1;
        if (vn)
            return -EEXIST;

        iovec payload{&mode, sizeof(mode)};
        if (logger::append_log(vpath, logger::FS_OPERATION::MKDIR, &payload) == -1 ||
            vfs::build_vfs() == -1 ||
            hmap::apply_vnode_create(vpath) == -1)
            return -1;

        return 0;
    }

    int rmdir(const char *vpath)
    {
        if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
            return -EACCES;

        vfs::vnode *vn;
        if (vfs::get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        vfs::vdir_children_map children;
        vfs::get_dir_children(vpath, children);
        if (!children.empty())
            return -ENOTEMPTY;

        if (logger::append_log(vpath, logger::FS_OPERATION::RMDIR) == -1 ||
            vfs::build_vfs() == -1 ||
            hmap::apply_vnode_delete(vpath) == -1)
            return -1;

        return 0;
    }

    int rename(const char *from_vpath, const char *to_vpath)
    {
        if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
            return -EACCES;

        vfs::vnode *vn;
        if (vfs::get_vnode(from_vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        iovec payload{(void *)to_vpath, strlen(to_vpath) + 1};
        if (logger::append_log(from_vpath, logger::FS_OPERATION::RENAME, &payload) == -1 ||
            vfs::build_vfs() == -1)
            return -1;

        return 0;
    }

    int unlink(const char *vpath)
    {
        if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
            return -EACCES;

        vfs::vnode *vn;
        if (vfs::get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        if (logger::append_log(vpath, logger::FS_OPERATION::UNLINK) == -1 ||
            vfs::build_vfs() == -1 ||
            hmap::apply_vnode_delete(vpath) == -1)
            return -1;

        return 0;
    }

    int create(const char *vpath, mode_t mode)
    {
        if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
            return -EACCES;

        vfs::vnode *vn;
        if (vfs::get_vnode(vpath, &vn) == -1)
            return -1;
        if (vn)
            return -EEXIST;

        iovec payload{&mode, sizeof(mode)};
        if (logger::append_log(vpath, logger::FS_OPERATION::CREATE, &payload) == -1 ||
            vfs::build_vfs() == -1 ||
            hmap::apply_vnode_create(vpath) == -1)
            return -1;

        return 0;
    }

    int read(const char *vpath, char *buf, const size_t size, const off_t offset)
    {
        vfs::vnode *vn;
        if (get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        if (vn->st.st_size == 0 || offset >= vn->st.st_size)
            return 0;

        size_t read_len = size;
        if ((offset + size) > vn->st.st_size)
            read_len = vn->st.st_size - offset;

        memcpy(buf, (uint8_t *)vn->mmap.ptr + offset, read_len);

        return read_len;
    }

    int write(const char *vpath, const char *buf, size_t wr_size, off_t wr_start)
    {
        if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
            return -EACCES;

        vfs::vnode *vn;
        if (get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        // We prepare list of block buf segments based on where the write buf lies within the block buf.
        off_t block_buf_start = 0, block_buf_end = 0;
        std::vector<iovec> block_buf_segs;
        vfs::populate_block_buf_segs(block_buf_segs, block_buf_start, block_buf_end,
                                     buf, wr_size, wr_start, vn->st.st_size, (uint8_t *)vn->mmap.ptr);

        const size_t block_buf_size = block_buf_end - block_buf_start;
        logger::op_write_payload_header wh{wr_size, wr_start, block_buf_size,
                                           block_buf_start, (wr_start - block_buf_start)};

        iovec payload{&wh, sizeof(wh)};
        if (logger::append_log(vpath, logger::FS_OPERATION::WRITE, &payload,
                               block_buf_segs.data(), block_buf_segs.size()) == -1 ||
            vfs::build_vfs() == -1 ||
            hmap::apply_vnode_update(vpath, *vn, wr_start, wr_size) == -1)
            return -1;

        return wr_size;
    }

    int truncate(const char *vpath, const off_t new_size)
    {
        if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
            return -EACCES;

        vfs::vnode *vn;
        if (get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;
        size_t current_size = vn->st.st_size;
        if (new_size == current_size)
            return 0;

        std::vector<iovec> block_buf_segs;
        logger::op_truncate_payload_header th{(size_t)new_size, 0, 0};

        if (new_size > current_size)
        {
            // If the new file size is bigger than old size, prepare a block buffer
            // so the extra NULL bytes can be mapped to memory.

            off_t block_buf_start = 0, block_buf_end = 0;
            vfs::populate_block_buf_segs(block_buf_segs, block_buf_start, block_buf_end,
                                         NULL, 0, new_size, vn->st.st_size, (uint8_t *)vn->mmap.ptr);

            const size_t block_buf_size = block_buf_end - block_buf_start;
            th.mmap_block_offset = block_buf_start;
            th.mmap_block_size = block_buf_size;
        }

        iovec payload{&th, sizeof(th)};
        if (logger::append_log(vpath, logger::FS_OPERATION::TRUNCATE, &payload,
                               block_buf_segs.data(), block_buf_segs.size()) == -1 ||
            vfs::build_vfs() == -1 ||
            hmap::apply_vnode_update(vpath, *vn,
                                     MIN(new_size, current_size),
                                     MAX(0, new_size - current_size)) == -1)
            return -1;

        return 0;
    }

} // namespace fuse_vfs