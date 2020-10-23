#include <string.h>
#include "vfs.hpp"
#include "fuse_adapter.hpp"
#include "virtual_filesystem.hpp"
#include "../hmap/tree.hpp"
#include "../audit.hpp"
#include "../util.hpp"

/**
 * Bridge between fuse interface and the hpfs virtual filesystem interface.
 */

namespace hpfs::vfs
{
    fuse_adapter::fuse_adapter(const bool readonly, virtual_filesystem &virt_fs,
                               hpfs::audit::audit_logger &logger,
                               std::optional<hpfs::hmap::tree::hmap_tree> &htree) : readonly(readonly),
                                                                                   virt_fs(virt_fs),
                                                                                   logger(logger),
                                                                                   htree(htree)
    {
    }

    int fuse_adapter::getattr(const char *vpath, struct stat *stbuf)
    {
        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        *stbuf = vn->st;
        return 0;
    }

    int fuse_adapter::readdir(const char *vpath, vfs::vdir_children_map &children)
    {
        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;
        if (!S_ISDIR(vn->st.st_mode))
            return -ENOTDIR;

        return virt_fs.get_dir_children(vpath, children);
    }

    int fuse_adapter::mkdir(const char *vpath, mode_t mode)
    {
        if (readonly)
            return -EACCES;

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (vn)
            return -EEXIST;

        iovec payload{&mode, sizeof(mode)};
        if (logger.append_log(vpath, hpfs::audit::FS_OPERATION::MKDIR, &payload) == -1 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_create(vpath) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::rmdir(const char *vpath)
    {
        if (readonly)
            return -EACCES;

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        vfs::vdir_children_map children;
        virt_fs.get_dir_children(vpath, children);
        if (!children.empty())
            return -ENOTEMPTY;

        if (logger.append_log(vpath, hpfs::audit::FS_OPERATION::RMDIR) == -1 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_delete(vpath) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::rename(const char *from_vpath, const char *to_vpath)
    {
        if (readonly)
            return -EACCES;

        vfs::vnode *vn;
        if (virt_fs.get_vnode(from_vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        iovec payload{(void *)to_vpath, strlen(to_vpath) + 1};
        if (logger.append_log(from_vpath, hpfs::audit::FS_OPERATION::RENAME, &payload) == -1 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_rename(from_vpath, to_vpath) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::unlink(const char *vpath)
    {
        if (readonly)
            return -EACCES;

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        if (logger.append_log(vpath, hpfs::audit::FS_OPERATION::UNLINK) == -1 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_delete(vpath) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::create(const char *vpath, mode_t mode)
    {
        if (readonly)
            return -EACCES;

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (vn)
            return -EEXIST;

        iovec payload{&mode, sizeof(mode)};
        if (logger.append_log(vpath, hpfs::audit::FS_OPERATION::CREATE, &payload) == -1 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_create(vpath) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::read(const char *vpath, char *buf, const size_t size, const off_t offset)
    {
        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
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

    int fuse_adapter::write(const char *vpath, const char *buf, size_t wr_size, off_t wr_start)
    {
        if (readonly)
            return -EACCES;

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        // We prepare list of block buf segments based on where the write buf lies within the block buf.
        off_t block_buf_start = 0, block_buf_end = 0;
        std::vector<iovec> block_buf_segs;
        virt_fs.populate_block_buf_segs(block_buf_segs, block_buf_start, block_buf_end,
                                        buf, wr_size, wr_start, vn->st.st_size, (uint8_t *)vn->mmap.ptr);

        const size_t block_buf_size = block_buf_end - block_buf_start;
        hpfs::audit::op_write_payload_header wh{wr_size, wr_start, block_buf_size,
                                                block_buf_start, (wr_start - block_buf_start)};

        iovec payload{&wh, sizeof(wh)};
        if (logger.append_log(vpath, hpfs::audit::FS_OPERATION::WRITE, &payload,
                              block_buf_segs.data(), block_buf_segs.size()) == -1 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_update(vpath, *vn, wr_start, wr_size) == -1))
            return -1;

        return wr_size;
    }

    int fuse_adapter::truncate(const char *vpath, const off_t new_size)
    {
        if (readonly)
            return -EACCES;

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;
        size_t current_size = vn->st.st_size;
        if (new_size == current_size)
            return 0;

        std::vector<iovec> block_buf_segs;
        hpfs::audit::op_truncate_payload_header th{(size_t)new_size, 0, 0};

        if (new_size > current_size)
        {
            // If the new file size is bigger than old size, prepare a block buffer
            // so the extra NULL bytes can be mapped to memory.

            off_t block_buf_start = 0, block_buf_end = 0;
            virt_fs.populate_block_buf_segs(block_buf_segs, block_buf_start, block_buf_end,
                                            NULL, 0, new_size, vn->st.st_size, (uint8_t *)vn->mmap.ptr);

            const size_t block_buf_size = block_buf_end - block_buf_start;
            th.mmap_block_offset = block_buf_start;
            th.mmap_block_size = block_buf_size;
        }

        iovec payload{&th, sizeof(th)};
        if (logger.append_log(vpath, hpfs::audit::FS_OPERATION::TRUNCATE, &payload,
                              block_buf_segs.data(), block_buf_segs.size()) == -1 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_update(vpath, *vn,
                                                MIN(new_size, current_size),
                                                MAX(0, new_size - current_size)) == -1))
            return -1;

        return 0;
    }
} // namespace hpfs::vfs