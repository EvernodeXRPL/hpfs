#include <string.h>
#include <libgen.h>
#include <shared_mutex>
#include "vfs.hpp"
#include "fuse_adapter.hpp"
#include "virtual_filesystem.hpp"
#include "../hmap/tree.hpp"
#include "../audit.hpp"
#include "../util.hpp"

/**
 * Bridge between fuse interface and the hpfs virtual filesystem interface.
 */

#define FS_READ_LOCK std::shared_lock lock(hpfs::vfs::fs_mutex);
#define FS_WRITE_LOCK std::unique_lock lock(hpfs::vfs::fs_mutex);

namespace hpfs::vfs
{
    // Global mutex between all fs session instances.
    std::shared_mutex fs_mutex;

    fuse_adapter::fuse_adapter(const bool readonly, virtual_filesystem &virt_fs,
                               hpfs::audit::audit_logger &logger,
                               std::optional<hpfs::hmap::tree::hmap_tree> &htree) : readonly(readonly),
                                                                                    virt_fs(virt_fs),
                                                                                    logger(logger),
                                                                                    htree(htree)
    {
    }

    int fuse_adapter::getattr(const std::string &vpath, struct stat *stbuf)
    {
        FS_READ_LOCK

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        *stbuf = vn->st;
        return 0;
    }

    int fuse_adapter::readdir(const std::string &vpath, vfs::vdir_children_map &children)
    {
        FS_READ_LOCK

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;
        if (!S_ISDIR(vn->st.st_mode))
            return -ENOTDIR;

        return virt_fs.get_dir_children(vpath, children);
    }

    int fuse_adapter::mkdir(const std::string &vpath, mode_t mode)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (vn)
            return -EEXIST;

        audit::log_record_header rh;
        iovec payload{&mode, sizeof(mode)};
        off_t log_rec_start_offset = logger.append_log(rh, vpath, hpfs::audit::FS_OPERATION::MKDIR, &payload);
        if (log_rec_start_offset == 0 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_create(vpath) == -1) ||
            (htree && logger.update_log_record(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::rmdir(const std::string &vpath)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        vfs::vdir_children_map children;
        virt_fs.get_dir_children(vpath, children);
        if (!children.empty())
            return -ENOTEMPTY;

        if (delete_entry(vpath, true) == -1)
            return -1;

        return 0;
    }

    int fuse_adapter::rename(const std::string &from_vpath, const std::string &to_vpath)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        // Fail if 'from' does not exist.
        vfs::vnode *vn_from;
        if (virt_fs.get_vnode(from_vpath, &vn_from) == -1)
            return -1;
        if (!vn_from)
            return -ENOENT;

        vfs::vnode *vn_to;
        if (virt_fs.get_vnode(to_vpath, &vn_to) == -1)
            return -1;

        // Rename logic:
        // If 'to' does not exist, 'from' will be renamed to 'to'.
        //      Fail if 'to' parent folder hierarchy does not exist.
        // If 'to' is an existing file,
        //      'from' will overwrite 'to' if 'from' is a file.
        //      Fail if 'from' is a dir.
        // If 'to' is an existing dir, 'from' will move under 'to'.

        const bool from_is_file = S_ISREG(vn_from->st.st_mode);
        const bool to_is_file = vn_to && S_ISREG(vn_to->st.st_mode);

        // Fail if we are renaming a dir to an existing file.
        if (!from_is_file && vn_to && to_is_file)
            return -EEXIST;

        // If 'to' path does not exist, check whether parent dir of 'to' exists.
        if (!vn_to)
        {
            vfs::vnode *to_parent;
            if (virt_fs.get_vnode(util::get_parent_path(to_vpath), &to_parent) == -1)
                return -1;
            if (!to_parent)
                return -ENOENT; // Destination parent dir does not exist. Cannot rename.
        }

        if (from_is_file)
        {
            // If 'to' is a file, delete it first.
            if (to_is_file && delete_entry(to_vpath, false) == -1)
                return -1;

            // We need to rename the 'from' file to the detination path.
            // If 'to' does not exist or is a file, destination is the 'to' path.
            // If 'to' is a dir, destination is the 'to' path + 'from' file name.
            const std::string destination = (!vn_to || to_is_file) ? to_vpath : (to_vpath + "/" + util::get_name(from_vpath));

            if (rename_entry(from_vpath, destination, false) == -1)
                return -1;
            return 0; // Done.
        }
        else
        {
            // Cannot rename if 'to' is a existing file.
            if (to_is_file)
                return -EEXIST; // Cannot rename dir to a file.

            // We need to rename the 'from' dir to the detination path.
            // If 'to' does not exist, destination is the 'to' path.
            // If 'to' exist and is a dir, destination is the 'to' path + 'from' dir name.
            const std::string destination = !vn_to ? to_vpath : (to_vpath + "/" + util::get_name(from_vpath));

            if (rename_entry(from_vpath, destination, true) == -1)
                return -1;
            return 0; // Done.
        }

        return 0;
    }

    int fuse_adapter::unlink(const std::string &vpath)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        if (delete_entry(vpath, false) == -1)
            return -1;

        return 0;
    }

    int fuse_adapter::create(const std::string &vpath, mode_t mode)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        vfs::vnode *vn;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (vn)
            return -EEXIST;

        audit::log_record_header rh;
        iovec payload{&mode, sizeof(mode)};
        off_t log_rec_start_offset = logger.append_log(rh, vpath, hpfs::audit::FS_OPERATION::CREATE, &payload);
        if (log_rec_start_offset == 0 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_create(vpath) == -1) ||
            (htree && logger.update_log_record(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::read(const std::string &vpath, char *buf, const size_t size, const off_t offset)
    {
        FS_READ_LOCK

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

    int fuse_adapter::write(const std::string &vpath, const char *buf, size_t wr_size, off_t wr_start)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

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

        audit::log_record_header rh;
        iovec payload{&wh, sizeof(wh)};
        off_t log_rec_start_offset = logger.append_log(rh, vpath, hpfs::audit::FS_OPERATION::WRITE, &payload,
                                                       block_buf_segs.data(), block_buf_segs.size());
        if (log_rec_start_offset == 0 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_update(vpath, *vn, wr_start, wr_size) == -1) ||
            (htree && logger.update_log_record(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return wr_size;
    }

    int fuse_adapter::truncate(const std::string &vpath, const off_t new_size)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

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

        audit::log_record_header rh;
        iovec payload{&th, sizeof(th)};
        off_t log_rec_start_offset = logger.append_log(rh, vpath, hpfs::audit::FS_OPERATION::TRUNCATE, &payload,
                                                       block_buf_segs.data(), block_buf_segs.size());
        if (log_rec_start_offset == 0 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_update(vpath, *vn,
                                                MIN(new_size, current_size),
                                                MAX(0, new_size - current_size)) == -1) ||
            (htree && logger.update_log_record(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::delete_entry(const std::string &vpath, const bool is_dir)
    {
        audit::log_record_header rh;
        off_t log_rec_start_offset = logger.append_log(rh, vpath, is_dir ? hpfs::audit::FS_OPERATION::RMDIR : hpfs::audit::FS_OPERATION::UNLINK);
        if (log_rec_start_offset == 0 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_delete(vpath) == -1) ||
            (htree && logger.update_log_record(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::rename_entry(const std::string &from_vpath, const std::string &to_vpath, const bool is_dir)
    {
        audit::log_record_header rh;
        iovec payload{(void *)to_vpath.data(), to_vpath.size() + 1};
        off_t log_rec_start_offset = logger.append_log(rh, from_vpath, hpfs::audit::FS_OPERATION::RENAME, &payload);
        if (log_rec_start_offset == 0 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_rename(from_vpath, to_vpath, is_dir) == -1) ||
            (htree && logger.update_log_record(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

} // namespace hpfs::vfs