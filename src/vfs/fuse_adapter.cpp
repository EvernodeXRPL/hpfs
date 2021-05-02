#include <string.h>
#include <libgen.h>
#include "vfs.hpp"
#include "fuse_adapter.hpp"
#include "virtual_filesystem.hpp"
#include "../hmap/tree.hpp"
#include "../util.hpp"

/**
 * Bridge between fuse interface and the hpfs virtual filesystem interface.
 */

#define FS_READ_LOCK std::shared_lock lock(fs_mutex);
#define FS_WRITE_LOCK std::unique_lock lock(fs_mutex);

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

    int fuse_adapter::getattr(const std::string &vpath, struct stat *stbuf)
    {
        FS_READ_LOCK

        vfs::vnode *vn = NULL;
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

        vfs::vnode *vn = NULL;
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

        vfs::vnode *vn = NULL;
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
            (htree && logger.update_log_record_hash(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::rmdir(const std::string &vpath)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        vfs::vnode *vn = NULL;
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

        vfs::vnode *vn = NULL;
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

        vfs::vnode *vn = NULL;
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
            (htree && logger.update_log_record_hash(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::read(const std::string &vpath, char *buf, const size_t size, const off_t offset)
    {
        FS_READ_LOCK

        vfs::vnode *vn = NULL;
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

    int fuse_adapter::write(const std::string &vpath, const char *buf, const size_t size, const off_t offset)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        vfs::vnode *vn = NULL;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        // Holds the offset of the log record that is going to be appended/modified.
        off_t log_record_offset = 0;

        // Log record header that was appended/modified.
        hpfs::audit::log_record_header rh;

        // The log record offset to build the vfs from. 0 indicates current offset vfs
        // is already is tracking will be used.
        off_t vfs_build_from = 0;

        // First, attempt an optimized write.
        const int optimze_res = optimized_write(vpath, buf, size, offset, vn, rh);
        if (optimze_res == -1)
        {
            return -1;
        }
        else if (optimze_res == 1) // Optimized write successful.
        {
            const hpfs::audit::log_header &h = logger.get_header();
            log_record_offset = vfs_build_from = h.last_record;
        }
        else // Optimized write criteria not met. So we need to perform a normal write.
        {
            if ((log_record_offset = normal_write(vpath, buf, size, offset, vn, rh)) == 0)
                return -1;
        }

        if (log_record_offset == 0 ||
            virt_fs.build_vfs(vfs_build_from) == -1 ||
            (htree && htree->apply_vnode_data_update(vpath, *vn, offset, size) == -1) ||
            (htree && logger.update_log_record_hash(log_record_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return size;
    }

    int fuse_adapter::truncate(const std::string &vpath, const off_t new_size)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        vfs::vnode *vn = NULL;
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
            (htree && htree->apply_vnode_data_update(vpath, *vn,
                                                     MIN(new_size, current_size),
                                                     MAX(0, new_size - current_size)) == -1) ||
            (htree && logger.update_log_record_hash(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

    int fuse_adapter::chmod(const std::string &vpath, mode_t mode)
    {
        if (readonly)
            return -EACCES;

        FS_WRITE_LOCK

        vfs::vnode *vn = NULL;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
            return -1;
        if (!vn)
            return -ENOENT;

        audit::log_record_header rh;
        iovec payload{&mode, sizeof(mode)};
        off_t log_rec_start_offset = logger.append_log(rh, vpath, hpfs::audit::FS_OPERATION::CHMOD, &payload);
        if (log_rec_start_offset == 0 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_metadata_update(vpath, *vn) == -1) ||
            (htree && logger.update_log_record_hash(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

    /**
     * Non-optimized, normal write which simply appends a log record with the written data.
     * @return Appended log record offset on success. 0 on error.
     */
    off_t fuse_adapter::normal_write(const std::string &vpath, const char *buf, const size_t wr_size, const off_t wr_start,
                                   vfs::vnode *vn, audit::log_record_header &rh)
    {
        // We prepare list of block buf segments based on where the write buf lies within the block buf.
        off_t block_buf_start = 0, block_buf_end = 0;
        std::vector<iovec> block_buf_segs;
        virt_fs.populate_block_buf_segs(block_buf_segs, block_buf_start, block_buf_end,
                                        buf, wr_size, wr_start, vn->st.st_size, (uint8_t *)vn->mmap.ptr);

        // No write-optimization performed.
        const size_t block_buf_size = block_buf_end - block_buf_start;
        hpfs::audit::op_write_payload_header wh{wr_size, wr_start, block_buf_size,
                                                block_buf_start, (wr_start - block_buf_start)};

        iovec payload{&wh, sizeof(wh)};
        const off_t log_rec_start_offset = logger.append_log(rh, vpath, hpfs::audit::FS_OPERATION::WRITE, &payload,
                                                             block_buf_segs.data(), block_buf_segs.size());

        return log_rec_start_offset;
    }

    /**
     * Performs an optimized-write without appending a log record if all required criterias are met.
     * @return 0 if optimal criteria not met. 1 if optimized write successfully performed. -1 on error.
     */
    int fuse_adapter::optimized_write(const std::string &vpath, const char *buf, const size_t wr_size, const off_t wr_start,
                                      vfs::vnode *vn, audit::log_record_header &rh)
    {
        // Write-oprimization
        // ------------------
        // If the last audited operation is a write operation to the same file, and the new write block
        // is same or next to the last write block, we can update the same log record instead
        // of creating a new log record.
        std::optional<hpfs::audit::fs_operation_summary> &last_op = logger.get_last_operation();
        if (!last_op || last_op->vpath != vpath || last_op->rh.operation != hpfs::audit::FS_OPERATION::WRITE)
            return 0; // Operation criteria not met.

        const hpfs::audit::op_write_payload_header &prev = *(hpfs::audit::op_write_payload_header *)last_op->payload.data();

        const off_t prev_block_start = BLOCK_START(prev.offset); // Block aligned start offset of previous write.
        const size_t prev_end = prev.offset + prev.size;         // End offset of previous write.
        const off_t prev_block_end = BLOCK_END(prev_end);        // Block aligned end offset of previous write.

        const off_t new_block_start = BLOCK_START(wr_start); // Block aligned start offset of new write.
        const size_t new_end = wr_start + wr_size;           // End offset of new write.
        const off_t new_block_end = BLOCK_END(new_end);      // Block aligned end offset of new write.

        // Check whether new write block is same or next to the previous write block.
        // If so, we must perform write-optimization. We replace or append the write block to existing log record.
        if (!(prev_block_start <= new_block_start && new_block_start <= prev_block_end))
            return 0; // Same/adjecent write block criteria not met.

        // Adjust the new write payload to simulate a union of previous and new write.
        const off_t union_wr_start = MIN(prev.offset, wr_start);
        const size_t union_wr_size = MAX(prev_end, new_end) - union_wr_start;
        const off_t union_block_buf_start = MIN(prev_block_start, new_block_start);
        const size_t union_block_buf_size = MAX(prev_block_end, new_block_end) - union_block_buf_start;

        hpfs::audit::op_write_payload_header union_wh{union_wr_size, union_wr_start, union_block_buf_size,
                                                      union_block_buf_start, (union_wr_start - union_block_buf_start)};
        iovec payload{&union_wh, sizeof(union_wh)};

        rh = last_op->rh;
        const hpfs::audit::log_record_metrics lm = hpfs::audit::audit_logger::get_metrics(rh);

        // If the new write buf is completely contained within the same block as previous write, we simply write the
        // raw buf into the log file.
        if (prev_block_start == new_block_start && prev_block_end == new_block_end)
        {
            iovec block_bufs[1] = {{(void *)buf, wr_size}};
            const size_t write_buf_padding = wr_start - new_block_start; // No. of padding bytes between actual write buf and the aligned block start.
            const off_t data_write_offset = lm.block_data_offset + write_buf_padding;
            if (logger.overwrite_last_log_record_bytes(lm.payload_offset, data_write_offset,
                                                       &payload, block_bufs, 1, 0, rh) == -1)
                return -1;
        }
        else
        {
            // We prepare list of block buf segments based on where the write buf lies within the block buf.
            off_t block_buf_start = 0, block_buf_end = 0;
            std::vector<iovec> block_buf_segs;
            virt_fs.populate_block_buf_segs(block_buf_segs, block_buf_start, block_buf_end,
                                            buf, wr_size, wr_start, vn->st.st_size, (uint8_t *)vn->mmap.ptr);

            // We need to place the new write block offset relative to the previous write block.
            const off_t block_data_write_offset = lm.block_data_offset + (new_block_start - prev_block_start);
            if (logger.overwrite_last_log_record_bytes(lm.payload_offset, block_data_write_offset,
                                                       &payload, block_buf_segs.data(), block_buf_segs.size(), union_block_buf_size, rh) == -1)
                return -1;
        }

        // Update the tracked last operation.
        last_op->update(vpath, rh, &payload);

        return 1; // Write optmization successfuly performed.
    }

    int fuse_adapter::delete_entry(const std::string &vpath, const bool is_dir)
    {
        audit::log_record_header rh;
        off_t log_rec_start_offset = logger.append_log(rh, vpath, is_dir ? hpfs::audit::FS_OPERATION::RMDIR : hpfs::audit::FS_OPERATION::UNLINK);
        if (log_rec_start_offset == 0 ||
            virt_fs.build_vfs() == -1 ||
            (htree && htree->apply_vnode_delete(vpath) == -1) ||
            (htree && logger.update_log_record_hash(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
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
            (htree && logger.update_log_record_hash(log_rec_start_offset, htree->get_root_hash(), rh) == -1))
            return -1;

        return 0;
    }

} // namespace hpfs::vfs