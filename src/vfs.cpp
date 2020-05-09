#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <unordered_map>
#include <iostream>
#include <dirent.h>
#include <sys/mman.h>
#include <vector>
#include <unordered_set>
#include <libgen.h>
#include "vfs.hpp"
#include "hpfs.hpp"
#include "logger.hpp"
#include "util.hpp"

namespace vfs
{

uint next_ino = 1;
vnode_map vnodes;
vfs_node *root_vnode;

// Last checkpoint offset for the use of ReadOnly session (inclusive of the checkpointed log record).
off_t last_checkpoint = 0;

int init()
{
    // In ReadOnly session, remember the last checkpoint record offset during initialisation.
    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO)
    {
        logger::log_header lh;
        if (logger::read_header(lh) == -1)
            return -1;
        last_checkpoint = lh.last_checkpoint;
    }

    struct stat st;
    stat(hpfs::ctx.seed_dir.c_str(), &st);

    vnode_map::iterator iter = vnodes.end();
    add_vnode("/", st, 0, iter);
    root_vnode = &iter->second;
}

void deinit()
{
    for (const auto &[vpath, vnode] : vnodes)
        close(vnode.seed_fd);
}

int add_vnode(const std::string &vpath, struct stat &st,
              const off_t log_offset, vnode_map::iterator &vnode_iter)
{
    st.st_ino = next_ino++;
    const auto [iter, success] = vnodes.try_emplace(vpath, vfs_node{st, log_offset});
    vnode_iter = iter;
    return 0;
}

int build_vnode(const std::string &vpath, vnode_map::iterator &iter, const bool stat_only)
{
    // Read the current log header with read-lock.
    flock log_header_lock;
    logger::log_header lh;
    if (logger::set_lock(log_header_lock, false, 0, sizeof(lh)) == -1 ||
        logger::read_header(lh) == -1 ||
        logger::release_lock(log_header_lock) == -1)
        return -1;

    // Calculate the offset upto which we should scan the log file (inclusive of log record).
    // In RO Session, it's upto the last checkpoint that was seen at RO session start.
    // In RW session, it's upto the current end of log file.
    // If there are no records, this will be 0.
    off_t scan_upto = 0;
    if (lh.first_record > 0)
        scan_upto = (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO) ? last_checkpoint : logger::get_eof();

    // Read-lock the log file offset range that we are going to scan.
    flock scan_lock;
    if (logger::set_lock(scan_lock, false, lh.first_record, (scan_upto - lh.first_record)) == -1)
        return -1;

    if (iter == vnodes.end())
        iter = vnodes.find(vpath); // Check whether we are already tracking this vpath.

    // Attempt to construct virtual node from within seed directory (if exist).
    if (iter == vnodes.end())
    {
        const std::string seed_path = std::string(hpfs::ctx.seed_dir).append(vpath);

        struct stat st;
        const int res = stat(seed_path.c_str(), &st);
        if (res == -1 && errno != ENOENT)
            goto failure;

        if (res != -1) // Seed file/dir exists. So we must initialize the virtual node.
        {
            if (add_vnode(vpath, st, 0, iter) == -1)
                return -1;

            if (S_ISREG(st.st_mode)) // is file.
            {
                const int fd = open(seed_path.c_str(), O_RDONLY);
                if (fd == -1)
                    return -1;
                if (st.st_size > 0)
                    iter->second.data_segs.push_back(vdata_segment{fd, (size_t)st.st_size, 0, 0});

                iter->second.seed_fd = fd;
            }
        }
    }

    // Attempt vnode build up using log records.
    {
        // Return immediately if there are no log records.
        if (scan_upto == 0)
            goto success;

        // Return immediately if the virtual node is already fully updated.
        off_t next_log_offset = (iter == vnodes.end()) ? lh.first_record : iter->second.log_scanned_upto;
        if (next_log_offset >= scan_upto)
            goto success;

        // Loop through any unapplied log records and apply any changes to the virtual node.
        do
        {
            logger::log_record record;
            if (logger::read_log_at(next_log_offset, next_log_offset, record) == -1)
                break;

            if (record.vpath == vpath)
            {
                if (iter == vnodes.end())
                {
                    // Use the root node stat as initial stat template.
                    struct stat st = root_vnode->st;
                    st.st_nlink = 0;
                    st.st_size = 0;
                    add_vnode(vpath, st, next_log_offset, iter);
                }

                std::vector<uint8_t> payload;
                if (logger::read_payload(payload, record) == -1 ||
                    apply_log_record_to_vnode(iter, record, payload) == -1)
                    goto failure;
            }

            if (iter != vnodes.end())
                iter->second.log_scanned_upto = record.offset + record.size;

        } while (next_log_offset > 0 && next_log_offset < scan_upto);
    }

success:
    // Update the memory mapped data blocks.
    if (!stat_only && iter != vnodes.end() && update_vnode_fmap(iter->second) == -1)
        goto failure;

    return logger::release_lock(scan_lock);

failure:
    logger::release_lock(scan_lock);
    return -1;
}

int get_vnode(const char *vpath, vfs_node **vnode, const bool stat_only)
{
    vnode_map::iterator iter = vnodes.end();
    if (build_vnode(vpath, iter, stat_only) == -1)
        return -1;

    *vnode = (iter == vnodes.end() || iter->second.is_removed) ? NULL : &(iter->second);
    return 0;
}

int apply_log_record_to_vnode(vnode_map::iterator &vnode_iter, const logger::log_record &record,
                              const std::vector<uint8_t> payload)
{
    vfs_node &vnode = vnode_iter->second;

    switch (record.operation)
    {
    case logger::FS_OPERATION::MKDIR:
        if (vnode.is_removed) // A previous log record has removed it and now we are creating again.
        {
            vnode.st.st_ino = next_ino++; // Allocate new inode no.
            vnode.is_removed = false;
        }
        vnode.st.st_mode = S_IFDIR | *(mode_t *)payload.data();
        break;

    case logger::FS_OPERATION::RMDIR:
        mark_vnode_as_removed(vnode);
        break;

    case logger::FS_OPERATION::RENAME:
    {
        const char *to_vpath = (char *)payload.data();

        // If "to" path already exists, delete it.
        vfs_node *ex_vnode;
        if (get_vnode(to_vpath, &ex_vnode) == -1)
            return -1;
        if (ex_vnode && mark_vnode_as_removed(*ex_vnode) == -1)
            return -1;
        vnodes.erase(to_vpath);

        // Rename this vnode. (erase it from the list and insert under new name)
        vfs_node vnode_copy = vnode;
        if (mark_vnode_as_removed(vnode) == -1)
            return -1;
        vnodes.erase(record.vpath);
        auto [iter, success] = vnodes.try_emplace(to_vpath, std::move(vnode_copy));
        vnode_iter = iter;

        break;
    }

    case logger::FS_OPERATION::UNLINK:
        mark_vnode_as_removed(vnode);
        break;

    case logger::FS_OPERATION::CREATE:
        if (vnode.is_removed) // A previous log record has removed it and now we are creating again.
        {
            vnode.st.st_ino = next_ino++; // Allocate new inode no.
            vnode.is_removed = false;
        }
        vnode.st.st_mode = S_IFREG | *(mode_t *)payload.data();
        break;

    case logger::FS_OPERATION::WRITE:
    {
        const logger::op_write_payload_header wh = *(logger::op_write_payload_header *)payload.data();

        const off_t logical_offset = util::get_block_start(wh.offset);
        vnode.data_segs.push_back(vdata_segment{logger::fd, record.block_data_len, record.block_data_offset, logical_offset});

        // Update stats, if the new data boundry is larger than current file size.
        if (vnode.st.st_size < (wh.offset + wh.size))
            vnode.st.st_size = wh.offset + wh.size;

        break;
    }

    default:
        break;
    }
}

int update_vnode_fmap(vfs_node &vnode)
{
    if (vnode.mapped_data_segs == vnode.data_segs.size())
        return 0;

    const off_t required_map_size = util::get_block_end(vnode.st.st_size);

    if (vnode.mmap.ptr && vnode.mmap.size != required_map_size)
    {
        if (munmap(vnode.mmap.ptr, vnode.mmap.size) == -1)
            return -1;

        vnode.mapped_data_segs = 0;
    }

    for (uint32_t idx = vnode.mapped_data_segs; idx < vnode.data_segs.size(); idx++)
    {
        const vdata_segment &seg = vnode.data_segs.at(idx);

        if (!vnode.mmap.ptr)
        {
            // Create the mapping for the full size needed.
            const size_t size = util::get_block_end(vnode.st.st_size);
            void *ptr = mmap(NULL, size, PROT_READ, MAP_PRIVATE, seg.physical_fd, seg.physical_offset);
            if (ptr == MAP_FAILED)
                return -1;

            vnode.mmap.ptr = ptr;
            vnode.mmap.size = size;
        }
        else
        {
            void *ptr = mmap(((uint8_t *)vnode.mmap.ptr + seg.logical_offset),
                             seg.size, PROT_READ, MAP_PRIVATE | MAP_FIXED,
                             seg.physical_fd, seg.physical_offset);

            if (ptr == MAP_FAILED)
                return -1;
        }
    }

    vnode.mapped_data_segs = vnode.data_segs.size();
    return 0;
}

int mark_vnode_as_removed(vfs_node &vnode)
{
    if (vnode.mmap.ptr && munmap(vnode.mmap.ptr, vnode.mmap.size) == -1)
        return -1;

    vnode.is_removed = true;
    vnode.mmap = {NULL, 0};
    vnode.data_segs.clear();
    vnode.mapped_data_segs = 0;
    vnode.seed_fd = 0;
    vnode.st.st_nlink = 0;
    vnode.st.st_size = 0;
    return 0;
}

int getattr(const char *vpath, struct stat *stbuf)
{
    vfs_node *vnode;
    if (get_vnode(vpath, &vnode) == -1)
        return -1;
    if (!vnode)
        return -ENOENT;

    *stbuf = vnode->st;
    return 0;
}

int readdir(const char *vpath, vdir_children_map &children)
{
    vfs_node *vnode;
    if (get_vnode(vpath, &vnode, true) == -1)
        return -1;
    if (!vnode)
        return -ENOENT;
    if (!S_ISDIR(vnode->st.st_mode))
        return -ENOTDIR;

    std::unordered_set<std::string> possible_child_names;

    {
        // Read possible children from seed dir;
        const std::string seed_path = std::string(hpfs::ctx.seed_dir).append(vpath);
        DIR *dirp = opendir(seed_path.c_str());
        if (dirp != NULL)
        {
            dirent *entry;
            while (entry = readdir(dirp))
                possible_child_names.emplace(entry->d_name);

            closedir(dirp);
        }
    }

    {
        // Find possible children from log records.
        off_t next_log_offset = 0;
        do
        {
            logger::log_record record;
            if (logger::read_log_at(next_log_offset, next_log_offset, record) == -1)
                break;

            char *path2 = strdup(record.vpath.c_str());
            char *parent_path = dirname(path2);
            if (strcmp(parent_path, vpath) == 0)
                possible_child_names.emplace(basename(record.vpath.data()));

        } while (next_log_offset > 0);
    }

    for (const auto &child_name : possible_child_names)
    {
        std::string child_vpath = std::string(vpath);
        if (child_vpath.back() != '/')
            child_vpath.append("/");
        child_vpath.append(child_name);

        vfs_node *child_vnode;
        if (get_vnode(child_vpath.c_str(), &child_vnode, true) == -1)
            return -1;

        if (child_vnode)
        {
            if (!child_vnode->is_removed)
                children.try_emplace(child_name, child_vnode->st);
        }
    }

    return 0;
}

int mkdir(const char *vpath, mode_t mode)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    // Check if vnode for same vpath already exists.
    vfs_node *vnode;
    if (get_vnode(vpath, &vnode) == -1)
        return -1;
    if (vnode)
        return -EEXIST;

    iovec payload{&mode, sizeof(mode)};
    return logger::append_log(vpath, logger::FS_OPERATION::MKDIR, &payload);
}

int rmdir(const char *vpath)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    vfs_node *vnode;
    if (get_vnode(vpath, &vnode) == -1)
        return -1;
    if (!vnode)
        return -ENOENT;

    return logger::append_log(vpath, logger::FS_OPERATION::RMDIR);
}

int rename(const char *from_vpath, const char *to_vpath)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    vfs_node *from_vnode;
    if (get_vnode(from_vpath, &from_vnode) == -1)
        return -1;
    if (!from_vnode)
        return -ENOENT;

    iovec payload{(void *)to_vpath, strlen(to_vpath) + 1};
    return logger::append_log(from_vpath, logger::FS_OPERATION::RENAME, &payload);
}

int unlink(const char *vpath)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    vfs_node *vnode;
    if (get_vnode(vpath, &vnode) == -1)
        return -1;
    if (!vnode)
        return -ENOENT;

    return logger::append_log(vpath, logger::FS_OPERATION::UNLINK);
}

int create(const char *vpath, mode_t mode)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    // Check if vnode for same vpath already exists.
    vfs_node *vnode;
    if (get_vnode(vpath, &vnode) == -1)
        return -1;
    if (vnode)
        return -EEXIST;

    iovec payload{&mode, sizeof(mode)};
    return logger::append_log(vpath, logger::FS_OPERATION::CREATE, &payload);
}

int read(const char *vpath, char *buf, size_t size, off_t offset)
{
    vfs_node *vnode;
    if (get_vnode(vpath, &vnode) == -1)
        return -1;
    if (!vnode)
        return -ENOENT;

    if (!vnode->mmap.ptr)
        return -1;

    size_t read_len = size;
    if ((offset + size) > vnode->st.st_size)
        read_len = vnode->st.st_size - offset;

    memcpy(buf, (uint8_t *)vnode->mmap.ptr + offset, read_len);

    return read_len;
}

int write(const char *vpath, const char *buf, size_t size, off_t offset)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    vfs_node *vnode;
    if (get_vnode(vpath, &vnode) == -1)
        return -1;
    if (!vnode)
        return -ENOENT;

    logger::op_write_payload_header wh{size, offset, 0};

    // Create block data buf
    const off_t block_data_start = util::get_block_start(offset);
    const size_t block_data_end = util::get_block_end(offset + size);
    const size_t block_data_size = block_data_end - block_data_start;

    iovec block_data_buf;
    std::vector<uint8_t> vec;

    if (block_data_start == offset && block_data_size == size)
    {
        // If the block start/end offsets are in perfect alignment,
        // log the incoming write buf as it is.
        block_data_buf = {(void *)buf, size};
    }
    else
    {
        // Construct new buf with block alignment.
        vec.resize(block_data_size);

        // Read any existing memory-mapped blocks and place on the new buf.
        const fmap &mmap = vnode->mmap;
        if (mmap.ptr && mmap.size > block_data_start)
        {
            off_t read_len = block_data_size;
            if ((block_data_start + read_len) > mmap.size)
                read_len = mmap.size - block_data_start;

            memcpy(vec.data(), (uint8_t *)mmap.ptr + block_data_start, read_len);
        }

        // Overlay the incoming write buf.
        wh.buf_offset_in_block_data = offset - block_data_start; // Real data offset within the block buf.
        memcpy(vec.data() + wh.buf_offset_in_block_data, buf, size);

        // Add the block buf to log record payload.
        block_data_buf = {vec.data(), block_data_size};
    }

    iovec payload{&wh, sizeof(wh)};
    return logger::append_log(vpath, logger::FS_OPERATION::WRITE, &payload, &block_data_buf);
}

} // namespace vfs