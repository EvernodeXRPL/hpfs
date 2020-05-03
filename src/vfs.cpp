#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <unordered_map>
#include <iostream>
#include <dirent.h>
#include "vfs.hpp"
#include "hpfs.hpp"
#include "logger.hpp"

namespace vfs
{

ino_t next_ino = 1;
vnode_map vnodes;

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
}

int add_vnode(const std::string &vpath, struct stat &st,
              const off_t log_offset, vnode_map::iterator &vnode_iter)
{
    st.st_ino = next_ino++;
    const auto [iter, success] = vnodes.try_emplace(vpath, vfs_node{st, log_offset});
    vnode_iter = iter;
    return 0;
}

// int init_vnode_from_seed_dir(const std::string &vpath, const std::string &seed_path,
//                              struct stat &st, vnode_map::iterator &iter)
// {
//     if (add_vnode(vpath, st, 0, iter) == -1)
//         return -1;

//     DIR *dirp = opendir(seed_path.c_str());
//     if (dirp == NULL)
//         return -1;

//     dirent *entry;
//     while (entry = readdir(dirp))
//     {
//     }

//     close(dirp)
// }

int build_vnode(const std::string &vpath, vnode_map::iterator &iter)
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
                // const int fd = open(seed_path.c_str(), O_RDONLY);
                // if (fd > 0)
                // {
                //     // TODO: Lock the file.
                //     // TODO: Map the seed file to memory (mmap)
                // }
            }
        }
    }

    // Attempt vnode build up using log records.
    {
        // Return immediately if there are no log records.
        if (scan_upto == 0)
            goto success;

        // Return immediately if the virtual node is already fully updated.
        off_t next_log_offset = (iter == vnodes.end()) ? lh.first_record : iter->second.scanned_upto;
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
                    struct stat st;
                    add_vnode(vpath, st, next_log_offset, iter);
                }

                std::vector<uint8_t> payload;
                if (logger::read_payload(payload, record) == -1 ||
                    apply_log_record_to_vnode(iter->second, record, payload) == -1)
                    goto failure;
            }

        } while (next_log_offset > 0 && next_log_offset < scan_upto);
    }

success:
    return logger::release_lock(scan_lock);

failure:
    logger::release_lock(scan_lock);
    return -1;
}

int get_vnode(const char *vpath, vfs_node **vnode)
{
    vnode_map::iterator iter = vnodes.end();
    if (build_vnode(vpath, iter) == -1)
        return -1;

    *vnode = iter == vnodes.end() ? NULL : &(iter->second);
    return 0;
}

int apply_log_record_to_vnode(vfs_node &vnode, const logger::log_record &record,
                              const std::vector<uint8_t> payload)
{
    switch (record.operation)
    {
    case logger::FS_OPERATION::MKDIR:
        vnode.st.st_mode = *(mode_t *)payload.data();
        break;

    default:
        break;
    }
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

    return logger::append_log(vpath, logger::FS_OPERATION::MKDIR, {&mode, sizeof(mode)});
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

    return logger::append_log(vpath, logger::FS_OPERATION::CREATE, {&mode, sizeof(mode)});
}

int read(const char *vpath, char *buf, size_t size, off_t offset)
{
}

int write(const char *vpath, const char *buf, size_t size, off_t offset)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    // Payload: [size][offset][buf]
    uint8_t header_buf[sizeof(size_t) + sizeof(off_t)];
    *header_buf = size;
    *(header_buf + sizeof(size_t)) = offset;

    std::vector<iovec> payload_bufs;
    payload_bufs.push_back({header_buf, sizeof(header_buf)});
    payload_bufs.push_back({(void *)buf, size});

    return logger::append_log(vpath, logger::FS_OPERATION::WRITE, payload_bufs);
}

} // namespace vfs