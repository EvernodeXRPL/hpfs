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
off_t last_checkpoint = 0; // Last checkpoint for the use of ReadOnly session.

int init()
{
    // In ReadOnly session, remember the last checkpoint offset during initialisation.
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

int getattr(const char *path, struct stat *stbuf)
{
    vnode_map::iterator iter = vnodes.end();
    if (build_vnode(path, iter) == -1)
        return -1;

    if (iter == vnodes.end())
        return -ENOENT;

    *stbuf = (iter->second).st;

    return 0;
}

int mkdir(const char *path, mode_t mode)
{
    // Check of vnode for same path already exists.
    vnode_map::iterator iter = vnodes.end();
    if (build_vnode(path, iter) == -1)
        return -1;
    if (iter != vnodes.end())
        return -EEXIST;

    struct stat st;
    if (add_vnode(path, st, logger::get_eof_offset(), iter) == -1)
        return -1;

    if (build_vnode(path, iter) == -1)
        return -1;

    return 0;
}

int create(const char *path, mode_t mode)
{
    return 0;
}

int read(const char *path, char *buf, size_t size, off_t offset)
{
}

int write(const char *path, const char *buf, size_t size, off_t offset)
{
    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW)
    {
        // Payload: [size][offset][buf]
        uint8_t header_buf[sizeof(size_t) + sizeof(off_t)];
        *header_buf = size;
        *(header_buf + sizeof(size_t)) = offset;

        iovec bufs[2];
        bufs[0].iov_base = header_buf;
        bufs[0].iov_len = sizeof(header_buf);
        bufs[1].iov_base = (void *)buf;
        bufs[1].iov_len = size;

        return logger::append_log(path, logger::FS_OPERATION::WRITE, {bufs, 2});
    }
    return -1;
}

int apply_log_record_to_vnode(vfs_node &vnode, const logger::log_record &record, std::string_view payload)
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

int add_vnode(const std::string &path, struct stat &st,
              const off_t log_offset, vnode_map::iterator &vnode_iter)
{
    st.st_ino = next_ino++;
    struct vfs_node vnode = {st, log_offset, false};
    const auto [iter, success] = vnodes.try_emplace(path, std::move(vnode));
    vnode_iter = iter;
    return 0;
}

int initialize_vnode_from_seed_dir(const std::string &seed_path, vfs_node &vnode)
{
    DIR *dirp = opendir(seed_path.c_str());
    if (dirp == NULL)
        return -1;

    dirent *dent = readdir(dirp);
}

int build_vnode(const std::string &path, vnode_map::iterator &iter)
{
    // Read the current log header with read-lock.
    flock log_header_lock;
    logger::log_header lh;
    if (logger::set_lock(log_header_lock, false, 0, sizeof(lh)) == -1 ||
        logger::read_header(lh) == -1 ||
        logger::release_lock(log_header_lock) == -1)
        return -1;

    // Calculate the offset upto which we should scan to build this vnode.
    const off_t scan_upto = (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO) ? last_checkpoint : lh.last_record;

    // Read-lock the log file offset range that we are going to scan.
    flock scan_lock;
    if (logger::set_lock(scan_lock, false, lh.first_record, scan_upto + 1) == -1)
        return -1;

    if (iter == vnodes.end())
        iter = vnodes.find(path); // Check whether we are already tracking this path.

    // Attempt to construct virtual node from within seed directory (if exist).
    if (iter == vnodes.end())
    {
        const std::string seed_path = std::string(hpfs::ctx.seed_dir).append(path);

        struct stat st;
        const int res = stat(seed_path.c_str(), &st);
        if (res == -1 && errno != ENOENT)
            goto failure;

        if (res != -1)
        {
            // Seed file/dir exists. So we must add a virtual node.
            add_vnode(path, st, 0, iter);

            if (S_ISDIR(st.st_mode)) // is dir
            {
                if (initialize_vnode_from_seed_dir(seed_path, iter->second) == -1)
                    goto failure;
            }
            else // is file
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
        // Return imeediately if the virtual node is already fully updated.
        off_t next_log_offset = (iter == vnodes.end()) ? 0 : iter->second.log_offset;
        if (scan_upto == 0 || next_log_offset > scan_upto)
            goto success;

        // Loop through any unapplied log records and apply any changes to the virtual node.
        do
        {
            logger::log_record record;
            if (logger::read_log_at(next_log_offset, next_log_offset, record) == -1)
                break;

            if (record.path == path)
            {
                if (iter == vnodes.end())
                {
                    struct stat st;
                    add_vnode(path, st, next_log_offset, iter);
                }

                std::string payload;
                if (logger::read_payload(payload, record) == -1)
                    goto failure;
                if (apply_log_record_to_vnode(iter->second, record, payload) == -1)
                    goto failure;
            }

        } while (next_log_offset > 0 && next_log_offset <= scan_upto);
    }

failure:
    logger::release_lock(scan_lock);
    return -1;
success:
    return logger::release_lock(scan_lock);
}

} // namespace vfs