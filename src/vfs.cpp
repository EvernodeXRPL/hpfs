#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <unordered_map>
#include <iostream>
#include "vfs.hpp"
#include "hpfs.hpp"
#include "logger.hpp"

namespace vfs
{

ino_t next_ino = 1;
vnode_map vnodes;

int init()
{
    struct stat st;
    stat(hpfs::ctx.seed_dir.c_str(), &st);

    vnode_map::iterator iter;
    add_vnode("/", st, 0, iter);
}

int getattr(const char *path, struct stat *stbuf)
{
    vnode_map::iterator iter;
    if (playback_vnode(path, iter) == -1)
        return -1;

    if (iter == vnodes.end())
        return -ENOENT;

    *stbuf = (iter->second).st;

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

int add_vnode(const std::string &path, struct stat &st,
              const off_t log_offset, vnode_map::iterator &vnode_iter)
{
    st.st_ino = next_ino++;
    std::cout << "Added inode " << st.st_ino << "\n";
    struct vfs_node vnode = {st, log_offset};
    const auto [iter, success] = vnodes.try_emplace(path, std::move(vnode));
    vnode_iter = iter;
    return 0;
}

int playback_vnode(const std::string &path, vnode_map::iterator &vnode_iter)
{
    struct stat vnode_st;
    memset(&vnode_st, 0, sizeof(struct stat));
    off_t vnode_log_offset = 0;

    // Check whether we are already tracking this path.
    vnode_iter = vnodes.find(path);
    if (vnode_iter != vnodes.end())
    {
        vnode_st = vnode_iter->second.st;
        vnode_log_offset = vnode_iter->second.log_offset;
    }

    if (vnode_iter == vnodes.end())
    {
        // Attempt to construct virtual node from within seed directory (if exist).
        const std::string seed_path = std::string(hpfs::ctx.seed_dir).append(path);

        struct stat st;
        const int res = stat(seed_path.c_str(), &st);
        if (res == -1 && errno != ENOENT)
            return -1;

        if (res != -1)
        {
            // Seed file/dir exists. So we must add a virtual node.
            add_vnode(path, st, 0, vnode_iter);

            if (S_ISDIR(st.st_mode)) // is dir
            {
                // Add any directory entries from seed dir.
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

    // Return if the virtual node is already fully updated.
    const off_t scan_upto = hpfs::ctx.run_mode == hpfs::RUN_MODE::RO ? logger::get_last_checkpoint_offset() : logger::get_last_offset();
    if (scan_upto == 0 || vnode_log_offset > scan_upto)
        return 0;

    // Loop through any unapplied log records and apply any changes to the virtual node.
    off_t log_offset = vnode_log_offset;
    bool found_vnode_updates = false;

    do
    {
        logger::log_record record;
        if (logger::read_log_at(log_offset, log_offset, record) == -1)
            break;

        if (record.path == path)
        {
            found_vnode_updates = true;
            // TODO: Apply log record change to virtual node.
        }

        vnode_log_offset = record.offset + record.size;

    } while (log_offset > 0 && log_offset <= scan_upto);

    // Update/create the vnode with latest information.
    if (vnode_iter != vnodes.end())
    {
        vnode_iter->second.st = vnode_st;
        vnode_iter->second.log_offset = vnode_log_offset;
    }
    else if (found_vnode_updates)
    {
        add_vnode(path, vnode_st, vnode_log_offset, vnode_iter);
    }
}

} // namespace vfs