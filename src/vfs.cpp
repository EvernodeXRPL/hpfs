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

ino_t next_ino = 1;
vnode_map vnodes;
struct stat default_stat;

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

    stat(hpfs::ctx.seed_dir.c_str(), &default_stat);
    default_stat.st_nlink = 0;
    default_stat.st_size = 0;
    default_stat.st_mode ^= S_IFDIR;

    if (build_vfs() == -1)
        return -1;
}

void deinit()
{
    for (const auto &[vpath, vnode] : vnodes)
    {
        if (vnode.seed_fd > 0)
            close(vnode.seed_fd);
    }
}

int get(const std::string &vpath, vnode **vn)
{
    vnode_map::iterator iter = vnodes.find(vpath);
    if (iter == vnodes.end() && add_vnode_from_seed(vpath, iter) == -1)
        return -1;

    *vn = (iter == vnodes.end()) ? NULL : &iter->second;

    return 0;
}

void add_vnode(const std::string &vpath, vnode_map::iterator &vnode_iter)
{
    vnode vn;
    vn.st = default_stat;
    vn.st.st_ino = vn.ino = next_ino++;

    auto [iter, success] = vnodes.try_emplace(vpath, std::move(vn));
    vnode_iter = iter;
}

int add_vnode_from_seed(const std::string &vpath, vnode_map::iterator &vnode_iter)
{
    const std::string seed_path = std::string(hpfs::ctx.seed_dir).append(vpath);

    struct stat st;
    const int res = stat(seed_path.c_str(), &st);
    if (res == -1 && errno != ENOENT)
        return -1;

    if (res != -1) // Seed file/dir exists. So we must initialize the virtual node.
    {
        vnode vn;
        vn.st = st;
        vn.st.st_ino = vn.ino = next_ino++;

        if (S_ISREG(st.st_mode)) // is file.
        {
            const int fd = open(seed_path.c_str(), O_RDONLY);
            if (fd == -1)
                return -1;
            if (vn.st.st_size > 0)
                vn.data_segs.push_back(vdata_segment{fd, (size_t)vn.st.st_size, 0, 0});

            vn.seed_fd = fd;
        }

        auto [iter, success] = vnodes.try_emplace(vpath, std::move(vn));
        vnode_iter = iter;
    }

    return 0;
}

int build_vfs()
{
    // Scan log records and build up vnodes relevant to log records.
    off_t next_log_offset = 0;

    do
    {
        logger::log_record record;
        if (logger::read_log_at(next_log_offset, next_log_offset, record) == -1)
            return -1;

        if (next_log_offset == -1) // No log record was read.
            break;

        std::vector<uint8_t> payload;
        if (logger::read_payload(payload, record) == -1 ||
            apply_log_record(record, payload) == -1)
            return -1;

    } while (next_log_offset > 0);

    return 0;
}

int apply_log_record(const logger::log_record &record, const std::vector<uint8_t> payload)
{
    vnode_map::iterator iter = vnodes.find(record.vpath);
    if (iter == vnodes.end())
    {

        if (record.operation != logger::FS_OPERATION::MKDIR &&
            record.operation != logger::FS_OPERATION::CREATE)
        {
            if (add_vnode_from_seed(record.vpath, iter) == -1 || iter == vnodes.end())
                return -1;
        }
        else
        {
            add_vnode(record.vpath, iter);
        }
    }

    // When we reach this point we are guaranteed that a vnode is there.
    vnode &vn = iter->second;

    switch (record.operation)
    {
    case logger::FS_OPERATION::MKDIR:
        vn.st.st_mode = S_IFDIR | *(mode_t *)payload.data();
        break;
    case logger::FS_OPERATION::RMDIR:
        delete_vnode(iter);
        break;
    }

    if (iter != vnodes.end())
        vn.st.st_ino = iter->second.ino;

    return 0;
}

int delete_vnode(vnode_map::iterator &vnode_iter)
{
    vnode &vn = vnode_iter->second;

    if (vn.mmap.ptr && munmap(vn.mmap.ptr, vn.mmap.size) == -1)
        return -1;

    vnodes.erase(vnode_iter);
    vnode_iter = vnodes.end();
    return 0;
}

} // namespace vfs