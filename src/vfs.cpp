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
#include <optional>
#include "vfs.hpp"
#include "hpfs.hpp"
#include "audit.hpp"
#include "util.hpp"

namespace hpfs::vfs
{
    std::optional<virtual_filesystem> virtual_filesystem::create()
    {
        std::optional<hpfs::audit::audit_logger> logger = hpfs::audit::audit_logger::create();
        if (logger.has_value())
        {
            virtual_filesystem virt_fs(std::move(logger.value()));
            if (virt_fs.init() != -1)
                return std::optional<virtual_filesystem>(std::move(virt_fs));
        }

        return std::optional<virtual_filesystem>();
    }

    virtual_filesystem::virtual_filesystem(hpfs::audit::audit_logger &&logger) : logger(logger)
    {
    }

    int virtual_filesystem::init()
    {
        // In ReadOnly session, remember the last checkpoint record offset during initialisation.
        if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO)
            last_checkpoint = logger.get_header().last_checkpoint;

        stat(hpfs::ctx.seed_dir.c_str(), &default_stat);
        default_stat.st_nlink = 0;
        default_stat.st_size = 0;
        default_stat.st_mode ^= S_IFDIR; // Negate the entry type.

        // We always add the root ("/") as a very first entry in the vfs so
        // it always have its inode number as 1.
        vnode_map::iterator iter;
        if (add_vnode_from_seed("/", iter) == -1 || build_vfs() == -1)
            return -1;

        initialized = true;
        return 0;
    }

    int virtual_filesystem::get_vnode(const std::string &vpath_ori, vnode **vn)
    {
        std::string vpath;
        if (vpath_ori.front() == '/' && vpath_ori.find_first_not_of('/') == std::string::npos)
            vpath = "/";
        else
            vpath = vpath_ori;

        vnode_map::iterator iter = vnodes.find(vpath);
        if (iter == vnodes.end() &&
            loaded_vpaths.count(vpath) == 0 &&
            add_vnode_from_seed(vpath, iter) == -1)
            return -1;

        *vn = (iter == vnodes.end()) ? NULL : &iter->second;
        loaded_vpaths.emplace(vpath);

        return 0;
    }

    void virtual_filesystem::add_vnode(const std::string &vpath, vnode_map::iterator &vnode_iter)
    {
        vnode vn;
        vn.st = default_stat;
        vn.st.st_ino = vn.ino = next_ino++;

        auto [iter, success] = vnodes.try_emplace(vpath, std::move(vn));
        vnode_iter = iter;
        loaded_vpaths.emplace(vpath);
    }

    int virtual_filesystem::add_vnode_from_seed(const std::string &vpath, vnode_map::iterator &vnode_iter)
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
                vn.max_size = vn.st.st_size;
            }

            if (update_vnode_mmap(vn) == -1)
            {
                if (vn.seed_fd > 0)
                    close(vn.seed_fd);
                return -1;
            }

            auto [iter, success] = vnodes.try_emplace(vpath, std::move(vn));
            vnode_iter = iter;
            loaded_vpaths.emplace(vpath);
        }

        return 0;
    }

    int virtual_filesystem::build_vfs()
    {
        // Return immediately if we have already reached last checkpoint in ReadOnly mode.
        if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO && log_scanned_upto >= last_checkpoint)
            return 0;

        // Scan log records and build up vnodes relevant to log records.
        off_t next_log_offset = log_scanned_upto;

        do
        {
            hpfs::audit::log_record record;
            if (logger.read_log_at(next_log_offset, next_log_offset, record) == -1)
                return -1;

            if (next_log_offset == -1) // No log record was read. We are at end of log.
                break;

            std::vector<uint8_t> payload;
            if (logger.read_payload(payload, record) == -1 ||
                apply_log_record(record, payload) == -1)
                return -1;

            log_scanned_upto = record.offset + record.size;

        } while (next_log_offset > 0 &&
                 (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW || log_scanned_upto < last_checkpoint));

        return 0;
    }

    int virtual_filesystem::apply_log_record(const hpfs::audit::log_record &record, const std::vector<uint8_t> payload)
    {
        vnode_map::iterator iter = vnodes.find(record.vpath);
        if (iter == vnodes.end())
        {

            if (record.operation != hpfs::audit::FS_OPERATION::MKDIR &&
                record.operation != hpfs::audit::FS_OPERATION::CREATE)
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
        case hpfs::audit::FS_OPERATION::MKDIR:
            vn.st.st_mode = S_IFDIR | *(mode_t *)payload.data();
            break;

        case hpfs::audit::FS_OPERATION::RMDIR:
            delete_vnode(iter);
            break;

        case hpfs::audit::FS_OPERATION::RENAME:
        {
            const char *to_vpath = (char *)payload.data();

            // If "to" path already exists, delete it.
            vnode_map::iterator ex_iter = vnodes.find(to_vpath);
            if (ex_iter != vnodes.end() && delete_vnode(ex_iter) == -1)
                return -1;

            // Rename this vnode. (erase it from the list and insert under new name)
            vnode vn2 = vn;     // Create a copy before erase.
            vnodes.erase(iter); //
            auto [iter2, success] = vnodes.try_emplace(to_vpath, std::move(vn2));
            iter = iter2;

            break;
        }

        case hpfs::audit::FS_OPERATION::UNLINK:
            delete_vnode(iter);
            break;

        case hpfs::audit::FS_OPERATION::CREATE:
            vn.st.st_mode = S_IFREG | *(mode_t *)payload.data();
            break;

        case hpfs::audit::FS_OPERATION::WRITE:
        {
            const hpfs::audit::op_write_payload_header wh = *(hpfs::audit::op_write_payload_header *)payload.data();

            if (record.block_data_len > 0)
                vn.data_segs.push_back(vdata_segment{logger.get_fd(), record.block_data_len,
                                                     record.block_data_offset, wh.mmap_block_offset});

            // Update stats, if the new data boundry is larger than current file size.
            if (vn.st.st_size < (wh.offset + wh.size))
            {
                vn.st.st_size = wh.offset + wh.size;
                if (vn.st.st_size > vn.max_size)
                    vn.max_size = vn.st.st_size;
            }

            if (update_vnode_mmap(vn) == -1)
                return -1;

            break;
        }

        case hpfs::audit::FS_OPERATION::TRUNCATE:
        {
            const hpfs::audit::op_truncate_payload_header th = *(hpfs::audit::op_truncate_payload_header *)payload.data();

            if (record.block_data_len > 0)
                vn.data_segs.push_back(vdata_segment{logger.get_fd(), record.block_data_len,
                                                     record.block_data_offset, th.mmap_block_offset});

            vn.st.st_size = th.size;
            if (vn.st.st_size > vn.max_size)
                vn.max_size = vn.st.st_size;

            if (update_vnode_mmap(vn) == -1)
                return -1;

            break;
        }
        }

        if (iter != vnodes.end())
            vn.st.st_ino = iter->second.ino;

        return 0;
    }

    int virtual_filesystem::delete_vnode(vnode_map::iterator &vnode_iter)
    {
        vnode &vn = vnode_iter->second;

        if (vn.mmap.ptr && munmap(vn.mmap.ptr, vn.mmap.size) == -1)
            return -1;

        vnodes.erase(vnode_iter);
        vnode_iter = vnodes.end();
        return 0;
    }

    int virtual_filesystem::update_vnode_mmap(vnode &vn)
    {
        if (vn.mapped_data_segs == vn.data_segs.size())
            return 0;

        const off_t required_map_size = util::get_block_end(vn.max_size);

        if (vn.mmap.ptr && vn.mmap.size < required_map_size)
        {
            if (munmap(vn.mmap.ptr, vn.mmap.size) == -1)
                return -1;

            vn.mmap.ptr = NULL;
            vn.mapped_data_segs = 0;
        }

        for (uint32_t idx = vn.mapped_data_segs; idx < vn.data_segs.size(); idx++)
        {
            const vdata_segment &seg = vn.data_segs.at(idx);

            if (!vn.mmap.ptr)
            {
                // Create the mapping for the full size needed.
                void *ptr = mmap(NULL, required_map_size, PROT_READ, MAP_PRIVATE, seg.physical_fd, seg.physical_offset);
                if (ptr == MAP_FAILED)
                    return -1;

                vn.mmap.ptr = ptr;
                vn.mmap.size = required_map_size;
            }
            else
            {
                void *ptr = mmap(((uint8_t *)vn.mmap.ptr + seg.logical_offset),
                                 seg.size, PROT_READ, MAP_PRIVATE | MAP_FIXED,
                                 seg.physical_fd, seg.physical_offset);

                if (ptr == MAP_FAILED)
                    return -1;
            }
        }

        vn.mapped_data_segs = vn.data_segs.size();
        return 0;
    }

    int virtual_filesystem::get_dir_children(const char *vpath, vdir_children_map &children)
    {
        std::unordered_set<std::string> possible_child_names;

        {
            // Read possible children from seed dir;
            const std::string seed_path = std::string(hpfs::ctx.seed_dir).append(vpath);
            DIR *dirp = opendir(seed_path.c_str());
            if (dirp != NULL)
            {
                dirent *entry;
                while (entry = readdir(dirp))
                {
                    if (strcmp(entry->d_name, ".") != 0 &&
                        strcmp(entry->d_name, "..") != 0 &&
                        strcmp(entry->d_name, "/") != 0)
                        possible_child_names.emplace(entry->d_name);
                }

                closedir(dirp);
            }
        }

        {
            // Find possible children from vnodes.
            for (const auto &[vn_path, vn] : vnodes)
            {
                if (vn_path == "/")
                    continue;

                char *path2 = strdup(vn_path.c_str());
                char *parent_path = dirname(path2);
                if (strcmp(parent_path, vpath) == 0)
                {
                    char *path3 = strdup(vn_path.c_str());
                    char *child_name = basename(path3);
                    possible_child_names.emplace(child_name);
                }
            }
        }

        for (const auto &child_name : possible_child_names)
        {
            std::string child_vpath = std::string(vpath);
            if (child_vpath.back() != '/')
                child_vpath.append("/");
            child_vpath.append(child_name);

            vnode *child_vnode;
            if (get_vnode(child_vpath.c_str(), &child_vnode) == -1)
                return -1;

            if (child_vnode)
            {
                children.try_emplace(child_name, child_vnode->st);
            }
        }

        return 0;
    }

    void virtual_filesystem::populate_block_buf_segs(std::vector<iovec> &block_buf_segs,
                                                     off_t &block_buf_start, off_t &block_buf_end,
                                                     const char *buf, const size_t wr_size,
                                                     const off_t wr_start, const size_t fsize, uint8_t *mmap_ptr)
    {
        const size_t wr_end = wr_start + wr_size;

        // Find the target file block offset range that should map to memory mapped file.
        block_buf_start = util::get_block_start(MIN(wr_start, fsize));
        block_buf_end = util::get_block_end(wr_start + wr_size);
        const size_t block_buf_size = block_buf_end - block_buf_start;

        // If write offset is after block start.
        if (block_buf_start < wr_start)
        {
            // If block start offset is before file end, add a segment containing existing
            // file data between block start and write offset.
            if (block_buf_start < fsize)
            {
                const size_t ex_data_len = MIN(fsize, wr_start) - block_buf_start;
                block_buf_segs.push_back({mmap_ptr + block_buf_start, ex_data_len});
            }

            // If write offset is beyond file size, add a segment for NULL bytes
            // between file end and write offset.
            if (fsize < wr_start)
                block_buf_segs.push_back({NULL, (wr_start - fsize)});
        }

        // Add segment for write buf.
        if (wr_size > 0)
            block_buf_segs.push_back({(void *)buf, wr_size});

        // If write end offset is before the block end.
        if (wr_end < block_buf_end)
        {
            // If write end offset is before file end, add a segment containing existing
            // file data after write end upto block end.
            if (wr_end < fsize)
                block_buf_segs.push_back({(mmap_ptr + wr_end), (MIN(fsize, block_buf_end) - wr_end)});

            // Append segment for NULL data until block end.
            const off_t null_data_start = MAX(wr_end, fsize);
            if (null_data_start < block_buf_end)
                block_buf_segs.push_back({NULL, (size_t)(block_buf_end - null_data_start)});
        }
    }

    virtual_filesystem::~virtual_filesystem()
    {
        if (initialized)
        {
            for (const auto &[vpath, vnode] : vnodes)
            {
                if (vnode.seed_fd > 0)
                    close(vnode.seed_fd);

                if (vnode.mmap.ptr)
                    munmap(vnode.mmap.ptr, vnode.mmap.size);
            }
        }
    }

} // namespace hpfs::vfs