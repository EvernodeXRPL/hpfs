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
#include "virtual_filesystem.hpp"
#include "../inodes.hpp"
#include "../util.hpp"
#include "../tracelog.hpp"

namespace hpfs::vfs
{

    int virtual_filesystem::create(std::optional<virtual_filesystem> &virt_fs, const bool readonly, std::string_view seed_dir,
                                   hpfs::audit::audit_logger &logger)
    {
        virt_fs.emplace(readonly, seed_dir, logger);
        if (virt_fs->init() == -1)
        {
            virt_fs.reset();
            return -1;
        }

        return 0;
    }

    virtual_filesystem::virtual_filesystem(const bool readonly,
                                           std::string_view seed_dir,
                                           hpfs::audit::audit_logger &logger) : readonly(readonly),
                                                                                seed_dir(seed_dir),
                                                                                seed_paths(seed_dir),
                                                                                logger(logger)
    {
    }

    int virtual_filesystem::init()
    {
        // In ReadOnly session, remember the last checkpoint record offset during initialisation.
        if (readonly)
            last_checkpoint = logger.get_header().last_checkpoint;

        // We always add the root ("/") as a very first entry in the vfs.
        vnode_map::iterator iter;
        if (add_vnode_from_seed("/", iter) == -1 || build_vfs() == -1)
        {
            LOG_ERROR << "Error in vfs init.";
            return -1;
        }

        initialized = true;
        LOG_DEBUG << "VFS init complete.";
        return 0;
    }

    int virtual_filesystem::get_vnode(const std::string &vpath_ori, vnode **vn)
    {
        std::scoped_lock lock(vnodes_mutex);

        const std::string &vpath = (vpath_ori.front() == '/' &&
                                    vpath_ori.find_first_not_of('/') == std::string::npos)
                                       ? "/"
                                       : vpath_ori;

        vnode_map::iterator iter = vnodes.find(vpath);
        if (iter == vnodes.end() && add_vnode_from_seed(vpath, iter) == -1)
        {
            LOG_ERROR << "Error in vfs vnode get.";
            return -1;
        }

        *vn = (iter == vnodes.end()) ? NULL : &iter->second;

        return 0;
    }

    void virtual_filesystem::add_vnode(const std::string &vpath, vnode_map::iterator &vnode_iter)
    {
        vnode vn;
        vn.st = ctx.default_stat;
        vn.st.st_ino = vn.ino = inodes::next();

        auto [iter, success] = vnodes.try_emplace(vpath, std::move(vn));
        vnode_iter = iter;
    }

    int virtual_filesystem::add_vnode_from_seed(const std::string &vpath, vnode_map::iterator &vnode_iter)
    {
        const std::string original_seed_path = seed_paths.resolve(vpath);
        if (original_seed_path.empty() || seed_paths.is_removed(original_seed_path) || seed_paths.is_renamed(original_seed_path))
            return 0;

        const std::string seed_path = std::string(seed_dir).append(original_seed_path);

        struct stat st;
        const int res = stat(seed_path.c_str(), &st);
        if (res == -1 && errno != ENOENT)
        {
            LOG_ERROR << errno << ": Error in stat of seed file." << seed_path;
            return -1;
        }

        if (res != -1) // Seed file/dir exists. So we must initialize the virtual node.
        {
            vnode vn;
            vn.st = st;
            vn.st.st_ino = vn.ino = inodes::next();

            if (S_ISREG(st.st_mode)) // is file.
            {
                const int fd = open(seed_path.c_str(), O_RDONLY);
                if (fd == -1)
                {
                    LOG_ERROR << errno << ": Error when opening seed file." << seed_path;
                    return -1;
                }
                if (vn.st.st_size > 0)
                    vn.data_segs.push_back(vdata_segment{fd, (size_t)vn.st.st_size, 0, 0});

                vn.seed_fd = fd;
                vn.max_size = vn.st.st_size;
            }

            if (update_vnode_mmap(vn) == -1)
            {
                if (vn.seed_fd > 0)
                    close(vn.seed_fd);

                LOG_ERROR << "Error when mmap update of seed file." << seed_path;
                return -1;
            }

            auto [iter, success] = vnodes.try_emplace(vpath, std::move(vn));
            vnode_iter = iter;
        }

        return 0;
    }

    /**
     * Playback any unread logs and build up the latest view of the virtual fs.
     * @return 0 on success. -1 on failure;
     */
    int virtual_filesystem::build_vfs()
    {
        // Return immediately if we have already reached last checkpoint in ReadOnly mode.
        if (readonly && log_scanned_upto >= last_checkpoint)
            return 0;

        // Scan log records and build up vnodes relevant to log records.
        off_t next_log_offset = log_scanned_upto;

        do
        {
            hpfs::audit::log_record record;
            if (logger.read_log_at(next_log_offset, next_log_offset, record) == -1)
            {
                LOG_ERROR << "Error in vfs read log.";
                return -1;
            }

            if (next_log_offset == -1) // No log record was read. We are at end of log.
                break;

            std::vector<uint8_t> payload;
            if (logger.read_payload(payload, record) == -1 ||
                apply_log_record(record, payload) == -1)
            {
                LOG_ERROR << "Error in vfs read and apply log.";
                return -1;
            }

            log_scanned_upto = record.offset + record.size;

        } while (next_log_offset > 0 &&
                 (!readonly || log_scanned_upto < last_checkpoint));

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
                {
                    LOG_ERROR << "Error in vfs adding vnode from seed in apply log record. " << record.vpath;
                    return -1;
                }
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
            seed_paths.remove(record.vpath, true);
            break;

        case hpfs::audit::FS_OPERATION::RENAME:
        {
            const std::string &from_vpath = record.vpath;
            const std::string &to_vpath = (char *)payload.data();

            // Apply seed path rename logic.
            seed_paths.rename(from_vpath, to_vpath, S_ISDIR(vn.st.st_mode));

            // Rename all vnode sub paths under this path. (Erase them and insert under new name)
            {
                std::vector<std::string> vpaths_to_move;
                for (const auto &[vpath, vnode] : vnodes)
                {
                    if (vpath.size() > from_vpath.size() && vpath.rfind(from_vpath, 0) == 0)
                        vpaths_to_move.push_back(vpath);
                }

                for (const std::string &vpath : vpaths_to_move)
                {
                    const auto move_iter = vnodes.find(vpath);
                    vnode move_vn = move_iter->second; // Create a copy.
                    vnodes.erase(move_iter);
                    const std::string new_path = to_vpath + vpath.substr(from_vpath.size());
                    vnodes.try_emplace(new_path, std::move(move_vn)); // Insert under new name.
                }
            }

            // Rename this vnode. (erase it from the list and insert under new name)
            vnode vn2 = vn; // Create a copy before erase.
            vnodes.erase(iter);
            auto [iter2, success] = vnodes.try_emplace(to_vpath, std::move(vn2));
            iter = iter2;

            break;
        }

        case hpfs::audit::FS_OPERATION::UNLINK:
            delete_vnode(iter);
            seed_paths.remove(record.vpath, false);
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
            {
                LOG_ERROR << "Error in vnode mmap update in apply log record (op:write).";
                return -1;
            }

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
            {
                LOG_ERROR << "Error in vnode mmap update in apply log record (op:trunc).";
                return -1;
            }

            break;
        }

        case hpfs::audit::FS_OPERATION::CHMOD:
            vn.st.st_mode = (S_ISREG(vn.st.st_mode) ? S_IFREG : S_IFDIR) | *(mode_t *)payload.data();
            break;
        }

        return 0;
    }

    int virtual_filesystem::delete_vnode(vnode_map::iterator &vnode_iter)
    {
        vnode &vn = vnode_iter->second;

        if (vn.mmap.ptr && munmap(vn.mmap.ptr, vn.mmap.size) == -1)
            return -1;

        if (vn.seed_fd > 0)
            close(vn.seed_fd);

        vnodes.erase(vnode_iter);
        vnode_iter = vnodes.end();
        return 0;
    }

    /**
     * Performs the necessary adjustments to the vfs and last data block memory map of the specified vnode to reflect the
     * indicated block size increment.
     */
    int virtual_filesystem::apply_last_write_log_adjustment(vfs::vnode &vn, const off_t wr_offset, const size_t wr_size,
                                                            const size_t block_size_increase)
    {
        if (block_size_increase > 0)
        {
            log_scanned_upto += block_size_increase; // Increase the log scanned marker to include the increased block bytes.

            if (vn.data_segs.empty())
            {
                LOG_ERROR << "No vnode data seg to extend.";
                return -1;
            }

            if (!vn.mmap.ptr)
            {
                LOG_ERROR << "No existing mmap to unmap and extend.";
                return -1;
            }

            vdata_segment &seg = vn.data_segs.back();

            // Unmap the last data segment before remapping it with the increased size.
            if (munmap((uint8_t *)vn.mmap.ptr + seg.logical_offset, seg.size) == -1)
            {
                LOG_ERROR << errno << ": Error in munmap when extending last data seg.";
                return -1;
            }
            vn.mapped_data_segs--;
            seg.size += block_size_increase; // Increase the last segment block size;
        }

        // Update vnode stats, if the new data boundry is larger than the previous file size.
        if (vn.st.st_size < (wr_offset + wr_size))
        {
            vn.st.st_size = wr_offset + wr_size;
            if (vn.st.st_size > vn.max_size)
                vn.max_size = vn.st.st_size;
        }

        // Trigger a mmap update with the new information.
        if (update_vnode_mmap(vn) == -1)
            return -1;

        return 0;
    }

    int virtual_filesystem::update_vnode_mmap(vnode &vn)
    {
        if (vn.mapped_data_segs == vn.data_segs.size())
            return 0;

        const off_t required_map_size = BLOCK_END(vn.max_size);

        if (vn.mmap.ptr && vn.mmap.size < required_map_size)
        {
            if (munmap(vn.mmap.ptr, vn.mmap.size) == -1)
            {
                LOG_ERROR << errno << ": Error in munmap.";
                return -1;
            }

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
                {
                    LOG_ERROR << errno << ": Error in vnode mmap creation.";
                    return -1;
                }

                vn.mmap.ptr = ptr;
                vn.mmap.size = required_map_size;
            }
            else
            {
                void *ptr = mmap(((uint8_t *)vn.mmap.ptr + seg.logical_offset),
                                 seg.size, PROT_READ, MAP_PRIVATE | MAP_FIXED,
                                 seg.physical_fd, seg.physical_offset);

                if (ptr == MAP_FAILED)
                {
                    LOG_ERROR << errno << ": Error in vnode mmap update.";
                    return -1;
                }
            }
        }

        vn.mapped_data_segs = vn.data_segs.size();
        return 0;
    }

    int virtual_filesystem::get_dir_children(const std::string &vpath, vdir_children_map &children)
    {
        std::unordered_set<std::string> possible_child_names;

        {
            // Read possible children from seed dir
            const std::string original_seed_path = seed_paths.resolve(vpath);
            if (!original_seed_path.empty())
            {
                const std::string seed_path = std::string(seed_dir).append(original_seed_path);
                DIR *dirp = opendir(seed_path.c_str());
                if (dirp != NULL)
                {
                    dirent *entry;
                    while (entry = readdir(dirp))
                    {
                        if (strcmp(entry->d_name, ".") != 0 &&
                            strcmp(entry->d_name, "..") != 0 &&
                            strcmp(entry->d_name, "/") != 0)
                        {
                            // Add only seed files and directories that haven't been renamed or deleted.
                            const std::string child_seed_path = original_seed_path + (original_seed_path.back() == '/' ? "" : "/") + entry->d_name;
                            if (!seed_paths.is_removed(child_seed_path) && !seed_paths.is_renamed(child_seed_path))
                                possible_child_names.emplace(entry->d_name);
                        }
                    }

                    closedir(dirp);
                }
            }
        }

        {
            // Find possible children from vnodes.
            for (const auto &[vn_path, vn] : vnodes)
            {
                if (vn_path == "/")
                    continue;

                const std::string parent_path = util::get_parent_path(vn_path);
                if (parent_path == vpath)
                    possible_child_names.emplace(util::get_name(vn_path));
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
            {
                LOG_ERROR << "Error in dir children child vnode get. " << child_vpath;
                return -1;
            }

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
        block_buf_start = BLOCK_START(MIN(wr_start, fsize));
        block_buf_end = BLOCK_END(wr_start + wr_size);
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
    /**
     * Cleanup the existing vfs and re-build the vfs again.
     * @return -1 on error and 0 on success.
    */
    int virtual_filesystem::re_build_vfs()
    {
        log_scanned_upto = 0;
        if (initialized && !moved)
        {
            for (const auto &[vpath, vnode] : vnodes)
            {
                if (vnode.seed_fd > 0)
                    close(vnode.seed_fd);

                if (vnode.mmap.ptr)
                    munmap(vnode.mmap.ptr, vnode.mmap.size);
            }
            vnodes.clear();

            vnode_map::iterator iter;
            if (add_vnode_from_seed("/", iter) == -1 || build_vfs() == -1)
            {
                LOG_ERROR << "Error in vfs init.";
                return -1;
            }
        }
        return 0;
    }

    virtual_filesystem::~virtual_filesystem()
    {
        if (initialized && !moved)
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