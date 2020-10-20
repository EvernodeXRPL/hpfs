#ifndef _HPFS_VFS_VIRTUAL_FILESYSTEM_
#define _HPFS_VFS_VIRTUAL_FILESYSTEM_

#include <unordered_map>
#include <unordered_set>
#include "vfs.hpp"
#include "../audit.hpp"

namespace hpfs::vfs
{
    typedef std::unordered_map<std::string, vnode> vnode_map;
    typedef std::unordered_map<std::string, struct stat> vdir_children_map;

    class virtual_filesystem
    {
    private:
        bool initialized = false; // Indicates that the instance has been initialized properly.
        const bool readonly;
        std::string_view seed_dir;
        ino_t next_ino = 1;
        vnode_map vnodes;
        struct stat default_stat;
        std::unordered_set<std::string> loaded_vpaths;
        hpfs::audit::audit_logger &logger;

        // Last checkpoint offset for the use of ReadOnly session
        // (inclusive of the checkpointed log record).
        off_t last_checkpoint = 0;

        // Log offset that has been scanned for vfs buildup
        // (inclusive of log record).
        off_t log_scanned_upto = 0;

        virtual_filesystem(const bool readonly, std::string_view seed_dir, hpfs::audit::audit_logger &logger);
        int init();

    public:
        static std::optional<virtual_filesystem> create(const bool readonly, std::string_view seed_dir,
                                                        hpfs::audit::audit_logger &logger);
        int get_vnode(const std::string &vpath, vnode **vn);
        void add_vnode(const std::string &vpath, vnode_map::iterator &vnode_iter);
        int add_vnode_from_seed(const std::string &vpath, vnode_map::iterator &vnode_iter);
        int build_vfs();
        int apply_log_record(const hpfs::audit::log_record &record, const std::vector<uint8_t> payload);
        int delete_vnode(vnode_map::iterator &vnode_iter);
        int update_vnode_mmap(vnode &vn);
        int get_dir_children(const char *vpath, vdir_children_map &children);
        void populate_block_buf_segs(std::vector<iovec> &block_buf_segs,
                                     off_t &block_buf_start, off_t &block_buf_end,
                                     const char *buf, const size_t wr_size,
                                     const off_t wr_start, const size_t fsize, uint8_t *mmap_ptr);
        ~virtual_filesystem();
    };

} // namespace hpfs::vfs

#endif