#ifndef _HPFS_VFS_
#define _HPFS_VFS_

#include <sys/types.h>
#include <sys/stat.h>
#include <unordered_map>
#include <vector>
#include <optional>
#include "audit.hpp"

namespace hpfs::vfs
{
    struct vnode_mmap
    {
        void *ptr = NULL;
        size_t size = 0;
    };

    struct vdata_segment
    {
        int physical_fd = 0;
        size_t size = 0;
        off_t physical_offset = 0;
        off_t logical_offset = 0;
    };

    struct vnode
    {
        ino_t ino = 0;
        struct stat st;
        int seed_fd = 0;

        // How many data segs from the begining of list that has been mapped to memory.
        uint32_t mapped_data_segs = 0;
        std::vector<vdata_segment> data_segs;
        struct vnode_mmap mmap;

        // Max file size that has been there for this vnode throughout the log history.
        size_t max_size = 0;

        vnode()
        {
            memset(&st, 0, sizeof(struct stat));
            memset(&mmap, 0, sizeof(struct vnode_mmap));
        }
    };

    typedef std::unordered_map<std::string, vnode> vnode_map;
    typedef std::unordered_map<std::string, struct stat> vdir_children_map;

    class virtual_filesystem
    {
    private:
        bool initialized = false; // Indicates that the instance has been initialized properly.
        ino_t next_ino = 1;
        vnode_map vnodes;
        struct stat default_stat;
        std::unordered_set<std::string> loaded_vpaths;
        hpfs::audit::audit_logger logger;

        // Last checkpoint offset for the use of ReadOnly session
        // (inclusive of the checkpointed log record).
        off_t last_checkpoint = 0;

        // Log offset that has been scanned for vfs buildup
        // (inclusive of log record).
        off_t log_scanned_upto = 0;

        virtual_filesystem(hpfs::audit::audit_logger &&logger);
        int init();

    public:
        static std::optional<virtual_filesystem> create();
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