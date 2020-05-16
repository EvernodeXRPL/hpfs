#ifndef _HPFS_VFS_
#define _HPFS_VFS_

#include <sys/types.h>
#include <sys/stat.h>
#include <unordered_map>
#include <vector>
#include "logger.hpp"

namespace vfs
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
    };

    typedef std::unordered_map<std::string, vnode> vnode_map;
    typedef std::unordered_map<std::string, struct stat> vdir_children_map;

    int init();
    void deinit();
    int get_vnode(const std::string &vpath, vnode **vn);
    void add_vnode(const std::string &vpath, vnode_map::iterator &vnode_iter);
    int add_vnode_from_seed(const std::string &vpath, vnode_map::iterator &vnode_iter);
    int build_vfs();
    int apply_log_record(const logger::log_record &record, const std::vector<uint8_t> payload);
    int delete_vnode(vnode_map::iterator &vnode_iter);
    int update_vnode_mmap(vnode &vn);
    int get_dir_children(const char *vpath, vdir_children_map &children);
    void populate_block_buf_segs(std::vector<iovec> &block_buf_segs,
                                 off_t &block_buf_start, off_t &block_buf_end,
                                 const char *buf, const size_t wr_size,
                                 const off_t wr_start, const size_t fsize, uint8_t *mmap_ptr);

} // namespace vfs

#endif