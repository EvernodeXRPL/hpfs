#ifndef _HPFS_HMAP_HMAP_
#define _HPFS_HMAP_HMAP_

#include <string>
#include <vector>
#include "h32.hpp"
#include "../vfs.hpp"

namespace hmap
{
    struct vnode_hmap
    {
        bool is_file;
        h32 node_hash;                 // Overall hash of this vnode.
        h32 vpath_hash;                // Vpath hash.
        std::vector<h32> block_hashes; // Only relevant for files.
    };

    int init();
    void deinit();
    int read_hmap_file();
    int calculate_dir_hash(h32 &node_hash, const std::string &vpath);
    int calculate_file_hash(h32 &node_hash, const std::string &vpath);
    void propogate_hash_update(const std::string &vpath, const h32 &old_hash, const h32 &new_hash);
    int apply_vnode_create(const std::string &vpath);
    int apply_vnode_update(const std::string &vpath, const vfs::vnode &vn,
                           const off_t file_update_offset, const size_t file_update_size);
    int apply_file_data_update(vnode_hmap &node_hmap, const vfs::vnode &vn,
                               const off_t update_offset, const size_t update_size);
    int apply_vnode_delete(const std::string &vpath);
    int apply_vnode_rename(const std::string &from_vpath, const std::string &to_vpath);

} // namespace hmap

#endif