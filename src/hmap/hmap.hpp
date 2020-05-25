#ifndef _HPFS_HMAP_HMAP_
#define _HPFS_HMAP_HMAP_

#include <string>
#include <vector>
#include "h32.hpp"

namespace hmap
{
    struct vnode_hmap
    {
        bool is_file;
        h32 hash;                      // Overall hash of this vnode.
        std::vector<h32> block_hashes; // Only relevant for files.
    };

    int init();
    void deinit();
    int read_hmap_file();
    int calculate_dir_hash(h32 &hash, const std::string &vpath);
    int calculate_file_hash(h32 &hash, const std::string &vpath);
    void propogate_hash_update(const std::string &vpath, const h32 &old_hash, const h32 &new_hash);
    int add_new_vnode_hmap(const std::string &vpath, const bool is_file);
    int update_block_hashes(const std::string &vpath, const off_t update_offset, const size_t update_size);

} // namespace hmap

#endif