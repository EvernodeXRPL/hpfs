#ifndef _HPFS_HMAP_STORE_
#define _HPFS_HMAP_STORE_

#include <string>
#include <vector>
#include "hasher.hpp"

namespace hmap::store
{
    struct vnode_hmap
    {
        bool is_file;
        hasher::h32 node_hash;                 // Overall hash of this vnode.
        hasher::h32 vpath_hash;                // Vpath hash.
        std::vector<hasher::h32> block_hashes; // Only relevant for files.
    };

    void set_dirty(const std::string &vpath);
    vnode_hmap *find_hash_map(const std::string &vpath);
    void erase_hash_map(const std::string &vpath);
    void insert_hash_map(const std::string &vpath, vnode_hmap &&node_hmap);
    int persist_hash_maps();
    int persist_hash_map_cache_file(const vnode_hmap &node_hmap, const std::string &filename);
    std::string get_vpath_hash_name(const std::string &vpath);
} // namespace hmap::store

#endif