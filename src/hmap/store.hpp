#ifndef _HPFS_HMAP_STORE_
#define _HPFS_HMAP_STORE_

#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include "hasher.hpp"

namespace hpfs::hmap::store
{
    struct vnode_hmap
    {
        bool is_file;
        hasher::h32 node_hash;                 // Overall hash of this vnode.
        hasher::h32 name_hash;                 // Name hash.
        hasher::h32 meta_hash;                 // Metadata (mode) hash.
        std::vector<hasher::h32> block_hashes; // Only relevant for files.
    };

    class hmap_store
    {
    private:
        // Hash maps of vnodes keyed by the vpath.
        std::unordered_map<std::string, vnode_hmap> hash_map;

        // List of vpaths with modifications (including deletions) during the session.
        std::unordered_set<std::string> dirty_vpaths;
        int read_hash_map_cache_file(vnode_hmap &node_hmap, const std::string &vpath);
        int persist_hash_map_cache_file(const vnode_hmap &node_hmap, const std::string &filename);
        const std::string get_vpath_cache_file(const std::string &vpath);
        const std::string get_vpath_cache_dir(const std::string &vpath);

    public:
        void set_dirty(const std::string &vpath);
        vnode_hmap *find_hash_map(const std::string &vpath);
        void erase_hash_map(const std::string &vpath);
        void insert_hash_map(const std::string &vpath, vnode_hmap &&node_hmap);
        int move_hash_map_cache(const std::string &from_vpath, const std::string &to_vpath, const bool is_dir);
        int persist_hash_maps();
        int clear();
    };
} // namespace hpfs::hmap::store

#endif