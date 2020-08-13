#ifndef _HPFS_HMAP_STORE_
#define _HPFS_HMAP_STORE_

#include <string>

namespace hmap::store
{
    void set_dirty(const std::string &vpath);
    int persist_hash_maps();
    int persist_hash_map_cache_file(const vnode_hmap &node_hmap, const std::string &filename);
    std::string get_vpath_hash_name(const std::string &vpath);
} // namespace hmap::store

#endif