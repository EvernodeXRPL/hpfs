#ifndef _HPFS_HMAP_HMAP_
#define _HPFS_HMAP_HMAP_

#include <string>
#include <vector>
#include "hasher.hpp"
#include "store.hpp"
#include "../vfs.hpp"

namespace hmap
{
    int init();
    void deinit();
    int get_vnode_hmap(store::vnode_hmap **node_hmap, const std::string &vpath);
    void propogate_hash_update(const std::string &vpath, const hasher::h32 &old_hash, const hasher::h32 &new_hash);
    int apply_vnode_create(const std::string &vpath);
    int apply_vnode_update(const std::string &vpath, const vfs::vnode &vn,
                           const off_t file_update_offset, const size_t file_update_size);
    int apply_file_data_update(store::vnode_hmap &node_hmap, const vfs::vnode &vn,
                               const off_t update_offset, const size_t update_size);
    int apply_vnode_delete(const std::string &vpath);
    int apply_vnode_rename(const std::string &from_vpath, const std::string &to_vpath);

} // namespace hmap

#endif