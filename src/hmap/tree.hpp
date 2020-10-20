#ifndef _HPFS_HMAP_TREE_
#define _HPFS_HMAP_TREE_

#include <string>
#include <vector>
#include <optional>
#include "hasher.hpp"
#include "store.hpp"
#include "../vfs/vfs.hpp"
#include "../vfs/virtual_filesystem.hpp"

namespace hpfs::hmap::tree
{
    class hmap_tree
    {
    private:
        bool initialized = false; // Indicates that the instance has been initialized properly.
        store::hmap_store store;
        hpfs::vfs::virtual_filesystem &virt_fs;
        hmap_tree(hpfs::vfs::virtual_filesystem &virt_fs);
        int init();

    public:
        static std::optional<hmap_tree> create(hpfs::vfs::virtual_filesystem &virt_fs);
        int get_vnode_hmap(store::vnode_hmap **node_hmap, const std::string &vpath);
        int calculate_dir_hash(hasher::h32 &node_hash, const std::string &vpath);
        int calculate_file_hash(hasher::h32 &node_hash, const std::string &vpath);
        void propogate_hash_update(const std::string &vpath, const hasher::h32 &old_hash, const hasher::h32 &new_hash);
        int apply_vnode_create(const std::string &vpath);
        int apply_vnode_update(const std::string &vpath, const vfs::vnode &vn,
                               const off_t file_update_offset, const size_t file_update_size);
        int apply_file_data_update(store::vnode_hmap &node_hmap, const vfs::vnode &vn,
                                   const off_t update_offset, const size_t update_size);
        int apply_vnode_delete(const std::string &vpath);
        int apply_vnode_rename(const std::string &from_vpath, const std::string &to_vpath);
        ~hmap_tree();
    };

} // namespace hpfs::hmap::tree

#endif