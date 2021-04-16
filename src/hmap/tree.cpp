#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <string.h>
#include <libgen.h>
#include <math.h>
#include <optional>
#include "hasher.hpp"
#include "store.hpp"
#include "tree.hpp"
#include "../tracelog.hpp"
#include "../util.hpp"
#include "../vfs/vfs.hpp"
#include "../vfs/virtual_filesystem.hpp"

namespace hpfs::hmap::tree
{
#define PRINT_ROOT_HASH LOG_DEBUG << " Root hash: " << store.find_hash_map(ROOT_VPATH)->node_hash;

    constexpr size_t BLOCK_SIZE = 4194304; // 4MB
    constexpr const char *ROOT_VPATH = "/";

    int hmap_tree::create(std::optional<hmap_tree> &tree, hpfs::vfs::virtual_filesystem &virt_fs)
    {
        tree.emplace(virt_fs);
        if (tree->init() == -1)
        {
            tree.reset();
            return -1;
        }

        return 0;
    }

    hmap_tree::hmap_tree(hpfs::vfs::virtual_filesystem &virt_fs) : virt_fs(virt_fs)
    {
    }

    int hmap_tree::init()
    {
        LOG_INFO << "Initializing hash map...";

        // Check whether there's already a persisted root hash map.
        const store::vnode_hmap *root_hmap = store.find_hash_map(ROOT_VPATH);
        if (root_hmap == NULL)
        {
            // Calculate entire filesystem hash from scratch.
            hasher::h32 root_hash;
            if (calculate_dir_hash(root_hash, ROOT_VPATH) == -1)
                return -1;
            LOG_INFO << "Calculated root hash: " << root_hash;
        }
        else
        {
            LOG_INFO << "Loaded root hash: " << root_hmap->node_hash;
        }

        initialized = true;
        return 0;
    }

    int hmap_tree::get_vnode_hmap(store::vnode_hmap **node_hmap, const std::string &vpath)
    {
        *node_hmap = store.find_hash_map(vpath);
        return 0;
    }

    int hmap_tree::calculate_dir_hash(hasher::h32 &node_hash, const std::string &vpath)
    {
        vfs::vnode *vn = NULL;
        if (vpath != ROOT_VPATH && (virt_fs.get_vnode(vpath, &vn) == -1 || !vn))
        {
            LOG_ERROR << "File hash calc failure in vfs vnode get. " << vpath;
            return -1;
        }

        vfs::vdir_children_map dir_children;
        if (virt_fs.get_dir_children(vpath.c_str(), dir_children) == -1)
        {
            LOG_ERROR << "Dir hash calc failure in vfs dir children get. " << vpath;
            return -1;
        }

        // Initialize dir hash with the dir name hash.
        store::vnode_hmap dir_hmap{false};
        generate_name_hash(dir_hmap, vpath);
        if (vn)
            generate_meta_hash(dir_hmap, *vn);
        else
            dir_hmap.meta_hash = hasher::h32_empty;

        dir_hmap.node_hash = dir_hmap.name_hash;
        dir_hmap.node_hash ^= dir_hmap.meta_hash;

        for (const auto &[child_name, st] : dir_children)
        {
            std::string child_vpath = std::string(vpath);
            if (child_vpath.back() != '/')
                child_vpath.append("/");
            child_vpath.append(child_name);

            const bool is_dir = S_ISDIR(st.st_mode);

            hasher::h32 child_hash;
            if ((is_dir && calculate_dir_hash(child_hash, child_vpath) == -1) ||
                (!is_dir && calculate_file_hash(child_hash, child_vpath) == -1))
                return -1;

            // XOR child hash to the parent dir node hash.
            dir_hmap.node_hash ^= child_hash;
        }

        node_hash = dir_hmap.node_hash;
        store.insert_hash_map(vpath, std::move(dir_hmap));
        store.set_dirty(vpath);

        return 0;
    }

    int hmap_tree::calculate_file_hash(hasher::h32 &node_hash, const std::string &vpath)
    {
        vfs::vnode *vn = NULL;
        if (virt_fs.get_vnode(vpath, &vn) == -1 || !vn)
        {
            LOG_ERROR << "File hash calc failure in vfs vnode get. " << vpath;
            return -1;
        }

        store::vnode_hmap file_hmap{true};
        generate_name_hash(file_hmap, vpath);                                // Name hash.
        generate_meta_hash(file_hmap, *vn);                                  // Meta hash.
        if (apply_file_data_update(file_hmap, *vn, 0, vn->st.st_size) == -1) // File hash.
        {
            LOG_ERROR << "File hash calc failure in applying file data update. " << vpath;
            return -1;
        }

        node_hash = file_hmap.node_hash;
        store.insert_hash_map(vpath, std::move(file_hmap));
        store.set_dirty(vpath);

        return 0;
    }

    void hmap_tree::propogate_hash_update(const std::string &vpath, const hasher::h32 &old_hash, const hasher::h32 &new_hash)
    {
        std::string parent_path = util::get_parent_path(vpath);
        store::vnode_hmap *hmap_entry = store.find_hash_map(parent_path);
        if (hmap_entry == NULL)
            return;
        store::vnode_hmap &parent_hmap = *hmap_entry;

        // XOR old hash and new hash into parent hash.
        // Remember the old parent hash before updating it.
        const hasher::h32 parent_old_hash = parent_hmap.node_hash;
        parent_hmap.node_hash ^= old_hash;
        parent_hmap.node_hash ^= new_hash;
        store.set_dirty(parent_path);

        if (parent_path != ROOT_VPATH)
            propogate_hash_update(parent_path, parent_old_hash, parent_hmap.node_hash);
    }

    int hmap_tree::apply_vnode_create(const std::string &vpath)
    {
        vfs::vnode *vn = NULL;
        if (virt_fs.get_vnode(vpath, &vn) == -1 || !vn)
            return -1;

        const bool is_file = S_ISREG(vn->st.st_mode);

        // Initial node hash is the name+meta hash.
        store::vnode_hmap vn_hmap{is_file};
        generate_name_hash(vn_hmap, vpath);
        generate_meta_hash(vn_hmap, *vn);
        vn_hmap.node_hash = vn_hmap.name_hash;
        vn_hmap.node_hash ^= vn_hmap.meta_hash;

        hasher::h32 new_node_hash = vn_hmap.node_hash;
        store.insert_hash_map(vpath, std::move(vn_hmap));
        store.set_dirty(vpath);

        propogate_hash_update(vpath, hasher::h32_empty, new_node_hash);
        PRINT_ROOT_HASH

        return 0;
    }

    int hmap_tree::apply_vnode_metadata_update(const std::string &vpath, const vfs::vnode &vn)
    {
        store::vnode_hmap *hmap_entry = store.find_hash_map(vpath);
        if (hmap_entry == NULL)
        {
            LOG_ERROR << "Hash calc vnode update apply failed. No hmap entry. " << vpath;
            return -1;
        }

        store::vnode_hmap &node_hmap = *hmap_entry;
        const hasher::h32 old_hash = node_hmap.node_hash; // Remember old hash before we modify.

        node_hmap.node_hash ^= node_hmap.meta_hash;
        generate_meta_hash(node_hmap, vn);
        node_hmap.node_hash ^= node_hmap.meta_hash;
        store.set_dirty(vpath);

        propogate_hash_update(vpath, old_hash, node_hmap.node_hash);
        PRINT_ROOT_HASH

        return 0;
    }

    int hmap_tree::apply_vnode_data_update(const std::string &vpath, const vfs::vnode &vn,
                                           const off_t file_update_offset, const size_t file_update_size)
    {
        store::vnode_hmap *hmap_entry = store.find_hash_map(vpath);
        if (hmap_entry == NULL)
        {
            LOG_ERROR << "Hash calc vnode update apply failed. No hmap entry. " << vpath;
            return -1;
        }

        store::vnode_hmap &node_hmap = *hmap_entry;
        const hasher::h32 old_hash = node_hmap.node_hash; // Remember old hash before we modify.

        // If this is a file update operation, update the block hashes and recalculate
        // the file hash.
        if (S_ISREG(vn.st.st_mode))
        {
            if (apply_file_data_update(node_hmap, vn, file_update_offset, file_update_size) == -1)
            {
                LOG_ERROR << "Hash calc vnode update apply failed. File data update failure. " << vpath;
                return -1;
            }

            store.set_dirty(vpath);
        }

        propogate_hash_update(vpath, old_hash, node_hmap.node_hash);
        PRINT_ROOT_HASH

        return 0;
    }

    int hmap_tree::apply_file_data_update(store::vnode_hmap &node_hmap, const vfs::vnode &vn,
                                          const off_t update_offset, const size_t update_size)
    {
        const size_t file_size = vn.st.st_size;
        const uint32_t old_block_count = node_hmap.block_hashes.size();
        const uint32_t required_block_count = file_size == 0
                                                  ? 0
                                                  : ceil((double)file_size / (double)BLOCK_SIZE);

        if (old_block_count == required_block_count && old_block_count == 0)
            return 0;

        // Resize the block hashes list according to current file size.
        node_hmap.block_hashes.resize(required_block_count);

        // Reset file hash with name and meta hash.
        node_hmap.node_hash = node_hmap.name_hash;
        node_hmap.node_hash ^= node_hmap.meta_hash;

        const off_t update_end_offset = update_offset + update_size;

        // Calculate hashes of updated blocks.
        for (uint32_t block_id = (update_offset / BLOCK_SIZE);; block_id++)
        {
            const off_t block_offset = block_id * BLOCK_SIZE;
            if (block_offset >= update_end_offset)
                break;

            // Calculate the new block hash.
            const void *read_buf = (uint8_t *)vn.mmap.ptr + block_offset;
            const int read_len = MIN(BLOCK_SIZE, (file_size - block_offset));

            uint8_t block_offset_buf[8];
            util::uint64_to_bytes(block_offset_buf, block_offset);

            hasher::h32 block_hash;
            hasher::hash_buf(block_hash, block_offset_buf, sizeof(block_offset_buf), read_buf, read_len);

            node_hmap.block_hashes[block_id] = block_hash;
        }

        // Add block hashes to the file hash.
        for (const hasher::h32 &block_hash : node_hmap.block_hashes)
            node_hmap.node_hash ^= block_hash;

        return 0;
    }

    int hmap_tree::apply_vnode_delete(const std::string &vpath)
    {
        store::vnode_hmap *hmap_entry = store.find_hash_map(vpath);
        if (hmap_entry == NULL)
        {
            LOG_ERROR << "Hash calc vnode delete apply failed. No hmap entry. " << vpath;
            return -1;
        }

        const hasher::h32 node_hash = hmap_entry->node_hash;
        store.erase_hash_map(vpath);
        store.set_dirty(vpath);

        propogate_hash_update(vpath, node_hash, hasher::h32_empty);
        PRINT_ROOT_HASH

        return 0;
    }

    int hmap_tree::apply_vnode_rename(const std::string &from_vpath, const std::string &to_vpath, const bool is_dir)
    {
        // Backup and delete the hash node.
        store::vnode_hmap *hmap_entry = store.find_hash_map(from_vpath);
        if (hmap_entry == NULL)
        {
            LOG_ERROR << "Hash calc vnode rename apply failed. No hmap entry. " << from_vpath;
            return -1;
        }

        // Persist all dirty hash maps so far and move the cache file/dir to new location.
        store.persist_hash_maps();
        if (store.move_hash_map_cache(from_vpath, to_vpath, is_dir) == -1)
            return -1;

        store::vnode_hmap node_hmap = *hmap_entry; // Create a copy and erase the hmap entry.
        store.erase_hash_map(from_vpath);

        // Update hash map with removed node hash.
        propogate_hash_update(from_vpath, node_hmap.node_hash, hasher::h32_empty);

        // Update the node hash for the new vpath name.
        node_hmap.node_hash ^= node_hmap.name_hash; // XOR old name hash.
        generate_name_hash(node_hmap, to_vpath);
        node_hmap.node_hash ^= node_hmap.name_hash; // XOR new name hash.

        // Update hash map with new node hash.
        propogate_hash_update(to_vpath, hasher::h32_empty, node_hmap.node_hash);

        store.insert_hash_map(to_vpath, std::move(node_hmap));
        store.set_dirty(to_vpath);

        PRINT_ROOT_HASH
        return 0;
    }

    hmap::hasher::h32 hmap_tree::get_root_hash()
    {
        store::vnode_hmap *node_hmap = store.find_hash_map(ROOT_VPATH);
        if (node_hmap == NULL)
        {
            return hmap::hasher::h32_empty;
        }
        return node_hmap->node_hash;
    }

    void hmap_tree::generate_name_hash(store::vnode_hmap &vn_hmap, std::string_view vpath)
    {
        hasher::hash_buf(vn_hmap.name_hash, util::get_name(vpath));
    }

    void hmap_tree::generate_meta_hash(store::vnode_hmap &vn_hmap, const vfs::vnode &vn)
    {
        uint8_t buf[4];
        util::uint32_to_bytes(buf, vn.st.st_mode);
        hasher::hash_buf(vn_hmap.meta_hash, std::string_view((const char *)buf, sizeof(buf)));
    }

    /**
     * Clear the hash map calculate the entire root hash and persist into the file.
     * @param root_hash Recalculated root hash.
     * @return -1 on error and 0 on success.
    */
    int hmap_tree::re_build_hash_maps(hasher::h32 &root_hash)
    {
        if (store.clear() == -1 ||                             // Clear the existing hash store.
            calculate_dir_hash(root_hash, ROOT_VPATH) == -1 || // Calculate entire filesystem hash from scratch.
            store.persist_hash_maps() == -1)                   // Persist calculated hashes to disk.
        {
            LOG_ERROR << "Error re building the hash map.";
            return -1;
        }
        return 0;
    }

    /**
     * Persist hash map to the disk.
     * @return -1 on error and 0 on success.
    */
    int hmap_tree::persist_hash_maps()
    {
        return store.persist_hash_maps();
    }

    hmap_tree::~hmap_tree()
    {
        if (initialized && !moved)
        {
            // Persist any hash map updates to the disk.
            store.persist_hash_maps();
        }
    }

} // namespace hpfs::hmap::tree