#include <unordered_set>
#include <unistd.h>
#include "../hpfs.hpp"
#include "store.hpp"
#include "hasher.hpp"
#include "hmap.hpp"

namespace hmap::store
{
    constexpr const char *HASH_MAP_CACHE_FILE_EXT = ".hcache";
    constexpr int FILE_PERMS = 0644;

    // Hash maps of vnodes keyed by the vpath.
    std::unordered_map<std::string, vnode_hmap> hash_map;

    // List of vpaths with modifications (including deletions) during the session.
    std::unordered_set<std::string> dirty_vpaths;

    void set_dirty(const std::string &vpath)
    {
        dirty_vpaths.emplace(vpath);
    }

    vnode_hmap *find_hash_map(const std::string &vpath)
    {
        const auto iter = hash_map.find(vpath);
        vnode_hmap *node_hmap = (iter == hash_map.end()) ? NULL : &iter->second;
        return node_hmap;
    }

    void erase_hash_map(const std::string &vpath)
    {
        hash_map.erase(vpath);
    }

    void insert_hash_map(const std::string &vpath, vnode_hmap &&node_hmap)
    {
        hash_map.try_emplace(vpath, std::move(node_hmap));
    }

    int persist_hash_maps()
    {
        for (const std::string &vpath : dirty_vpaths)
        {
            hasher::h32 vpath_hash;
            const auto iter = hash_map.find(vpath);

            if (iter == hash_map.end())
                hash_buf(vpath_hash, vpath.data(), vpath.size());
            else
                vpath_hash = iter->second.vpath_hash;

            std::string cache_filename;
            cache_filename
                .append(hpfs::ctx.hmap_dir)
                .append("/")
                .append(vpath_hash.to_hex())
                .append(HASH_MAP_CACHE_FILE_EXT);

            if (iter == hash_map.end())
            {
                // This means the hash map has been deleted. So we should erase the cache file.
                if (unlink(cache_filename.c_str()) == -1)
                    return -1;
            }
            else
            {
                if (persist_hash_map_cache_file(iter->second, cache_filename) == -1)
                    return -1;
            }
        }

        return 0;
    }

    int persist_hash_map_cache_file(const vnode_hmap &node_hmap, const std::string &filename)
    {
        const int fd = open(filename.c_str(), O_CREAT | O_TRUNC | O_RDWR, FILE_PERMS);
        if (fd == -1)
            return -1;

        const uint8_t is_file = node_hmap.is_file ? 1 : 0;

        iovec memsegs[4] = {{(void *)&is_file, sizeof(is_file)},
                            {(void *)&node_hmap.node_hash, sizeof(hasher::h32)},
                            {(void *)&node_hmap.vpath_hash, sizeof(hasher::h32)},
                            {(void *)node_hmap.block_hashes.data(), sizeof(hasher::h32) * node_hmap.block_hashes.size()}};

        if (writev(fd, memsegs, 4) == -1)
            return -1;

        return 0;
    }

    std::string get_vpath_hash_name(const std::string &vpath)
    {
        hasher::h32 vpath_hash;
        hash_buf(vpath_hash, vpath.data(), vpath.size());
        return vpath_hash.to_hex();
    }

} // namespace hmap::store