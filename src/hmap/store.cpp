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
        auto iter = hash_map.find(vpath);

        if (iter == hash_map.end())
        {
            // Attempt to load from persisted cache.
            vnode_hmap cached_hmap;
            int res = read_hash_map_cache_file(cached_hmap, vpath);
            if (res == 1)
            {
                const auto [insert_iter, success] = hash_map.try_emplace(vpath, std::move(cached_hmap));
                iter = insert_iter;
            }
        }

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

            const std::string cache_filename = get_vpath_cache_filename(vpath_hash);

            if (iter == hash_map.end())
            {
                // This means the hash map has been deleted. So we should erase the cache file (if exists).
                unlink(cache_filename.c_str());
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

    /**
     * Attempts to read a hash map from the persisted cache file (if exists).
     * @return 0 if cache file not exists. 1 if cache file read success. -1 on error.
     */
    int read_hash_map_cache_file(vnode_hmap &node_hmap, const std::string &vpath)
    {
        hasher::h32 vpath_hash;
        hash_buf(vpath_hash, vpath.data(), vpath.size());
        const std::string cache_filename = get_vpath_cache_filename(vpath_hash);

        const int fd = open(cache_filename.c_str(), O_RDONLY);
        if (fd == -1)
            return errno == ENOENT ? 0 : -1;

        struct stat st;
        if (fstat(fd, &st) == -1)
            return -1;

        const size_t file_size = st.st_size;

        // 65 bytes are taken for the is_file flag, node hash and vpath hash.
        // Rest of the bytes are block hashes.
        const uint32_t block_count = (file_size - 65) / sizeof(hasher::h32);
        node_hmap.block_hashes.resize(block_count);
        uint8_t is_file = 0;

        iovec memsegs[4] = {{(void *)&is_file, sizeof(is_file)},
                            {(void *)&node_hmap.node_hash, sizeof(hasher::h32)},
                            {(void *)&node_hmap.vpath_hash, sizeof(hasher::h32)},
                            {(void *)node_hmap.block_hashes.data(), sizeof(hasher::h32) * node_hmap.block_hashes.size()}};

        if (readv(fd, memsegs, 4) == -1)
            return -1;

        node_hmap.is_file = (is_file == 1);

        return 1;
    }

    std::string get_vpath_cache_filename(const hasher::h32 vpath_hash)
    {
        std::string cache_filename;
        cache_filename
            .append(hpfs::ctx.hmap_dir)
            .append("/")
            .append(vpath_hash.to_hex())
            .append(HASH_MAP_CACHE_FILE_EXT);
        return cache_filename;
    }

} // namespace hmap::store