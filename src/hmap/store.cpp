#include <unordered_set>
#include <unordered_map>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include "store.hpp"
#include "hasher.hpp"
#include "../hpfs.hpp"
#include "../tracelog.hpp"

namespace hpfs::hmap::store
{
    constexpr const char *HASH_MAP_CACHE_FILE_EXT = ".hcache";
    constexpr int FILE_PERMS = 0644;

    void hmap_store::set_dirty(const std::string &vpath)
    {
        dirty_vpaths.emplace(vpath);
    }

    vnode_hmap *hmap_store::find_hash_map(const std::string &vpath)
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

    void hmap_store::erase_hash_map(const std::string &vpath)
    {
        hash_map.erase(vpath);
    }

    void hmap_store::insert_hash_map(const std::string &vpath, vnode_hmap &&node_hmap)
    {
        hash_map.try_emplace(vpath, std::move(node_hmap));
    }

    int hmap_store::persist_hash_maps()
    {
        for (const std::string &vpath : dirty_vpaths)
        {
            const auto iter = hash_map.find(vpath);
            const std::string cache_filename = get_vpath_cache_filename(vpath);

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

    int hmap_store::persist_hash_map_cache_file(const vnode_hmap &node_hmap, const std::string &filename)
    {
        const int fd = open(filename.c_str(), O_CREAT | O_TRUNC | O_RDWR, FILE_PERMS);
        if (fd == -1)
            return -1;

        const uint8_t is_file = node_hmap.is_file ? 1 : 0;

        iovec memsegs[4] = {{(void *)&is_file, sizeof(is_file)},
                            {(void *)&node_hmap.node_hash, sizeof(hasher::h32)},
                            {(void *)&node_hmap.name_hash, sizeof(hasher::h32)},
                            {(void *)node_hmap.block_hashes.data(), sizeof(hasher::h32) * node_hmap.block_hashes.size()}};

        if (writev(fd, memsegs, 4) == -1)
        {
            LOG_ERROR << errno << ": Error in persist hmap writev. " << filename;
            close(fd);
            return -1;
        }

        close(fd);
        return 0;
    }

    /**
     * Attempts to read a hash map from the persisted cache file (if exists).
     * @return 0 if cache file not exists. 1 if cache file read success. -1 on error.
     */
    int hmap_store::read_hash_map_cache_file(vnode_hmap &node_hmap, const std::string &vpath)
    {
        const std::string cache_filename = get_vpath_cache_filename(vpath);

        const int fd = open(cache_filename.c_str(), O_RDONLY);
        if (fd == -1)
        {
            if (errno == ENOENT)
                return 0;

            LOG_ERROR << errno << ": Error in hmap cache file open. " << cache_filename;
            return -1;
        }

        struct stat st;
        if (fstat(fd, &st) == -1)
        {
            close(fd);
            LOG_ERROR << errno << ": Error in hmap cache file stat. " << cache_filename;
            return -1;
        }

        const size_t file_size = st.st_size;

        // 65 bytes are taken for the is_file flag, node hash and name hash.
        // Rest of the bytes are block hashes.
        const uint32_t block_count = (file_size - 65) / sizeof(hasher::h32);
        node_hmap.block_hashes.resize(block_count);
        uint8_t is_file = 0;

        iovec memsegs[4] = {{(void *)&is_file, sizeof(is_file)},
                            {(void *)&node_hmap.node_hash, sizeof(hasher::h32)},
                            {(void *)&node_hmap.name_hash, sizeof(hasher::h32)},
                            {(void *)node_hmap.block_hashes.data(), sizeof(hasher::h32) * node_hmap.block_hashes.size()}};

        if (readv(fd, memsegs, 4) == -1)
        {
            close(fd);
            LOG_ERROR << errno << ": Error in hmap cache file readv. " << cache_filename;
            return -1;
        }

        close(fd);
        node_hmap.is_file = (is_file == 1);

        return 1;
    }

    const std::string hmap_store::get_vpath_cache_filename(const std::string &vpath)
    {
        return hpfs::ctx.hmap_dir + vpath + HASH_MAP_CACHE_FILE_EXT;
    }

} // namespace hpfs::hmap::store