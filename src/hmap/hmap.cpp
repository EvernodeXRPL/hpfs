#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unordered_map>
#include <string>
#include <string.h>
#include <libgen.h>
#include "h32.hpp"
#include "hmap.hpp"
#include "../hpfs.hpp"
#include "../util.hpp"
#include "../vfs.hpp"

namespace hmap
{
    constexpr const char *HMAP_FILE_NAME = "hmap.hpfs";
    constexpr int FILE_PERMS = 0644;
    constexpr uint16_t HPFS_VERSION = 1;
    constexpr size_t BLOCK_SIZE = 4194304; // 4MB

    std::string hmap_file_path;
    std::unordered_map<std::string, h32> dir_hashes;
    std::unordered_map<std::string, file_hmap> file_hashes;

    int init()
    {
        std::string hmap_file_path = std::string(hpfs::ctx.fs_dir).append("/").append(HMAP_FILE_NAME);

        const int read_status = read_hmap_file();
        if (read_status == -1)
            return -1;

        if (read_status == 0)
        {
            // Calculate the hash map for the filesystem from scratch.
            h32 root_hash;
            if (calculate_dir_hash(root_hash, "/") == -1)
                return -1;
            std::cout << root_hash << "\n";
        }

        return 0;
    }

    void deinit()
    {
    }

    int read_hmap_file()
    {
        const int fd = open(hmap_file_path.c_str(), O_RDONLY);
        if (fd == -1 && errno == ENOENT) // File does not exist.
            return 0;
        else if (fd == -1 && errno != ENOENT)
            return -1;

        return 1; // File exists and data was read successfully.
    }

    int calculate_dir_hash(h32 &hash, const std::string &vpath)
    {
        vfs::vdir_children_map dir_children;
        if (vfs::get_dir_children(vpath.c_str(), dir_children) == -1)
            return -1;

        // Initialize dir hash with the dir path hash.
        h32 dir_hash;
        if (hash_buf(dir_hash, vpath.c_str(), vpath.length()) == -1)
            return -1;

        for (const auto &[child_name, st] : dir_children)
        {
            std::string child_vpath = std::string(vpath);
            if (child_vpath.back() != '/')
                child_vpath.append("/");
            child_vpath.append(child_name);

            const bool is_dir = S_ISDIR(st.st_mode);

            h32 child_hash;
            if ((is_dir && calculate_dir_hash(child_hash, child_vpath) == -1) ||
                (!is_dir && calculate_file_hash(child_hash, child_vpath) == -1))
                return -1;

            // XOR child hash to the dir hash.
            dir_hash ^= child_hash;
        }

        hash = dir_hash;
        dir_hashes.try_emplace(vpath, std::move(dir_hash));

        return 0;
    }

    int calculate_file_hash(h32 &hash, const std::string &vpath)
    {
        vfs::vnode *vn;
        if (get_vnode(vpath, &vn) == -1 ||
            !vn ||
            vfs::update_vnode_mmap(*vn) == -1)
            return -1;

        file_hmap f_hmap;

        // Initialize file hash with the file path hash.
        if (hash_buf(f_hmap.file_hash, vpath.c_str(), vpath.length()) == -1)
            return -1;

        // Hash all data blocks.
        const size_t file_size = vn->st.st_size;
        for (off_t block_offset = 0; block_offset < file_size; block_offset += BLOCK_SIZE)
        {
            const void *read_buf = (uint8_t *)vn->mmap.ptr + block_offset;
            const int read_len = MIN(BLOCK_SIZE, (file_size - block_offset));
            h32 block_hash;
            if (hash_buf(block_hash, &block_offset, sizeof(off_t), read_buf, read_len) == -1)
                return -1;

            f_hmap.block_hashes.push_back(block_hash);
            f_hmap.file_hash ^= block_hash; // XOR block hash to the file hash.
        }

        hash = f_hmap.file_hash;
        file_hashes.try_emplace(vpath, std::move(f_hmap));

        return 0;
    }

    int persist_hmap()
    {
        // Open or create the hash map file.
        const int fd = open(hmap_file_path.c_str(), O_CREAT | O_RDWR, FILE_PERMS);
        if (fd == -1)
            return -1;

        flock file_lock;

        if (util::set_lock(fd, file_lock, true, 0, 1) == -1 || // Acquire exclusive rw lock.
            ftruncate(fd, 0) == -1 ||                          // Truncate the file.
            // Write new hash map
            util::release_lock(fd, file_lock) == -1 || // Release rw lock.
            close(fd) == -1)                           // Close the file.
            return -1;

        return 0;
    }
} // namespace hmap