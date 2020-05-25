#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unordered_map>
#include <string>
#include <string.h>
#include <libgen.h>
#include <math.h>
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
    constexpr size_t BLOCK_SIZE = 4096; //4194304; // 4MB

    std::string hmap_file_path; // File path to persis the hash map.
    std::unordered_map<std::string, vnode_hmap> hash_map;

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
        vnode_hmap dir_hmap;
        if (hash_buf(dir_hmap.hash, vpath.c_str(), vpath.length()) == -1)
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

            // XOR child hash to the parent dir hash.
            dir_hmap.hash ^= child_hash;
        }

        hash = dir_hmap.hash;
        hash_map.try_emplace(vpath, std::move(dir_hmap));

        return 0;
    }

    int calculate_file_hash(h32 &hash, const std::string &vpath)
    {
        vfs::vnode *vn;
        if (get_vnode(vpath, &vn) == -1 ||
            !vn ||
            vfs::update_vnode_mmap(*vn) == -1)
            return -1;

        vnode_hmap file_hmap;

        // Initialize file hash with the file path hash.
        if (hash_buf(file_hmap.hash, vpath.c_str(), vpath.length()) == -1)
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

            file_hmap.block_hashes.push_back(block_hash);
            file_hmap.hash ^= block_hash; // XOR block hash to the file hash.
        }

        hash = file_hmap.hash;
        hash_map.try_emplace(vpath, std::move(file_hmap));

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
            // Write new hash map.
            util::release_lock(fd, file_lock) == -1 || // Release rw lock.
            close(fd) == -1)                           // Close the file.
            return -1;

        return 0;
    }

    void propogate_hash_update(const std::string &vpath, const h32 &old_hash, const h32 &new_hash)
    {
        char *path2 = strdup(vpath.c_str());
        const char *parent_path = dirname(path2);

        // XOR old hash and new hash into parent hash.
        const auto iter = hash_map.find(parent_path);
        vnode_hmap &parent_hmap = iter->second;

        // Remember the old parent hash and update it.
        const h32 parent_old_hash = parent_hmap.hash;
        parent_hmap.hash ^= old_hash;
        parent_hmap.hash ^= new_hash;

        if (strcmp(parent_path, "/") != 0)
            propogate_hash_update(parent_path, parent_old_hash, parent_hmap.hash);
    }

    int add_new_vnode_hmap(const std::string &vpath, const bool is_file)
    {
        // Calculate node hash with the vpath hash.
        h32 hash;
        if (hash_buf(hash, vpath.c_str(), vpath.length()) == -1)
            return -1;

        hash_map.try_emplace(vpath, vnode_hmap{is_file, hash});
        propogate_hash_update(vpath, h32_empty, hash);
        return 0;
    }

    int update_block_hashes(const std::string &vpath, const off_t update_offset, const size_t update_size)
    {
        vfs::vnode *vn;
        if (get_vnode(vpath, &vn) == -1 ||
            !vn ||
            vfs::update_vnode_mmap(*vn) == -1)
            return -1;

        const auto iter = hash_map.find(vpath);
        vnode_hmap &file_hmap = iter->second;
        const h32 old_file_hash = file_hmap.hash;

        const off_t ex_last_block_id = file_hmap.block_hashes.empty()
                                           ? 0
                                           : file_hmap.block_hashes.size() - 1;
        const off_t update_from_block_id = MIN((update_offset / BLOCK_SIZE), ex_last_block_id);
        const off_t update_upto_block_id = (update_offset + update_size) / BLOCK_SIZE;

        // If the updated blocks lie beyond the end of existing blocks that means
        // file size has increased due to write(). We must update block hashes of
        // the blocks that are effected by a file size increase as well.

        // Make sure the block hash list contains total no. of elements required.
        // Resizing will cause the extra block hashes to become 0 which is what we want.
        if (file_hmap.block_hashes.size() < update_upto_block_id + 1)
            file_hmap.block_hashes.resize(update_upto_block_id + 1);

        for (off_t block_id = update_from_block_id; block_id <= update_upto_block_id; block_id++)
        {
            const off_t block_offset = block_id * BLOCK_SIZE;

            // Calculate the new block hash.
            const void *read_buf = (uint8_t *)vn->mmap.ptr + block_offset;
            const int read_len = MIN(BLOCK_SIZE, (vn->st.st_size - block_offset));
            h32 block_hash;
            if (hash_buf(block_hash, &block_offset, sizeof(off_t), read_buf, read_len) == -1)
                return -1;

            // Update the file hash with the new block hash.
            file_hmap.hash ^= file_hmap.block_hashes[block_id]; // XOR old hash.
            file_hmap.hash ^= block_hash;                       // XOR new hash.

            file_hmap.block_hashes[block_id] = block_hash;
        }

        propogate_hash_update(vpath, old_file_hash, file_hmap.hash);
        return 0;
    }

} // namespace hmap