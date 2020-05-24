#include <blake2.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unordered_map>
#include <string>
#include "h32.hpp"
#include "hmap.hpp"
#include "../hpfs.hpp"
#include "../util.hpp"

namespace hmap
{
    constexpr const char *HMAP_FILE_NAME = "hmap.hpfs";
    constexpr int FILE_PERMS = 0644;
    constexpr uint16_t HPFS_VERSION = 1;

    std::string hmap_file_path;
    std::unordered_map<std::string, h32> dir_hashes;

    int init()
    {
        std::string hmap_file_path = std::string(hpfs::ctx.fs_dir).append("/").append(HMAP_FILE_NAME);

        const int read_status = read_hmap_file();
        if (read_status == -1)
            return -1;

        if (read_status == 0)
        {
            // Calculate the hash map.
        }

        h32 hash;
        const char *text = "Hello";
        if (blake2b((uint8_t *)&hash, text, NULL, sizeof(hash), 5, 0) == -1)
        {
            std::cerr << errno << ": blake failed\n";
            return 1;
        }

        std::cout << "hash:" << std::hex << hash << "\n";
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

    /**
     * Calculate the hash map for the filesystem from scratch.
     */
    int calculate_hmap()
    {
        return 0; //calculate_dir_hash("/");
    }

    int calculate_dir_hash(const std::string &path)
    {
        return 0;
    }

    int calculate_file_hash(const std::string &path)
    {
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
            util::release_lock(fd, file_lock) == -1 ||         // Release rw lock.
            close(fd) == -1)                                   // Close the file.
            return -1;

        return 0;
    }
} // namespace hmap