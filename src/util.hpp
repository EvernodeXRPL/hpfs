#ifndef _HPFS_UTIL_
#define _HPFS_UTIL_

#include <string>
#include <vector>

// Write() data block size. We choose this to be the page size for mmap() page alignment.
constexpr size_t BLOCK_SIZE = 4096;

#define MAX(a, b) ((a > b) ? a : b)
#define MIN(a, b) ((a < b) ? a : b)
#define BLOCK_START(x) (((x) / BLOCK_SIZE) * BLOCK_SIZE)
#define BLOCK_END(x) ((x + ((__typeof__(x))(BLOCK_SIZE)-1)) & ~((__typeof__(x))(BLOCK_SIZE)-1))

namespace util
{
    int64_t epoch();
    bool is_dir_exists(std::string_view path);
    bool is_file_exists(std::string_view path);
    int set_lock(const int fd, struct flock &lock, const bool is_rwlock,
                 const off_t start, const off_t len);
    int release_lock(const int fd, struct flock &lock);
    void mask_signal();
    const std::string get_name(std::string_view path);
    const std::string get_parent_path(std::string_view path);
    int create_dir_tree_recursive(std::string_view path);
    void uint16_to_bytes(uint8_t *dest, const uint16_t x);
    uint16_t uint16_from_bytes(const uint8_t *data);
    void uint32_to_bytes(uint8_t *dest, const uint32_t x);
    uint32_t uint32_from_bytes(const uint8_t *data);
    void uint64_to_bytes(uint8_t *dest, const uint64_t x);
    uint64_t uint64_from_bytes(const uint8_t *data);
    int stoull(const std::string &str, uint64_t &result);
    const std::vector<std::string> split_string(std::string_view s, std::string_view delimiter);
    int remove_directory_recursively(std::string_view dir_path);

} // namespace util

#endif