#ifndef _HPFS_UTIL_
#define _HPFS_UTIL_

#include <string>

// Write() data block size. We choose this to be the page size for mmap() page alignment.
constexpr size_t BLOCK_SIZE = 4096;

#define MAX(a, b) ((a > b) ? a : b)
#define MIN(a, b) ((a < b) ? a : b)
#define BLOCK_START(x) ((x / BLOCK_SIZE) * BLOCK_SIZE)
#define BLOCK_END(x) (((x) + ((typeof(x))(BLOCK_SIZE)-1)) & ~((typeof(x))(BLOCK_SIZE)-1))

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
    void uint32_to_bytes(uint8_t *dest, uint32_t x);
    uint32_t uint32_from_bytes(uint8_t *data);

} // namespace util

#endif