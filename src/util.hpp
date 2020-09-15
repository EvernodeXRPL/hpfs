#ifndef _HPFS_UTIL_
#define _HPFS_UTIL_

#include <string>

#define MAX(a, b) ((a > b) ? a : b)
#define MIN(a, b) ((a < b) ? a : b)

namespace util
{

    // Write() data block size. We choose this to be the page size for mmap() page alignment.
    constexpr size_t BLOCK_SIZE = 4096;

    int64_t epoch();
    bool is_dir_exists(std::string_view path);
    int set_lock(const int fd, struct flock &lock, const bool is_rwlock,
                 const off_t start, const off_t len);
    int release_lock(const int fd, struct flock &lock);
    off_t get_block_start(const off_t raw_offset);
    off_t get_block_end(const off_t raw_offset);
    void mask_signal();

} // namespace util

#endif