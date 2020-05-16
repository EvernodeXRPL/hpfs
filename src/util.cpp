#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <chrono>
#include <math.h>
#include "util.hpp"

namespace util
{

    /**
     * Returns current time in UNIX epoch milliseconds.
     */
    int64_t epoch()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    bool is_dir_exists(std::string_view path)
    {
        struct stat st;
        return (stat(path.data(), &st) == 0 && S_ISDIR(st.st_mode));
    }

    int set_lock(const int fd, struct flock &lock, const bool is_rwlock,
                 const off_t start, const off_t len)
    {
        lock.l_type = is_rwlock ? F_WRLCK : F_RDLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = start,
        lock.l_len = len;
        return fcntl(fd, F_SETLKW, &lock);
    }

    int release_lock(const int fd, struct flock &lock)
    {
        lock.l_type = F_UNLCK;
        return fcntl(fd, F_SETLKW, &lock);
    }

    off_t get_block_start(const off_t raw_offset)
    {
        // Integer division.
        return (raw_offset / BLOCK_SIZE) * BLOCK_SIZE;
    }

    off_t get_block_end(const off_t raw_offset)
    {
        const double div = (double)raw_offset / (double)BLOCK_SIZE;
        return ((off_t)ceil(div)) * BLOCK_SIZE;
    }

} // namespace util