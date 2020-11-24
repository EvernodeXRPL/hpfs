#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <chrono>
#include <math.h>
#include <signal.h>
#include "util.hpp"
#include "tracelog.hpp"

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
        const int ret = fcntl(fd, F_SETLKW, &lock);
        if (ret == -1)
            LOG_ERROR << errno << ": Error when setting lock. type:" << lock.l_type;

        return ret;
    }

    int release_lock(const int fd, struct flock &lock)
    {
        lock.l_type = F_UNLCK;
        const int ret = fcntl(fd, F_SETLKW, &lock);
        if (ret == -1)
            LOG_ERROR << errno << ": Error when releasing lock. type:" << lock.l_type;

        return ret;
    }

    // Applies signal mask to the calling thread.
    void mask_signal()
    {
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGINT);
        sigaddset(&mask, SIGPIPE);
        pthread_sigmask(SIG_BLOCK, &mask, NULL);
    }

} // namespace util