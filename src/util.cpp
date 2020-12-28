#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <chrono>
#include <signal.h>
#include <libgen.h>
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

    bool is_file_exists(std::string_view path)
    {
        struct stat st;
        return (stat(path.data(), &st) == 0 && S_ISREG(st.st_mode));
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

    // Returns the file/dir name of the given path.
    const std::string get_name(std::string_view path)
    {
        char *path2 = strdup(path.data());
        std::string name = basename(path2);
        free(path2);
        return name;
    }

    // Returns the parent full path of the given vpath.
    const std::string get_parent_path(std::string_view path)
    {
        char *path2 = strdup(path.data());
        std::string parent_path = dirname(path2);
        free(path2);
        return parent_path;
    }

    /**
     * Recursively creates directories and sub-directories if not exist. 
     * @param path Directory path.
     * @return Returns 0 operations succeeded otherwise -1.
     */
    int create_dir_tree_recursive(std::string_view path)
    {
        if (strcmp(path.data(), "/") == 0) // No need of checking if we are at root.
            return 0;

        // Check whether this dir exists or not.
        struct stat st;
        if (stat(path.data(), &st) != 0 || !S_ISDIR(st.st_mode))
        {
            // Check and create parent dir tree first.
            if (create_dir_tree_recursive(util::get_parent_path(path)) == -1)
                return -1;

            // Create this dir.
            if (mkdir(path.data(), S_IRWXU | S_IRWXG | S_IROTH) == -1)
                return -1;
        }

        return 0;
    }
} // namespace util