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
        lock.l_pid = 0;
        const int ret = fcntl(fd, F_OFD_SETLKW, &lock);
        if (ret == -1)
            LOG_ERROR << errno << ": Error when setting lock. type:" << lock.l_type;

        return ret;
    }

    int release_lock(const int fd, struct flock &lock)
    {
        lock.l_type = F_UNLCK;
        const int ret = fcntl(fd, F_OFD_SETLKW, &lock);
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
        const std::string name = basename(path2);
        free(path2);
        return name;
    }

    // Returns the parent full path of the given vpath.
    const std::string get_parent_path(std::string_view path)
    {
        char *path2 = strdup(path.data());
        const std::string parent_path = dirname(path2);
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

    /**
     * Convert the given uint16_t number to bytes in big endian format.
     * @param dest Byte array pointer.
     * @param x Number to be converted.
    */
    void uint16_to_bytes(uint8_t *dest, const uint16_t x)
    {
        dest[0] = (uint8_t)((x >> 8) & 0xff);
        dest[1] = (uint8_t)((x >> 0) & 0xff);
    }

    /**
     * Read the uint16_t number from the given byte array which is in big endian format.
     * @param data Byte array pointer.
     * @return The uint16_t number in the given byte array.
    */
    uint16_t uint16_from_bytes(const uint8_t *data)
    {
        return ((uint16_t)data[0] << 8) +
               (uint16_t)data[1];
    }

    void uint32_to_bytes(uint8_t *dest, const uint32_t x)
    {
        dest[0] = (uint8_t)((x >> 24) & 0xff);
        dest[1] = (uint8_t)((x >> 16) & 0xff);
        dest[2] = (uint8_t)((x >> 8) & 0xff);
        dest[3] = (uint8_t)((x >> 0) & 0xff);
    }

    uint32_t uint32_from_bytes(const uint8_t *data)
    {
        return ((uint32_t)data[0] << 24) +
               ((uint32_t)data[1] << 16) +
               ((uint32_t)data[2] << 8) +
               ((uint32_t)data[3]);
    }

    void uint64_to_bytes(uint8_t *dest, const uint64_t x)
    {
        dest[0] = (uint8_t)((x >> 56) & 0xff);
        dest[1] = (uint8_t)((x >> 48) & 0xff);
        dest[2] = (uint8_t)((x >> 40) & 0xff);
        dest[3] = (uint8_t)((x >> 32) & 0xff);
        dest[4] = (uint8_t)((x >> 24) & 0xff);
        dest[5] = (uint8_t)((x >> 16) & 0xff);
        dest[6] = (uint8_t)((x >> 8) & 0xff);
        dest[7] = (uint8_t)((x >> 0) & 0xff);
    }

    uint64_t uint64_from_bytes(const uint8_t *data)
    {
        return ((uint64_t)data[0] << 56) +
               ((uint64_t)data[1] << 48) +
               ((uint64_t)data[2] << 40) +
               ((uint64_t)data[3] << 32) +
               ((uint64_t)data[4] << 24) +
               ((uint64_t)data[5] << 16) +
               ((uint64_t)data[6] << 8) +
               ((uint64_t)data[7]);
    }

    /**
     * Converts given string to a uint_64. A wrapper function for std::stoull. 
     * @param str String variable.
     * @param result Variable to store the answer from the conversion.
     * @return Returns 0 in a successful conversion and -1 on error.
    */
    int stoull(const std::string &str, uint64_t &result)
    {
        try
        {
            result = std::stoull(str);
        }
        catch (const std::exception &e)
        {
            // Return -1 if any exceptions are captured.
            return -1;
        }
        return 0;
    }

    /**
     * Splits the provided string by given delimeter.
     * @param s String value to be splited.
     * @param delimiter Splitting delimiter.
     * @return The list of resulting strings.
     */
    const std::vector<std::string> split_string(std::string_view s, std::string_view delimiter)
    {
        size_t pos = 0;
        std::vector<std::string> list;
        std::string value(s);
        while ((pos = value.find(delimiter)) != std::string::npos)
        {
            list.push_back(value.substr(0, pos));
            value.erase(0, pos + delimiter.length());
        }
        list.push_back(value);

        return list;
    }

} // namespace util