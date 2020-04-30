#include <sys/stat.h>
#include <string>
#include <chrono>

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

} // namespace util