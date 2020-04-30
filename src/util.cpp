#include "util.hpp"
#include <sys/stat.h>

namespace util
{

bool is_dir_exists(std::string_view path)
{
    struct stat st;
    return (stat(path.data(), &st) == 0 && S_ISDIR(st.st_mode));
}

} // namespace util