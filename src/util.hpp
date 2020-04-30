#ifndef _HPFS_UTIL_
#define _HPFS_UTIL_

#include <string>

namespace util
{
    bool is_dir_exists(std::string_view path);
}

#endif