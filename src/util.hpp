#ifndef _HPFS_UTIL_
#define _HPFS_UTIL_

#include <string>

namespace util
{

int64_t epoch();
bool is_dir_exists(std::string_view path);
int set_lock(const int fd, struct flock &lock, bool is_rwlock, const off_t start, const off_t len);
int release_lock(const int fd, struct flock &lock);

} // namespace util

#endif