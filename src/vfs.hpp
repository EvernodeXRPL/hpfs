#ifndef _HPFS_VFS_
#define _HPFS_VFS_

#include <sys/types.h>

namespace vfs
{

int create(const char *path, mode_t mode);
int write(const char *path, const char *buf, size_t size, off_t offset);

} // namespace vfs

#endif