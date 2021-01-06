#ifndef _HPFS_INODES_
#define _HPFS_INODES_

#include <sys/types.h>

namespace hpfs::inodes
{
    constexpr ino_t ROOT_INO = 1; // Filesystem root inode
    
    ino_t next();

} // namespace hpfs::inodes

#endif