#ifndef _HPFS_FUSE_VFS_
#define _HPFS_FUSE_VFS_

namespace fuse_vfs
{
int getattr(const char *vpath, struct stat *stbuf);
}

#endif