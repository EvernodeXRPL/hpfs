#ifndef _HPFS_FUSE_VFS_
#define _HPFS_FUSE_VFS_

namespace fuse_vfs
{

int getattr(const char *vpath, struct stat *stbuf);
int mkdir(const char *vpath, mode_t mode);
int rmdir(const char *vpath);

} // namespace fuse_vfs

#endif