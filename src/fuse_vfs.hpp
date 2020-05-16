#ifndef _HPFS_FUSE_VFS_
#define _HPFS_FUSE_VFS_

namespace fuse_vfs
{

    int getattr(const char *vpath, struct stat *stbuf);
    int readdir(const char *vpath, vfs::vdir_children_map &children);
    int mkdir(const char *vpath, mode_t mode);
    int rmdir(const char *vpath);
    int rename(const char *from_vpath, const char *to_vpath);
    int unlink(const char *vpath);
    int create(const char *vpath, mode_t mode);
    int read(const char *vpath, char *buf, size_t size, off_t offset);
    int write(const char *vpath, const char *buf, size_t size, off_t offset);
    int truncate(const char *vpath, off_t size);

} // namespace fuse_vfs

#endif