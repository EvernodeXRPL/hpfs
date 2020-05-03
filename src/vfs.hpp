#ifndef _HPFS_VFS_
#define _HPFS_VFS_

#include <sys/types.h>
#include <sys/stat.h>
#include <unordered_map>

namespace vfs
{

struct vfs_node
{
    struct stat st;

    // The offset position of the log file upto which has been
    // read to construct this vnode.
    off_t log_offset;
    bool is_removed;
};

typedef std::unordered_map<std::string, vfs_node> vnode_map;

int init();
int getattr(const char *path, struct stat *stbuf);
int mkdir(const char *path, mode_t mode);
int create(const char *path, mode_t mode);
int read(const char *path, char *buf, size_t size, off_t offset);
int write(const char *path, const char *buf, size_t size, off_t offset);

int add_vnode(const std::string &path, struct stat &st,
              const off_t log_offset, vnode_map::iterator &vnode_iter);
int build_vnode(const std::string &path, vnode_map::iterator &vnode_iter);

} // namespace vfs

#endif