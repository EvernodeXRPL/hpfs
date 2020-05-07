#ifndef _HPFS_VFS_
#define _HPFS_VFS_

#include <sys/types.h>
#include <sys/stat.h>
#include <unordered_map>
#include "logger.hpp"
#include "mmapper.hpp"

namespace vfs
{

struct vfs_node
{
    struct stat st;

    // The offset position of the log file upto which has been
    // read to construct this vnode (inclusive of log record).
    off_t scanned_upto;
    bool is_removed;
    mmapper::fmap mmap;
};

typedef std::unordered_map<std::string, vfs_node> vnode_map;

int init();
int add_vnode(const std::string &vpath, struct stat &st,
              const off_t log_offset, vnode_map::iterator &vnode_iter);
int build_vnode(const std::string &vpath, vnode_map::iterator &vnode_iter);
int get_vnode(const char *vpath, vfs_node **vnode);
int apply_log_record_to_vnode(vfs_node &vnode, const logger::log_record &record,
                              std::vector<uint8_t> payload);

int getattr(const char *vpath, struct stat *stbuf);
int mkdir(const char *vpath, mode_t mode);
int rmdir(const char *vpath);
int create(const char *vpath, mode_t mode);
int read(const char *vpath, char *buf, size_t size, off_t offset);
int write(const char *vpath, const char *buf, size_t size, off_t offset);

} // namespace vfs

#endif