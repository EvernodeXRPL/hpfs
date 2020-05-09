#ifndef _HPFS_VFS2_
#define _HPFS_VFS2_

#include <sys/types.h>
#include <sys/stat.h>
#include <unordered_map>
#include <vector>
#include "logger.hpp"

namespace vfs2
{

struct fmap
{
    void *ptr = NULL;
    size_t size = 0;
};

struct vdata_segment
{
    int physical_fd = 0;
    size_t size = 0;
    off_t physical_offset = 0;
    off_t logical_offset = 0;
};

struct vfs_node
{
    struct stat st;

    // The offset position of the log file upto which has been
    // read to construct this vnode (inclusive of log record).
    off_t log_scanned_upto = 0;
    int seed_fd = 0;
    fmap mmap;

    std::vector<vdata_segment> data_segs;
    
    // How many data segs from the beginig of list that has been mapped to memory.
    uint32_t mapped_data_segs = 0;
};

typedef std::unordered_map<std::string, vfs_node> vnode_map;
typedef std::unordered_map<std::string, struct stat> vdir_children_map;

int init();
void deinit();
int add_vnode(const std::string &vpath, struct stat &st,
              const off_t log_offset, vnode_map::iterator &vnode_iter);
int build_vnode(const std::string &vpath, vnode_map::iterator &vnode_iter, const bool stat_only);
bool is_record_matches_vpath(std::string_view vpath, const logger::log_record &record);
int get_vnode(const char *vpath, vfs_node **vnode, const bool stat_only);
int apply_log_record_to_vnode(vnode_map::iterator &vnode_iter, const logger::log_record &record,
                              std::vector<uint8_t> payload);
int update_vnode_fmap(vfs_node &vnode);
int delete_vnode(vnode_map::iterator &vnode_iter);

int getattr(const char *vpath, struct stat *stbuf);
int readdir(const char *vpath, vdir_children_map &children);
int mkdir(const char *vpath, mode_t mode);
int rmdir(const char *vpath);
int rename(const char *from_vpath, const char *to_vpath);
int unlink(const char *vpath);
int create(const char *vpath, mode_t mode);
int read(const char *vpath, char *buf, size_t size, off_t offset);
int write(const char *vpath, const char *buf, size_t size, off_t offset);

} // namespace vfs

#endif