#ifndef _HPFS_VFS_FUSE_ADAPTER_
#define _HPFS_VFS_FUSE_ADAPTER_

#include "virtual_filesystem.hpp"
#include "../hmap/tree.hpp"
#include "../audit.hpp"

namespace hpfs::vfs
{
    class fuse_adapter
    {
    private:
        const bool readonly;
        virtual_filesystem &virt_fs;
        hpfs::audit::audit_logger &logger;
        std::optional<hpfs::hmap::tree::hmap_tree> &htree;

    private:
        int delete_entry(const std::string &to_vpath, const bool is_dir);
        int rename_entry(const std::string &vpath, const std::string &new_vpath, const bool is_dir);

    public:
        fuse_adapter(const bool readonly, virtual_filesystem &virt_fs,
                     hpfs::audit::audit_logger &logger, std::optional<hpfs::hmap::tree::hmap_tree> &htree);
        int getattr(const char *vpath, struct stat *stbuf);
        int readdir(const char *vpath, vfs::vdir_children_map &children);
        int mkdir(const char *vpath, mode_t mode);
        int rmdir(const char *vpath);
        int rename(const std::string &from_vpath, const std::string &to_vpath);
        int unlink(const char *vpath);
        int create(const char *vpath, mode_t mode);
        int read(const char *vpath, char *buf, const size_t size, const off_t offset);
        int write(const char *vpath, const char *buf, const size_t size, const off_t offset);
        int truncate(const char *vpath, const off_t new_size);
    };

} // namespace hpfs::vfs

#endif