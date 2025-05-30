#ifndef _HPFS_VFS_FUSE_ADAPTER_
#define _HPFS_VFS_FUSE_ADAPTER_

#include <shared_mutex>
#include "virtual_filesystem.hpp"
#include "../hmap/tree.hpp"
#include "../audit/audit.hpp"

namespace hpfs::vfs
{
    class fuse_adapter
    {
    private:
        const bool readonly;
        virtual_filesystem &virt_fs;
        hpfs::audit::audit_logger &logger;
        std::optional<hpfs::hmap::tree::hmap_tree> &htree;
        std::shared_mutex fs_mutex;

    private:
        off_t normal_write(const std::string &vpath, const char *buf, const size_t wr_size, const off_t wr_start,
                         vfs::vnode *vn, audit::log_record_header &rh);
        int optimized_write(const std::string &vpath, const char *buf, const size_t wr_size, const off_t wr_start,
                            vfs::vnode &vn, audit::log_record_header &rh);
        int delete_entry(const std::string &to_vpath, const bool is_dir);
        int rename_entry(const std::string &vpath, const std::string &new_vpath, const bool is_dir);

    public:
        fuse_adapter(const bool readonly, virtual_filesystem &virt_fs,
                     hpfs::audit::audit_logger &logger, std::optional<hpfs::hmap::tree::hmap_tree> &htree);
        int getattr(const std::string &vpath, struct stat *stbuf);
        int readdir(const std::string &vpath, vfs::vdir_children_map &children);
        int mkdir(const std::string &vpath, mode_t mode);
        int rmdir(const std::string &vpath);
        int rename(const std::string &from_vpath, const std::string &to_vpath);
        int unlink(const std::string &vpath);
        int create(const std::string &vpath, mode_t mode);
        int read(const std::string &vpath, char *buf, const size_t size, const off_t offset);
        int write(const std::string &vpath, const char *buf, const size_t size, const off_t offset);
        int truncate(const std::string &vpath, const off_t new_size);
        int chmod(const std::string &vpath, mode_t mode);
    };

} // namespace hpfs::vfs

#endif