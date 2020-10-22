#ifndef _HPFS_SESSION_
#define _HPFS_SESSION_

#include <optional>
#include "vfs/virtual_filesystem.hpp"
#include "vfs/fuse_adapter.hpp"
#include "hmap/tree.hpp"
#include "hmap/query.hpp"
#include "audit.hpp"

namespace hpfs::session
{
    struct fs_session
    {
        std::optional<vfs::virtual_filesystem> virt_fs;
        std::optional<vfs::fuse_adapter> fuse_adapter;
        std::optional<audit::audit_logger> audit_logger;
        std::optional<hmap::tree::hmap_tree> hmap_tree;
        std::optional<hmap::query::hmap_query> hmap_query;
    };

    int session_check_getattr(const char *path, struct stat *stbuf);
    int session_check_create(const char *path);
    int session_check_unlink(const char *path);
    int start();
    int stop();
    std::optional<fs_session> &get();

} // namespace hpfs::session

#endif