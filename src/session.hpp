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
    struct fs_session_args
    {
        bool valid;
        bool readonly;
        std::string name;
        bool hmap_enabled;
    };

    struct fs_session
    {
        ino_t ino; // Session's own inode no. We treat this as unique session id.
        bool readonly;
        bool hmap_enabled;
        std::optional<vfs::virtual_filesystem> virt_fs;
        std::optional<vfs::fuse_adapter> fuse_adapter;
        std::optional<audit::audit_logger> audit_logger;
        std::optional<hmap::tree::hmap_tree> hmap_tree;
        std::optional<hmap::query::hmap_query> hmap_query;
    };

    int session_check_getattr(const char *path, struct stat *stbuf);
    int session_check_create(const char *path);
    int session_check_unlink(const char *path);
    const fs_session_args parse_session_args(std::string_view path);
    fs_session *get(std::string_view path);
    int start(const fs_session_args &args);
    void stop_all();

} // namespace hpfs::session

#endif