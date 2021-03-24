#ifndef _HPFS_SESSION_
#define _HPFS_SESSION_

#include <optional>
#include <map>
#include <shared_mutex>
#include "vfs/virtual_filesystem.hpp"
#include "vfs/fuse_adapter.hpp"
#include "hmap/tree.hpp"
#include "hmap/query.hpp"
#include "audit/audit.hpp"

#define SESSION_READ_LOCK std::shared_lock lock(hpfs::session::sessions_mutex);
#define SESSION_WRITE_LOCK std::unique_lock lock(hpfs::session::sessions_mutex);

namespace hpfs::session
{
    extern std::shared_mutex sessions_mutex;

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

        fs_session(const ino_t ino, const bool readonly, const bool hmap_enabled)
            : ino(ino), readonly(readonly), hmap_enabled(hmap_enabled)
        {
        }
    };

    const std::pair<std::string, std::string> split_path(std::string_view path);
    const fs_session_args parse_session_args(std::string_view path);
    int session_check_getattr(const char *path, struct stat *stbuf);
    int session_check_create(const char *path);
    int session_check_unlink(const char *path);
    fs_session *get(const std::string &name);
    int start(const fs_session_args &args);
    void stop_all();
    const std::map<ino_t, std::string> get_sessions();

} // namespace hpfs::session

#endif