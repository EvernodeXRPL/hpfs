#include <optional>
#include <string>
#include <string_view>
#include <map>
#include <shared_mutex>
#include "session.hpp"
#include "vfs/virtual_filesystem.hpp"
#include "vfs/fuse_adapter.hpp"
#include "hmap/tree.hpp"
#include "hmap/query.hpp"
#include "inodes.hpp"
#include "audit.hpp"
#include "hpfs.hpp"
#include "tracelog.hpp"

#define CHECK_FAILURE(optional) \
    if (!optional.has_value())  \
    {                           \
        sessions.erase(itr);    \
        return -1;              \
    }

namespace hpfs::session
{

    /*
     * User can start/stop sessions via the FUSE interface by creating and deleting a file with a reserved naming convention.
     * To check for an existance of a session, the existance of the same file can be checked with 'stat'.
     * There can only be single ReadWrite session at any time. There can be multiple ReadOnly sessions in parallel.
     * RW session file: /::hpfs.rw.hmap
     * RO session file: /::hpfs.ro.hmap.<unique name>
     */
    constexpr const char *RW_HMAP_FILE = "/::hpfs.rw.hmap";
    constexpr const char *RW_NOHMAP_FILE = "/::hpfs.rw";
    constexpr const char *RO_HMAP_FILE = "/::hpfs.ro.hmap.";
    constexpr const char *RO_NOHMAP_FILE = "/::hpfs.ro.";
    constexpr const char *RW_SESSION_NAME = "rw";

    std::map<std::string, fs_session> sessions;
    std::shared_mutex sessions_mutex;

    /**
     * Splits the provided path into session name and resource path components.
     * First directory name will be the session name.
     * Anything after session name is the resource path.
     */
    const std::pair<std::string, std::string> split_path(std::string_view path)
    {
        const size_t dir_separator = path.find("/", 1);
        return std::pair<std::string, std::string>(
            // Session name component.
            std::string(dir_separator == std::string::npos ? path.substr(1) : path.substr(1, dir_separator - 1)),
            // Resource path component.
            std::string(dir_separator == std::string::npos ? "/" : path.substr(dir_separator)));
    }

    const fs_session_args parse_session_args(std::string_view path)
    {
        if (path == RW_HMAP_FILE)
            return {true, false, RW_SESSION_NAME, true};
        if (path == RW_NOHMAP_FILE)
            return {true, false, RW_SESSION_NAME, false};

        if (strncmp(path.data(), RO_HMAP_FILE, 16) == 0)
        {
            if (path.size() > 16)
                return {true, true, std::string(path.substr(16)), true};
            else
                return {false};
        }

        if (strncmp(path.data(), RO_NOHMAP_FILE, 11) == 0)
        {
            if (path.size() > 11)
                return {true, true, std::string(path.substr(11)), false};
            else
                return {false};
        }

        return {false};
    }

    /**
     * Checks getattr requests for any session-related metadata activity.
     * @return 0 if request succesfully was interpreted by session control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int session_check_getattr(const char *path, struct stat *stbuf)
    {
        const fs_session_args args = parse_session_args(path);

        // Invalid session request. Pass through to vfs.
        if (!args.valid)
            return 1;

        const auto itr = sessions.find(args.name);
        if (itr == sessions.end())
            return -ENOENT; // No session found.

        // Session found. Return session stat with session id inode number.
        *stbuf = ctx.default_stat;
        stbuf->st_ino = itr->second.ino;
        stbuf->st_mode |= S_IFREG;
        return 0;
    }

    /**
     * Checks file create requests for any session-related metadata activity.
     * @return 0 if request was succesfully interpreted by session control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int session_check_create(const char *path)
    {
        const fs_session_args args = parse_session_args(path);

        // Invalid session request. Pass through to vfs.
        if (!args.valid)
            return 1;

        // Check if this is a readonly session with reserved name.
        if (args.name.empty() || (args.readonly && args.name == RW_SESSION_NAME))
            return -EINVAL;

        {
            SESSION_WRITE_LOCK

            if (sessions.count(args.name) == 1)
                return -EEXIST; // Session name already exists

            return start(args);
        }
    }

    /**
     * Checks file unlink requests for any session-related metadata activity.
     * @return 0 if request succesfully was interpreted by session control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int session_check_unlink(const char *path)
    {
        const fs_session_args args = parse_session_args(path);

        // Invalid session request. Pass through to vfs.
        if (!args.valid)
            return 1;

        {
            SESSION_WRITE_LOCK

            const auto itr = sessions.find(args.name);
            if (itr != sessions.end())
            {
                const fs_session &session = itr->second;
                if (session.readonly == args.readonly && session.hmap_enabled == args.hmap_enabled)
                {
                    sessions.erase(itr);
                    LOG_INFO << (args.readonly ? "RO" : "RW") << " session '" << args.name << "' stopped.";
                    return 0;
                }
            }

            return -ENOENT;
        }
    }

    fs_session *get(const std::string &name)
    {
        const auto itr = sessions.find(name);
        return itr == sessions.end() ? NULL : &itr->second;
    }

    int start(const fs_session_args &args)
    {
        const auto [itr, success] = sessions.emplace(args.name, fs_session{inodes::next(), args.readonly, args.hmap_enabled});
        fs_session &session = itr->second;

        LOG_INFO << "Starting " << (args.readonly ? "RO" : "RW") << " session '" << args.name << "'...";

        auto audit_logger = audit::audit_logger::create(args.readonly ? audit::LOG_MODE::RO : audit::LOG_MODE::RW,
                                                        ctx.log_file_path);

        CHECK_FAILURE(audit_logger);
        session.audit_logger.emplace(std::move(audit_logger.value()));

        auto virt_fs = vfs::virtual_filesystem::create(args.readonly,
                                                       ctx.seed_dir,
                                                       session.audit_logger.value());
        CHECK_FAILURE(virt_fs);
        session.virt_fs.emplace(std::move(virt_fs.value()));
        LOG_DEBUG << "VFS init complete.";

        if (args.hmap_enabled)
        {
            auto hmap_tree = hmap::tree::hmap_tree::create(session.virt_fs.value());

            CHECK_FAILURE(hmap_tree);
            session.hmap_tree.emplace(std::move(hmap_tree.value()));
            session.hmap_query.emplace(hmap::query::hmap_query(session.hmap_tree.value(),
                                                               session.virt_fs.value()));
            LOG_DEBUG << "Hashmap init complete.";
        }

        session.fuse_adapter.emplace(vfs::fuse_adapter(args.readonly,
                                                       session.virt_fs.value(),
                                                       session.audit_logger.value(),
                                                       session.hmap_tree));

        LOG_INFO << (args.readonly ? "RO" : "RW") << " session '" << args.name << "' started.";
        return 0;
    }

    void stop_all()
    {
        SESSION_WRITE_LOCK
        sessions.clear();
    }

    const std::map<ino_t, std::string> get_sessions()
    {
        std::map<ino_t, std::string> list;
        for (const auto &[sess_name, sess] : sessions)
            list.emplace(sess.ino, sess_name);

        return list;
    }

} // namespace hpfs::session