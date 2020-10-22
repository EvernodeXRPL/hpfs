#include <optional>
#include "session.hpp"
#include "vfs/virtual_filesystem.hpp"
#include "vfs/fuse_adapter.hpp"
#include "hmap/tree.hpp"
#include "hmap/query.hpp"
#include "audit.hpp"
#include "hpfs.hpp"
#include "tracelog.hpp"

#define CHECK_FAILURE(optional, session) \
    if (!optional.has_value())           \
    {                                    \
        session.reset();                 \
        return -1;                       \
    }

namespace hpfs::session
{

    // User can start/stop sessions via the FUSE interface by creating and deleting a file with this reserved name.
    // To check for an existance of a session, the existance of the same file can be checked with 'stat'.
    constexpr const char *SESSION_METAFILE_PATH = "/::hpfs.session";

    std::optional<fs_session> default_session;
    struct stat session_metafile_stat;

    /**
     * Checks getattr requests for any session-related metadata activity.
     * @return 0 if request succesfully was interpreted by session control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int session_check_getattr(const char *path, struct stat *stbuf)
    {
        // When there is no session, treat root path as success so we will return dummy stat for root.
        // Filesystem will fail if we return error code for root.
        if (!default_session && strcmp(path, "/") == 0)
        {
            // Create dummy stat for successful session control response.
            *stbuf = ctx.default_stat;
            stbuf->st_ino = hpfs::ROOT_INO;
            stbuf->st_mode |= S_IFDIR;
            return 0;
        }
        else if (strcmp(path, SESSION_METAFILE_PATH) == 0)
        {
            // If a session exists, we reply as session meta file exists.
            if (default_session)
            {
                *stbuf = ctx.default_stat;
                stbuf->st_ino = hpfs::SESSION_METAFILE_INO;
                stbuf->st_mode |= S_IFREG;
                return 0;
            }
            else
            {
                return -ENOENT;
            }
        }
        else if (!default_session)
        {
            // Return error code for any incompatible session request and no session available.
            return -ECANCELED;
        }
        else
        {
            // A session exists and the request should be handled by virtual fs.
            return 1;
        }
    }

    /**
     * Checks file create requests for any session-related metadata activity.
     * @return 0 if request succesfully was interpreted by session control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int session_check_create(const char *path)
    {
        if (strcmp(path, SESSION_METAFILE_PATH) == 0)
        {
            if (default_session)
                return -EEXIST;
            else
                return start();
        }
        else if (!default_session)
        {
            // Return error code for any incompatible session request and no session available.
            return -ECANCELED;
        }
        else
        {
            // A session exists and the request should be handled by virtual fs.
            return 1;
        }
    }

    /**
     * Checks file unlink requests for any session-related metadata activity.
     * @return 0 if request succesfully was interpreted by session control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int session_check_unlink(const char *path)
    {
        if (strcmp(path, SESSION_METAFILE_PATH) == 0)
        {
            if (!default_session)
                return -ENOENT;
            else
                return stop();
        }
        else if (!default_session)
        {
            // Return error code for any incompatible session request and no session available.
            return -ECANCELED;
        }
        else
        {
            // A session exists and the request should be handled by virtual fs.
            return 1;
        }
    }

    int start()
    {
        // Silently return if session already exists.
        if (default_session.has_value())
            return -1;

        default_session.emplace(fs_session{});
        auto &session = default_session.value();

        const bool readonly = ctx.run_mode == RUN_MODE::RO;

        LOG_INFO << "Starting hpfs " << (readonly ? "RO" : "RW") << " session...";

        auto audit_logger = audit::audit_logger::create(ctx.run_mode,
                                                        ctx.log_file_path);

        CHECK_FAILURE(audit_logger, default_session);
        session.audit_logger.emplace(std::move(audit_logger.value()));

        auto virt_fs = vfs::virtual_filesystem::create(readonly,
                                                       ctx.seed_dir,
                                                       session.audit_logger.value());
        CHECK_FAILURE(virt_fs, default_session);
        session.virt_fs.emplace(std::move(virt_fs.value()));
        LOG_DEBUG << "VFS init complete.";

        if (ctx.hmap_enabled)
        {
            auto hmap_tree = hmap::tree::hmap_tree::create(session.virt_fs.value());

            CHECK_FAILURE(hmap_tree, default_session);
            session.hmap_tree.emplace(std::move(hmap_tree.value()));
            session.hmap_query.emplace(hmap::query::hmap_query(session.hmap_tree.value(),
                                                               session.virt_fs.value()));
            LOG_DEBUG << "Hashmap init complete.";
        }

        session.fuse_adapter.emplace(vfs::fuse_adapter(readonly,
                                                       session.virt_fs.value(),
                                                       session.audit_logger.value(),
                                                       session.hmap_tree));

        LOG_INFO << "hpfs " << (readonly ? "RO" : "RW") << " session started.";
        return 0;
    }

    int stop()
    {
        if (!default_session)
            return -1;

        default_session.reset();

        const bool readonly = ctx.run_mode == RUN_MODE::RO;
        LOG_INFO << "hpfs " << (readonly ? "RO" : "RW") << " session stopped.";

        return 0;
    }

    std::optional<fs_session> &get()
    {
        return default_session;
    }
} // namespace hpfs::session