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
    std::optional<fs_session> default_session;

    int start()
    {
        // Silently return if session already exists.
        if (default_session.has_value())
            return 0;

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
        default_session.reset();
        return 0;
    }

    std::optional<fs_session> &get()
    {
        return default_session;
    }
} // namespace hpfs::session