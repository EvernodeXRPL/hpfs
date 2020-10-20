#include <optional>
#include "session.hpp"
#include "vfs/virtual_filesystem.hpp"
#include "vfs/fuse_adapter.hpp"
#include "hmap/tree.hpp"
#include "hmap/query.hpp"
#include "audit.hpp"
#include "hpfs.hpp"
#include "tracelog.hpp"

namespace hpfs::session
{
    std::optional<fs_session> default_session;

    int start()
    {
        default_session.emplace(fs_session{});
        auto &session = default_session.value();

        const bool readonly = ctx.run_mode == RUN_MODE::RO;

        LOG_INFO << "Starting hpfs " << (readonly ? "RO" : "RW") << " session...";

        auto audit_logger = audit::audit_logger::create(ctx.run_mode,
                                                        ctx.log_file_path);
        if (!audit_logger)
            return -1;
        //default_session->audit_logger.swap(audit_logger);

        auto virt_fs = vfs::virtual_filesystem::create(readonly,
                                                       ctx.seed_dir,
                                                       session.audit_logger.value());
        if (!virt_fs)
            return -1;
        //session.virt_fs.swap(virt_fs);
        LOG_DEBUG << "VFS init complete.";

        if (ctx.hmap_enabled)
        {
            auto hmap_tree = hmap::tree::hmap_tree::create(session.virt_fs.value());
            if (!hmap_tree)
                return -1;
            //session.hmap_tree.swap(hmap_tree);
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