#include <iostream>
#include <limits.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <CLI/CLI.hpp>
#include "hpfs.hpp"
#include "util.hpp"
#include "fusefs.hpp"
#include "merger.hpp"
#include "tracelog.hpp"
#include "audit/audit.hpp"
#include "session.hpp"
#include "audit/logger_index.hpp"
#include "version.hpp"

namespace hpfs
{
    constexpr const char *SEED_DIR_NAME = "seed";
    constexpr const char *TRACE_DIR_NAME = "trace";
    constexpr const char *HMAP_DIR_NAME = "hmap";
    constexpr const char *LOG_FILE_NAME = "log.hpfs";
    constexpr const char *LOG_INDEX_FILE_NAME = "log.hpfs.idx";
    constexpr int DIR_PERMS = 0755;

    hpfs_context ctx;

    int init(int argc, char **argv)
    {
        const int n = 1;
        if (*(char *)&n != 1)
        {
            std::cerr << "Bigendian not supported.\n";
            return -1;
        }

        if (parse_cmd(argc, argv) == -1)
            return -1;

        if (version::init() == -1)
            return -1;

        if (ctx.run_mode == RUN_MODE::VERSION)
        {
            // Print the version
            std::cout << version::HPFS_VERSION << std::endl;
            return 0;
        }

        if (vaidate_context() == -1 || tracelog::init() == -1)
            return -1;

        // Populate default stat using seed dir stat.
        if (stat(ctx.seed_dir.data(), &ctx.default_stat) == -1)
        {
            LOG_ERROR << errno << ":Failed to load seed dir stat.";
            return -1;
        }

        LOG_INFO << "hpfs " << version::HPFS_VERSION;

        ctx.self_uid = getuid();
        ctx.self_gid = getgid();

        ctx.default_stat.st_ino = 0;
        ctx.default_stat.st_nlink = 0;
        ctx.default_stat.st_size = 0;
        ctx.default_stat.st_mode ^= S_IFDIR; // Negate the entry type.

        // Register exception handler for std exceptions.
        // This needs to be done after trace log init because we are logging exceptions there.
        std::set_terminate(&std_terminate);

        if (ctx.run_mode == RUN_MODE::RDLOG)
        {
            std::optional<audit::audit_logger> audit_logger;
            if (audit::audit_logger::create(audit_logger, audit::LOG_MODE::PRINT, ctx.log_file_path) == -1)
                return -1;

            audit_logger->print_log();
            return 0;
        }
        else
        {
            if (merger::init() == -1 || audit::logger_index::init(ctx.log_index_file_path))
                return -1;

            if (run_ro_rw_session(argv[0]) == -1)
            {
                audit::logger_index::deinit();
                merger::deinit();
                return -1;
            }

            audit::logger_index::deinit();
            merger::deinit();
            return 0;
        }
    }

    int run_ro_rw_session(char *arg0)
    {
        // Check and create fuse mount dir.
        bool remove_mount_dir = false;
        if (!util::is_dir_exists(ctx.mount_dir))
        {
            // If specified mount directory does not exist, we will create it
            // now and remove it upon exit.
            if (mkdir(ctx.mount_dir.c_str(), DIR_PERMS) == -1)
            {
                LOG_ERROR << errno << ": Error creating mount dir: " << ctx.mount_dir;
                return -1;
            }
            remove_mount_dir = true;
            LOG_DEBUG << "Mount dir created: " << ctx.mount_dir;
        }

        // This is a blocking call. This will exit when fuse_main receives a signal.
        LOG_INFO << "Starting FUSE session... (access: " << ctx.self_uid << ":" << ctx.self_gid
                 << (ctx.ugid_enabled ? (" + " + std::to_string(ctx.allowed_uid) + ":" + std::to_string(ctx.allowed_gid)) : "") << ")";
        const int ret = fusefs::init(arg0);
        LOG_INFO << "Ended FUSE session.";

        // Even though FUSE is up, we do not automatically create a hpfs session. In order to be able to
        // serve filesystem requests, user needs to start a session by sending a getattr request to a
        // reserved filename format. (look in fusefs.cpp fs_getattr())

        // Stop any ongoing fs sessions (if exists).
        session::stop_all();

        if (remove_mount_dir)
            rmdir(ctx.mount_dir.c_str());

        return ret;
    }

    int vaidate_context()
    {
        if (!ctx.run_mode)
            return 0;

        if (!util::is_dir_exists(ctx.fs_dir))
        {
            std::cerr << "Directory " << ctx.fs_dir << " does not exist.\n";
            return -1;
        }

        ctx.seed_dir.append(ctx.fs_dir).append("/").append(SEED_DIR_NAME);
        ctx.trace_dir.append(ctx.fs_dir).append("/").append(TRACE_DIR_NAME);
        ctx.hmap_dir.append(ctx.fs_dir).append("/").append(HMAP_DIR_NAME);
        ctx.log_file_path.append(ctx.fs_dir).append("/").append(LOG_FILE_NAME);
        ctx.log_index_file_path.append(ctx.fs_dir).append("/").append(LOG_INDEX_FILE_NAME);

        if (!util::is_dir_exists(ctx.seed_dir) && mkdir(ctx.seed_dir.c_str(), DIR_PERMS) == -1)
        {
            std::cerr << "Directory " << ctx.seed_dir << " cannot be located.\n";
            return -1;
        }

        if (ctx.trace_level != TRACE_LEVEL::NONE &&
            !util::is_dir_exists(ctx.trace_dir) && mkdir(ctx.trace_dir.c_str(), DIR_PERMS) == -1)
        {
            std::cerr << "Directory " << ctx.trace_dir << " cannot be located.\n";
            return -1;
        }

        if (!util::is_dir_exists(ctx.hmap_dir) && mkdir(ctx.hmap_dir.c_str(), DIR_PERMS) == -1)
        {
            std::cerr << "Directory " << ctx.hmap_dir << " cannot be located.\n";
            return -1;
        }

        return 0;
    }

    int parse_cmd(int argc, char **argv)
    {
        // Initialize CLI.
        CLI::App app("Hot Pocket File System");
        app.set_help_all_flag("--help-all", "Expand all help");

        // Initialize subcommands.
        CLI::App *version = app.add_subcommand("version", "hpfs version");
        CLI::App *fs = app.add_subcommand("fs", "Virtual filesystem mode");
        CLI::App *rdlog = app.add_subcommand("rdlog", "Log reader mode (for debugging)");

        // Initialize options.
        std::string fs_dir, mount_dir, ugid, trace_mode;
        bool is_merge_enabled;

        // fs
        fs->add_option("-f,--fs-dir", fs_dir, "Filesystem metadata dir")->required()->check(CLI::ExistingPath);
        fs->add_option("-m,--mount-dir", mount_dir, "Virtual filesystem mount dir")->required()->check(CLI::ExistingPath);
        fs->add_option("-g,--merge", is_merge_enabled, "Whether the log merger is enabled or not");
        fs->add_option("-u,--ugid", ugid, "Additional user group access in \"uid:gid\" format. Default: empty");
        fs->add_option("-t,--trace", trace_mode, "Trace mode")->check(CLI::IsMember({"dbg", "none", "inf", "wrn", "err"}))->default_str("wrn");

        // rdlog
        rdlog->add_option("-f,--fs-dir", fs_dir, "Filesystem metadata dir")->required()->check(CLI::ExistingPath);
        rdlog->add_option("-t,--trace", trace_mode, "Trace mode")->check(CLI::IsMember({"dbg", "none", "inf", "wrn", "err"}))->default_str("wrn");

        CLI11_PARSE(app, argc, argv);

        // Verifying subcommands.
        if (version->parsed())
        {
            ctx.run_mode = RUN_MODE::VERSION;
            return 0;
        }
        else if (fs->parsed() || rdlog->parsed())
        {
            char buf[PATH_MAX];

            // Common options for fs and rdlog.
            if (!fs_dir.empty())
            {
                realpath(fs_dir.c_str(), buf);
                ctx.fs_dir = buf;
            }

            if (!trace_mode.empty())
            {
                if (trace_mode == "dbg")
                    ctx.trace_level = TRACE_LEVEL::DEBUG;
                else if (trace_mode == "none")
                    ctx.trace_level = TRACE_LEVEL::NONE;
                else if (trace_mode == "inf")
                    ctx.trace_level = TRACE_LEVEL::INFO;
                else if (trace_mode == "wrn")
                    ctx.trace_level = TRACE_LEVEL::WARN;
                else if (trace_mode == "err")
                    ctx.trace_level = TRACE_LEVEL::ERROR;
                else
                    return -1;
            }

            // rdlog & fs operations.
            if (rdlog->parsed())
            {
                ctx.run_mode = RUN_MODE::RDLOG;
                return 0;
            }
            else if (fs->parsed())
            {
                ctx.run_mode = RUN_MODE::FS;
                ctx.merge_enabled = is_merge_enabled;

                // ugid arg (optional) specified uid/gid combination that is allowed to access the fuse mount
                // in addition to the mount owner.
                if (!mount_dir.empty())
                {
                    realpath(mount_dir.c_str(), buf);
                    ctx.mount_dir = buf;
                }

                return 0;
            }
        }

        ctx.run_mode = RUN_MODE::HELP;
        std::cout << app.help();
        return -1;
    }

    int read_ugid_arg(std::string_view arg)
    {
        const std::vector<std::string> ids = util::split_string(arg, ":");
        if (ids.size() == 2)
        {
            const int uid = atoi(ids[0].c_str());
            const int gid = atoi(ids[1].c_str());

            if (uid > 0 && gid > 0)
            {
                ctx.ugid_enabled = true;
                ctx.allowed_uid = uid;
                ctx.allowed_gid = gid;
                return 0;
            }
            else if (uid == 0 && gid == 0)
            {
                return 0;
            }
        }

        return -1;
    }

    /**
     * Global exception handler for std exceptions.
     */
    void std_terminate() noexcept
    {
        std::exception_ptr exptr = std::current_exception();
        if (exptr != 0)
        {
            try
            {
                std::rethrow_exception(exptr);
            }
            catch (std::exception &ex)
            {
                LOG_ERROR << "std error: " << ex.what();
            }
            catch (...)
            {
                LOG_ERROR << "std error: Terminated due to unknown exception.";
            }
        }
        else
        {
            LOG_ERROR << "std error: Terminated due to unknown reason.";
        }

        exit(1);
    }

} // namespace hpfs
