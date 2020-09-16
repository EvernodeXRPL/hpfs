#include <iostream>
#include <limits.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "hpfs.hpp"
#include "util.hpp"
#include "fusefs.hpp"
#include "logger.hpp"
#include "merger.hpp"
#include "vfs.hpp"
#include "hmap/hmap.hpp"
#include "tracelog.hpp"

namespace hpfs
{

    constexpr const char *SEED_DIR_NAME = "seed";
    constexpr const char *TRACE_DIR_NAME = "trace";
    constexpr const char *HMAP_DIR_NAME = "hmap";
    constexpr int DIR_PERMS = 0755;

    hpfs_context ctx;

    int init(int argc, char **argv)
    {
        if (parse_cmd(argc, argv) == -1)
        {
            std::cerr << "Invalid arguments.\n";
            std::cout << "Usage:\n"
                      << "hpfs [merge|rdlog] [fsdir] trace=[debug|info|warn|error]\n"
                      << "hpfs [ro|rw] [fsdir] [mountdir] hmap=[true|false] trace=[debug|info|warn|error]\n";
            return -1;
        }
        if (vaidate_context() == -1 || tracelog::init() == -1 || logger::init() == -1)
            return -1;

        // Register exception handler for std exceptions.
        // This needs to be done after trace log init because we are logging exceptions there.
        std::set_terminate(&std_terminate);

        int ret = 0;

        if (ctx.run_mode == RUN_MODE::RDLOG)
        {
            logger::print_log();
        }
        else if (ctx.run_mode == RUN_MODE::MERGE)
        {
            ret = merger::init();
        }
        else
        {
            ret = run_ro_rw_session(argv[0]);
        }

        logger::deinit();
        return ret;
    }

    int run_ro_rw_session(char *arg0)
    {
        int ret = 0;
        bool remove_mount_dir = false;

        LOG_INFO << "Starting hpfs " << ((ctx.run_mode == RUN_MODE::RW) ? "RW" : "RO") << " session...";

        if (vfs::init() == -1)
        {
            ret = -1;
            goto deinit_vfs;
        }

        LOG_DEBUG << "VFS init complete.";

        if (hmap::init() == -1)
        {
            ret = -1;
            goto deinit_hmap;
        }

        LOG_DEBUG << "Hashmap init complete.";

        // Check and create fuse mount dir.
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

        LOG_INFO << "hpfs " << ((ctx.run_mode == RUN_MODE::RW) ? "RW" : "RO") << " session started.";

        // This is a blocking call. This will exit when fuse_main receives a signal.
        ret = fusefs::init(arg0);

        LOG_INFO << "Ended FUSE session.";

    deinit_hmap:
        hmap::deinit();
    deinit_vfs:
        vfs::deinit();

        if (remove_mount_dir)
            rmdir(ctx.mount_dir.c_str());

        return ret;
    }

    int vaidate_context()
    {
        if (!util::is_dir_exists(ctx.fs_dir))
        {
            std::cerr << "Directory " << ctx.fs_dir << " does not exist.\n";
            return -1;
        }

        ctx.seed_dir.append(ctx.fs_dir).append("/").append(SEED_DIR_NAME);
        ctx.trace_dir.append(ctx.fs_dir).append("/").append(TRACE_DIR_NAME);
        ctx.hmap_dir.append(ctx.fs_dir).append("/").append(HMAP_DIR_NAME);

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

        if (ctx.hmap_enabled &&
            !util::is_dir_exists(ctx.hmap_dir) && mkdir(ctx.hmap_dir.c_str(), DIR_PERMS) == -1)
        {
            std::cerr << "Directory " << ctx.hmap_dir << " cannot be located.\n";
            return -1;
        }

        return 0;
    }

    int parse_cmd(int argc, char **argv)
    {
        if (argc == 4 || argc == 6)
        {
            if (strcmp(argv[1], "ro") == 0)
                ctx.run_mode = RUN_MODE::RO;
            else if (strcmp(argv[1], "rw") == 0)
                ctx.run_mode = RUN_MODE::RW;
            else if (strcmp(argv[1], "merge") == 0)
                ctx.run_mode = RUN_MODE::MERGE;
            else if (strcmp(argv[1], "rdlog") == 0)
                ctx.run_mode = RUN_MODE::RDLOG;
            else
                return -1;

            char buf[PATH_MAX];
            const char *resolved = realpath(argv[2], buf);
            ctx.fs_dir = buf;

            const char *trace_arg = argv[argc - 1];
            if (strcmp(trace_arg, "trace=debug") == 0)
                ctx.trace_level = TRACE_LEVEL::DEBUG;
            else if (strcmp(trace_arg, "trace=none") == 0)
                ctx.trace_level = TRACE_LEVEL::NONE;
            else if (strcmp(trace_arg, "trace=info") == 0)
                ctx.trace_level = TRACE_LEVEL::INFO;
            else if (strcmp(trace_arg, "trace=warn") == 0)
                ctx.trace_level = TRACE_LEVEL::WARN;
            else if (strcmp(trace_arg, "trace=error") == 0)
                ctx.trace_level = TRACE_LEVEL::ERROR;
            else
                return -1;

            if (argc == 4 && (ctx.run_mode == RUN_MODE::MERGE || ctx.run_mode == RUN_MODE::RDLOG))
            {
                return 0;
            }
            else if (argc == 6 && (ctx.run_mode == RUN_MODE::RO || ctx.run_mode == RUN_MODE::RW))
            {
                if (strcmp(argv[4], "hmap=true") == 0)
                    ctx.hmap_enabled = true;
                else if (strcmp(argv[4], "hmap=false") == 0)
                    ctx.hmap_enabled = false;
                else
                    return -1;

                const char *resolved = realpath(argv[3], buf);
                ctx.mount_dir = buf;
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
