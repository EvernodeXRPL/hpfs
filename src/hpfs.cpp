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

namespace hpfs
{

    constexpr const char *SEED_DIR_NAME = "seed";
    constexpr int DIR_PERMS = 0755;

    hpfs_context ctx;

    int init(int argc, char **argv)
    {
        if (parse_cmd(argc, argv) == -1)
        {
            std::cerr << "Invalid arguments.\n";
            std::cout << "Usage:\n"
                      << "hpfs [merge|rdlog] [fsdir]\n"
                      << "hpfs [ro|rw] [fsdir] [mountdir] hmap=[true|false]\n";
            return -1;
        }

        if (vaidate_context() == -1 || logger::init() == -1)
            return -1;

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

        if (!util::is_dir_exists(ctx.mount_dir))
        {
            // If specified mount directory does not exist, we will create it
            // now and remove it upon exit.
            if (mkdir(ctx.mount_dir.c_str(), DIR_PERMS) == -1)
                return -1;
            remove_mount_dir = true;
        }

        if (vfs::init() == -1)
        {
            ret = -1;
            goto deinit_vfs;
        }

        if (hmap::init() == -1)
        {
            ret = -1;
            goto deinit_hmap;
        }

        // This is a blocking call. This will exit when fuse_main receives a signal.
        ret = fusefs::init(arg0);

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

        if (!util::is_dir_exists(ctx.seed_dir) && mkdir(ctx.seed_dir.c_str(), DIR_PERMS) == -1)
        {
            std::cerr << "Directory " << ctx.seed_dir << " cannot be located.\n";
            return -1;
        }

        return 0;
    }

    int parse_cmd(int argc, char **argv)
    {
        if (argc == 3 || argc == 5)
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
            realpath(argv[2], buf);
            ctx.fs_dir = buf;

            if (argc == 3 && (ctx.run_mode == RUN_MODE::MERGE || ctx.run_mode == RUN_MODE::RDLOG))
            {
                return 0;
            }
            else if (argc == 5 && (ctx.run_mode == RUN_MODE::RO || ctx.run_mode == RUN_MODE::RW))
            {
                if (strcmp(argv[4], "hmap=true") == 0)
                    ctx.hmap_enabled = true;
                else if (strcmp(argv[4], "hmap=false") == 0)
                    ctx.hmap_enabled = false;
                else
                    return -1;

                realpath(argv[3], buf);
                ctx.mount_dir = buf;
                return 0;
            }
        }

        return -1;
    }

} // namespace hpfs
