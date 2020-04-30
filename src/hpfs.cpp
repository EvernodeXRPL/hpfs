#include <iostream>
#include <limits.h>
#include <string.h>
#include "hpfs.hpp"
#include "util.hpp"
#include "fusefs.hpp"
#include "logger.hpp"
#include "merger.hpp"

namespace hpfs
{

hpfs_context ctx;

int init(int argc, char **argv)
{
    if (parse_cmd(argc, argv) == -1)
    {
        std::cerr << "Invalid arguments.\n";
        std::cout << "Usage:\n"
                  << "hpfs [rw|merge] [fsdir]\n"
                  << "hpfs ro [fsdir] [mountdir]\n";
        return -1;
    }

    if (vaidate_context() == -1 || logger::init() == -1)
        return -1;

    if (ctx.run_mode == RUN_MODE::Merge)
    {
        //if (merger::init() == -1)
        //    return -1;
    }
    else
    {
        if (fusefs::init() == -1)
            return -1;
    }

    return 0;
}

int vaidate_context()
{
    if (!util::is_dir_exists(ctx.fs_dir))
    {
        std::cerr << "Directory " << ctx.fs_dir << " does not exist.\n";
        return -1;
    }

    if (ctx.run_mode != RUN_MODE::Merge && !util::is_dir_exists(ctx.mount_dir))
    {
        std::cerr << "Directory " << ctx.mount_dir << " does not exist.\n";
        return -1;
    }

    return 0;
}

int parse_cmd(int argc, char **argv)
{
    if (argc == 3 || argc == 4)
    {
        if (strcmp(argv[1], "ro") == 0)
            ctx.run_mode = RUN_MODE::RO;
        else if (strcmp(argv[1], "rw") == 0)
            ctx.run_mode = RUN_MODE::RW;
        else if (strcmp(argv[1], "merge") == 0)
            ctx.run_mode = RUN_MODE::Merge;
        else
            return -1;

        char buf[PATH_MAX];
        realpath(argv[2], buf);
        ctx.fs_dir = buf;

        if (argc == 3 && ctx.run_mode == RUN_MODE::Merge)
        {
            return 0;
        }
        else if (argc == 4 && (ctx.run_mode == RUN_MODE::RO || ctx.run_mode == RUN_MODE::RW))
        {
            realpath(argv[3], buf);
            ctx.mount_dir = buf;
            return 0;
        }
    }

    return -1;
}

} // namespace hpfs
