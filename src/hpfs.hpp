#ifndef _HPFS_HPFS_
#define _HPFS_HPFS_

#include <string>

namespace hpfs
{
    enum RUN_MODE
    {
        RO,
        RW,
        MERGE,
        RDLOG
    };

    enum TRACE_LEVEL
    {
        NONE,
        DEBUG,
        INFO,
        WARN,
        ERROR
    };

    struct hpfs_context
    {
        RUN_MODE run_mode;
        TRACE_LEVEL trace_level;
        std::string fs_dir;
        std::string seed_dir;
        std::string mount_dir;
        bool hmap_enabled;
    };
    extern hpfs_context ctx;

    int init(int argc, char **argv);
    int run_ro_rw_session(char *arg0);
    int vaidate_context();
    int parse_cmd(int argc, char **argv);
} // namespace hpfs

#endif