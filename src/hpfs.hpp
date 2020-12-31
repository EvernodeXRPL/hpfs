#ifndef _HPFS_HPFS_
#define _HPFS_HPFS_

#include <string>
#include <sys/stat.h>

namespace hpfs
{
    enum RUN_MODE
    {
        FS,    // rw/ro filesystem sessions.
        MERGE, // Merge process mode.
        RDLOG  // Log printing.
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
        std::string hmap_dir;
        std::string trace_dir;
        std::string log_file_path;
        struct stat default_stat; // Stat used as a base stat for virtual entries.
    };
    extern hpfs_context ctx;

    int init(int argc, char **argv);
    int run_ro_rw_session(char *arg0);
    int vaidate_context();
    int parse_cmd(int argc, char **argv);
    void std_terminate() noexcept;

} // namespace hpfs

#endif