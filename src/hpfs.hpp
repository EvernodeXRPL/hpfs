#ifndef _HPFS_HPFS_
#define _HPFS_HPFS_

#include <string>
#include <sys/stat.h>
#include <CLI/CLI.hpp>

namespace hpfs
{
    enum RUN_MODE
    {
        FS,     // rw/ro filesystem sessions.
        RDLOG,  // Log printing.
        VERSION // Version printing.
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
        bool merge_enabled;
        std::string fs_dir;
        std::string seed_dir;
        std::string mount_dir;
        std::string hmap_dir;
        std::string trace_dir;
        std::string log_file_path;
        std::string log_index_file_path;
        struct stat default_stat; // Stat used as a base stat for virtual entries.

        uid_t self_uid = 0;
        gid_t self_gid = 0;

        // If set, these uid/gid will be allowed to access the mount in addition to the mount owner.
        bool ugid_enabled = false;
        uid_t allowed_uid = 0;
        gid_t allowed_gid = 0;
    };
    extern hpfs_context ctx;

    int init(int argc, char **argv);
    int run_ro_rw_session(char *arg0);
    int vaidate_context();
    int parse_cmd(int argc, char **argv);
    int read_ugid_arg(std::string_view arg);
    void std_terminate() noexcept;

} // namespace hpfs

#endif