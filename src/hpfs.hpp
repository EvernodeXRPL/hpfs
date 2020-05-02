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

struct hpfs_context
{
    RUN_MODE run_mode;
    std::string fs_dir;
    std::string mount_dir;
    std::string seed_dir;
};
extern hpfs_context ctx;

int init(int argc, char **argv);
int vaidate_context();
int parse_cmd(int argc, char **argv);
}

#endif