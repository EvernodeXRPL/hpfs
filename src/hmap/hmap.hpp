#ifndef _HPFS_HMAP_HMAP_
#define _HPFS_HMAP_HMAP_

#include <string>
#include <vector>
#include "h32.hpp"

namespace hmap
{
    struct file_hmap
    {
        h32 file_hash;
        std::vector<h32> block_hashes;
    };

    int init();
    void deinit();
    int read_hmap_file();
    int calculate_dir_hash(h32 &hash, const std::string &vpath);
    int calculate_file_hash(h32 &hash, const std::string &vpath);
}

#endif