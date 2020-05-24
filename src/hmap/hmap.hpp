#ifndef _HPFS_HMAP_HMAP_
#define _HPFS_HMAP_HMAP_

#include <string>

namespace hmap
{
    int init();
    void deinit();
    int read_hmap_file();
}

#endif