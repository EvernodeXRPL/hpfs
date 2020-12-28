#ifndef _HPFS_VFS_VFS_
#define _HPFS_VFS_VFS_

#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <vector>
#include <stdint.h>

namespace hpfs::vfs
{
    struct vnode_mmap
    {
        void *ptr = NULL;
        size_t size = 0;
    };

    struct vdata_segment
    {
        int physical_fd = 0;
        size_t size = 0;
        off_t physical_offset = 0;
        off_t logical_offset = 0;
    };

    struct vnode
    {
        ino_t ino = 0;
        struct stat st;
        int seed_fd = 0;

        // How many data segs from the begining of list that has been mapped to memory.
        uint32_t mapped_data_segs = 0;
        std::vector<vdata_segment> data_segs;
        struct vnode_mmap mmap;

        // Max file size that has been there for this vnode throughout the log history.
        size_t max_size = 0;

        vnode()
        {
            memset(&st, 0, sizeof(struct stat));
            memset(&mmap, 0, sizeof(struct vnode_mmap));
        }
    };

} // namespace hpfs::vfs

#endif