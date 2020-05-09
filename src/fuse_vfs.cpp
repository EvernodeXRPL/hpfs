#include "vfs.hpp"

namespace fuse_vfs
{
int getattr(const char *vpath, struct stat *stbuf)
{
    vfs::vnode *vn;
    if (vfs::get(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    *stbuf = vn->st;
    return 0;
}
} // namespace fuse_vfs