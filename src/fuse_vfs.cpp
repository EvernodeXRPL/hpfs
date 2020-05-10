#include "vfs.hpp"
#include "hpfs.hpp"

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

int mkdir(const char *vpath, mode_t mode)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    vfs::vnode *vn;
    if (vfs::get(vpath, &vn) == -1)
        return -1;
    if (vn)
        return -EEXIST;

    iovec payload{&mode, sizeof(mode)};
    if (logger::append_log(vpath, logger::FS_OPERATION::MKDIR, &payload) == -1 ||
        vfs::build_vfs() == -1)
        return -1;

    return 0;
}

int rmdir(const char *vpath)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -1;

    vfs::vnode *vn;
    if (vfs::get(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    if (logger::append_log(vpath, logger::FS_OPERATION::RMDIR) == -1 ||
        vfs::build_vfs() == -1)
        return -1;

    return 0;
}

} // namespace fuse_vfs