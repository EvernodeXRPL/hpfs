#include <iostream>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include "vfs.hpp"
#include "hpfs.hpp"
#include "util.hpp"

namespace fuse_vfs
{

int getattr(const char *vpath, struct stat *stbuf)
{
    vfs::vnode *vn;
    if (vfs::get_vnode(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    *stbuf = vn->st;
    return 0;
}

int readdir(const char *vpath, vfs::vdir_children_map &children)
{
    vfs::vnode *vn;
    if (vfs::get_vnode(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;
    if (!S_ISDIR(vn->st.st_mode))
        return -ENOTDIR;

    return vfs::get_dir_children(vpath, children);
}

int mkdir(const char *vpath, mode_t mode)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -EACCES;

    vfs::vnode *vn;
    if (vfs::get_vnode(vpath, &vn) == -1)
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
        return -EACCES;

    vfs::vnode *vn;
    if (vfs::get_vnode(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    if (logger::append_log(vpath, logger::FS_OPERATION::RMDIR) == -1 ||
        vfs::build_vfs() == -1)
        return -1;

    return 0;
}

int rename(const char *from_vpath, const char *to_vpath)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -EACCES;

    vfs::vnode *vn;
    if (vfs::get_vnode(from_vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    iovec payload{(void *)to_vpath, strlen(to_vpath) + 1};
    if (logger::append_log(from_vpath, logger::FS_OPERATION::RENAME, &payload) == -1 ||
        vfs::build_vfs() == -1)
        return -1;

    return 0;
}

int unlink(const char *vpath)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -EACCES;

    vfs::vnode *vn;
    if (vfs::get_vnode(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    if (logger::append_log(vpath, logger::FS_OPERATION::UNLINK) == -1 ||
        vfs::build_vfs() == -1)
        return -1;

    return 0;
}

int create(const char *vpath, mode_t mode)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -EACCES;

    vfs::vnode *vn;
    if (vfs::get_vnode(vpath, &vn) == -1)
        return -1;
    if (vn)
        return -EEXIST;

    iovec payload{&mode, sizeof(mode)};
    if (logger::append_log(vpath, logger::FS_OPERATION::CREATE, &payload) == -1 ||
        vfs::build_vfs() == -1)
        return -1;

    return 0;
}

int read(const char *vpath, char *buf, size_t size, off_t offset)
{
    vfs::vnode *vn;
    if (get_vnode(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    if (vfs::update_vnode_mmap(*vn) == -1 || !vn->mmap.ptr)
        return -1;

    size_t read_len = size;
    if ((offset + size) > vn->st.st_size)
        read_len = vn->st.st_size - offset;

    memcpy(buf, (uint8_t *)vn->mmap.ptr + offset, read_len);

    return read_len;
}

int write(const char *vpath, const char *buf, size_t wr_size, off_t wr_start)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -EACCES;

    vfs::vnode *vn;
    if (get_vnode(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    if (vfs::update_vnode_mmap(*vn) == -1 || !vn->mmap.ptr)
        return -1;

    const size_t fsize = vn->st.st_size; // Current file size.
    const size_t wr_end = wr_start + wr_size;

    // Find the target file block offset range that should map to memory mapped file.
    const off_t block_buf_start = util::get_block_start(MIN(wr_start, fsize));
    const off_t block_buf_end = util::get_block_end(wr_start + wr_size);
    const size_t block_buf_size = block_buf_end - block_buf_start;

    logger::op_write_payload_header wh{wr_size, wr_start, block_buf_size,
                                       block_buf_start, (wr_start - block_buf_start)};

    // We maintain list of block buf segments based on where the wr_buf lies within the block buf.
    std::vector<iovec> block_buf_segs;

    // If write offset is after block start.
    if (block_buf_start < wr_start)
    {
        // If block start offset is before file end, add a segment containing existing
        // file data between block start and write offset.
        if (block_buf_start < fsize)
        {
            const size_t ex_data_len = MIN(fsize, wr_start) - block_buf_start;
            block_buf_segs.push_back({(uint8_t *)vn->mmap.ptr + block_buf_start, ex_data_len});
        }

        // If write offset is beyond file size, add a segment for NULL bytes
        // between file end and write offset.
        if (fsize < wr_start)
            block_buf_segs.push_back({NULL, (wr_start - fsize)});
    }

    // Add segment for write buf.
    block_buf_segs.push_back({(void *)buf, wr_size});

    // If write end offset is before the block end.
    if (wr_end < block_buf_end)
    {
        // If write end offset is before file end, add a segment containing existing
        // file data after write end.
        if (wr_end < fsize)
            block_buf_segs.push_back({((uint8_t *)vn->mmap.ptr + wr_end), (fsize - wr_end)});

        // If block end is beyond write end offset, add a segment for NULL bytes
        // between write end and block end.
        if (wr_end < block_buf_end)
        {
            const size_t null_data_len = block_buf_end - MAX(fsize, wr_end);
            block_buf_segs.push_back({NULL, null_data_len});
        }
    }

    iovec payload{&wh, sizeof(wh)};
    if (logger::append_log(vpath, logger::FS_OPERATION::WRITE, &payload,
                           block_buf_segs.data(), block_buf_segs.size()) == -1 ||
        vfs::build_vfs() == -1)
        return -1;

    return wr_size;
}

} // namespace fuse_vfs