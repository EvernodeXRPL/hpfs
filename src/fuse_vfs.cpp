#include <iostream>
#include <string.h>
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

int write(const char *vpath, const char *buf, size_t size, off_t offset)
{
    if (hpfs::ctx.run_mode != hpfs::RUN_MODE::RW)
        return -EACCES;

    vfs::vnode *vn;
    if (get_vnode(vpath, &vn) == -1)
        return -1;
    if (!vn)
        return -ENOENT;

    logger::op_write_payload_header wh{size, offset, 0};

    // Create block data buf
    const off_t block_data_start = util::get_block_start(offset);
    const size_t block_data_end = util::get_block_end(offset + size);
    const size_t block_data_size = block_data_end - block_data_start;

    iovec block_data_buf;
    std::vector<uint8_t> vec;

    if (block_data_start == offset && block_data_size == size)
    {
        // If the block start/end offsets are in perfect alignment,
        // log the incoming write buf as it is.
        block_data_buf = {(void *)buf, size};
    }
    else
    {
        // Construct new buf with block alignment.
        vec.resize(block_data_size);

        // Read any existing memory-mapped blocks and place on the new buf.
        if (vfs::update_vnode_mmap(*vn) == -1 || !vn->mmap.ptr)
            return -1;

        const vfs::vnode_mmap &mmap = vn->mmap;
        if (mmap.ptr && mmap.size > block_data_start)
        {
            off_t read_len = block_data_size;
            if ((block_data_start + read_len) > mmap.size)
                read_len = mmap.size - block_data_start;

            memcpy(vec.data(), (uint8_t *)mmap.ptr + block_data_start, read_len);
        }

        // Overlay the incoming write buf.
        wh.buf_offset_in_block_data = offset - block_data_start; // Real data offset within the block buf.
        memcpy(vec.data() + wh.buf_offset_in_block_data, buf, size);

        // Add the block buf to log record payload.
        block_data_buf = {vec.data(), block_data_size};
    }

    iovec payload{&wh, sizeof(wh)};
    if (logger::append_log(vpath, logger::FS_OPERATION::WRITE, &payload, &block_data_buf) == -1 ||
        vfs::build_vfs() == -1)
        return -1;

    return size;
}

} // namespace fuse_vfs