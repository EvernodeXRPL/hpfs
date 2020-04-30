#include "hpfs.hpp"
#include "logger.hpp"

namespace vfs
{

int create(const char *path, mode_t mode)
{
    return 0;
}

int write(const char *path, const char *buf, size_t size, off_t offset)
{
    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW)
    {
        // Payload length is payload header length + write buffer size
        const uint64_t payload_len = sizeof(size_t) + sizeof(off_t) + size;

        uint8_t header_buf[sizeof(size_t) + sizeof(off_t)];
        *header_buf = size;
        *(header_buf + sizeof(size_t)) = offset;

        iovec bufs[2];
        bufs[0].iov_base = header_buf;
        bufs[0].iov_len = sizeof(header_buf);
        bufs[1].iov_base = (void *)buf;
        bufs[1].iov_len = size;

        return logger::append_log(path, logger::FS_OPERATION::WRITE, {payload_len, bufs, 2});
    }
    return -1;
}

} // namespace vfs