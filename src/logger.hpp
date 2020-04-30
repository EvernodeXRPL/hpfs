#ifndef _HPFS_LOGGER_
#define _HPFS_LOGGER_

#include <string>
#include <sys/uio.h>

namespace logger
{

enum FS_OPERATION
{
    WRITE = 11
};

struct payload_def
{
    const uint64_t payload_len;
    const iovec *bufs;
    const uint8_t buf_count;
};

int init();
int append_log(const char *path, const FS_OPERATION operation, const payload_def payload);

} // namespace logger

#endif