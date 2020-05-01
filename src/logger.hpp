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

struct log_record
{
    off_t offset;
    int64_t timestamp;
    bool is_checkpoint;
    std::string path;
    FS_OPERATION operation;
    uint64_t payload_len;
    off_t payload_offset;
};

struct log_record_payload
{
    const iovec *bufs;
    const uint8_t buf_count;
};

int init();
off_t get_first_offset();
off_t get_last_offset();
off_t get_last_checkpoint_offset();
int append_log(std::string_view path, const FS_OPERATION operation, const log_record_payload payload);
int read_log_at(const off_t offset, off_t &next_offset, log_record &record);

} // namespace logger

#endif