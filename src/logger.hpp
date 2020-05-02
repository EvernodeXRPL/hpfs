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

struct log_header
{
    uint16_t version;
    off_t first_offset;
    off_t last_offset;
    off_t last_checkpoint_offset;
};

struct log_record_header
{
    int64_t timestamp;
    FS_OPERATION operation;
    size_t path_len;
    uint64_t payload_len;
};

struct log_record
{
    off_t offset;   // Position of this log record within the log file.
    size_t size;    // Total length of this log record.

    int64_t timestamp;
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
void deinit();
void print_log();
off_t get_first_offset();
off_t get_last_offset();
off_t get_last_checkpoint_offset();
int update_header(const off_t first_offset, const off_t last_offset, const off_t last_checkpoint_offset);
int append_log(std::string_view path, const FS_OPERATION operation, const log_record_payload payload);
int read_log_at(const off_t offset, off_t &next_offset, log_record &record);

} // namespace logger

#endif