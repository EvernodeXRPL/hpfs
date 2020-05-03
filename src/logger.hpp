#ifndef _HPFS_LOGGER_
#define _HPFS_LOGGER_

#include <string>
#include <sys/uio.h>
#include <fcntl.h>

namespace logger
{

enum FS_OPERATION
{
    MKDIR = 1,
    RMDIR = 2,
    RENAME = 3,
    UNLINK = 6,
    CHMOD = 7,
    CHOWN = 8,
    CREATE = 10,
    WRITE = 11,
    TRUNCATE = 12
};

struct log_header
{
    uint16_t version;
    off_t first_record;
    off_t last_record;
    off_t last_checkpoint;
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
off_t get_eof_offset();
int set_lock(flock &lock, bool is_rwlock, const off_t start, const off_t len);
int release_lock(flock &lock);
int read_header(log_header &lh);
int commit_header(log_header &lh);
int append_log(std::string_view path, const FS_OPERATION operation, const log_record_payload payload);
int read_log_at(const off_t offset, off_t &next_offset, log_record &record);
int read_payload(std::string &payload, const log_record &record);

} // namespace logger

#endif