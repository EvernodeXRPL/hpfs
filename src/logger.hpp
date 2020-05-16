#ifndef _HPFS_LOGGER_
#define _HPFS_LOGGER_

#include <string>
#include <sys/uio.h>
#include <fcntl.h>
#include <vector>

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

        // Begin offset of the first log record. 0 indicates there are no records.
        off_t first_record;

        // Begin offset of the last log record. 0 indicates there are no records.
        off_t last_record;

        // Last checkpoint offset (inclusive of the checkpointed log record).
        off_t last_checkpoint;
    };

    struct log_record_header
    {
        int64_t timestamp;
        FS_OPERATION operation;
        size_t vpath_len;
        size_t payload_len;
        size_t block_data_padding_len;
        size_t block_data_len;
    };

    struct log_record
    {
        off_t offset; // Position of this log record within the log file.
        size_t size;  // Total length of this log record.

        int64_t timestamp;
        std::string vpath;
        FS_OPERATION operation;
        off_t payload_offset;
        size_t payload_len;
        off_t block_data_offset;
        size_t block_data_len;
    };

    struct op_write_payload_header
    {
        size_t size = 0;  // Original write buffer size.
        off_t offset = 0; // Original write offset.

        size_t mmap_block_size = 0;  // Memory map block size.
        off_t mmap_block_offset = 0; // Memory map placement offset for the block data.

        // Position of the original write buffer relative to block data start.
        // Used for merge operation.
        off_t data_offset_in_block = 0;
    };

    struct op_truncate_payload_header
    {
        size_t size = 0;
        size_t mmap_block_size = 0;  // Memory map block size.
        off_t mmap_block_offset = 0; // Memory map placement offset for the block data.
    };

    extern int fd;

    int init();
    void deinit();
    void print_log();
    off_t get_eof();
    int set_lock(struct flock &lock, const bool is_rwlock, const off_t start, const off_t len);
    int release_lock(struct flock &lock);
    int read_header(log_header &lh);
    int commit_header(log_header &lh);
    int append_log(std::string_view vpath, const FS_OPERATION operation, const iovec *payload_buf = NULL,
                   const iovec *block_bufs = NULL, const int block_buf_count = 0);
    int read_log_at(const off_t offset, off_t &next_offset, log_record &record);
    int read_payload(std::vector<uint8_t> &payload, const log_record &record);

} // namespace logger

#endif