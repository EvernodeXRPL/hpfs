#ifndef _HPFS_AUDIT_LOGGER_
#define _HPFS_AUDIT_LOGGER_

#include <string>
#include <sys/uio.h>
#include <fcntl.h>
#include <vector>
#include <optional>
#include "hpfs.hpp"

namespace hpfs::audit
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

    enum LOCK_TYPE
    {
        SESSION_LOCK, // Used by RO/RW session to indicate existance of session.
        UPDATE_LOCK,  // Used by RW session to make updates to the header.
        MERGE_LOCK    // Used by MERGE session to acquire exclusive access to the log.
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

    class audit_logger
    {
    private:
        bool moved = false;
        bool initialized = false; // Indicates that the instance has been initialized properly.
        const hpfs::RUN_MODE run_mode;
        std::string_view log_file_path;
        int fd = 0;                // The log file fd used throughout the session.
        off_t eof = 0;             // End of file (End offset of log file).
        struct log_header header;  // The log file header loaded into memory.
        struct flock session_lock; // Session lock placed on the log file.

        audit_logger(const hpfs::RUN_MODE run_mode, std::string_view log_file_path);
        int init();
        int load_log_file();

    public:
        static std::optional<audit_logger> create(const hpfs::RUN_MODE run_mode, std::string_view log_file_path);
        audit_logger(const audit_logger &) = delete; // No copy constructor;
        audit_logger(audit_logger &&old);
        int get_fd();
        const log_header &get_header();
        void print_log();
        int set_lock(struct flock &lock, const LOCK_TYPE type);
        int release_lock(struct flock &lock);
        int read_header();
        int commit_header();
        int append_log(std::string_view vpath, const FS_OPERATION operation, const iovec *payload_buf = NULL,
                       const iovec *block_bufs = NULL, const int block_buf_count = 0);
        int read_log_at(const off_t offset, off_t &next_offset, log_record &record);
        int read_payload(std::vector<uint8_t> &payload, const log_record &record);
        int purge_log(const log_record &record);
        ~audit_logger();
    };

} // namespace hpfs::audit

#endif