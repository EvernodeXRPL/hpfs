#ifndef _HPFS_AUDIT_LOGGER_
#define _HPFS_AUDIT_LOGGER_

#include <string>
#include <string.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <vector>
#include <optional>
#include "../hpfs.hpp"
#include "../hmap/hasher.hpp"

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

    enum LOG_MODE
    {
        RO,
        RW,
        MERGE,
        PRINT,
        LOG_SYNC
    };

    enum LOCK_TYPE
    {
        SESSION_LOCK, // Used by RO/RW session to indicate existance of session.
        UPDATE_LOCK,  // Used by RW session to make updates to the header.
        MERGE_LOCK,   // Used by MERGE session to acquire exclusive access to the log.
        SYNC_LOCK     // Used by LOG_SYNC session to acquire exclusive access to the log.
    };

    struct log_header
    {
        // Begin offset of the first log record. 0 indicates there are no records.
        off_t first_record = 0;

        // Begin offset of the last log record. 0 indicates there are no records.
        off_t last_record = 0;

        // Last checkpoint offset (inclusive of the checkpointed log record).
        off_t last_checkpoint = 0;
    } __attribute__((packed));

    struct log_record_header
    {
        int64_t timestamp = 0;
        FS_OPERATION operation = FS_OPERATION::MKDIR;
        size_t vpath_len = 0;
        size_t payload_len = 0;
        size_t block_data_len = 0;
        hmap::hasher::h32 root_hash = hmap::hasher::h32_empty;
    } __attribute__((packed));

    struct log_record
    {
        off_t offset = 0; // Position of this log record within the log file.
        size_t size = 0;  // Total length of this log record.

        int64_t timestamp = 0;
        std::string vpath;
        FS_OPERATION operation = FS_OPERATION::MKDIR;
        off_t payload_offset = 0;
        size_t payload_len = 0;
        off_t block_data_offset = 0;
        size_t block_data_len = 0;
        hmap::hasher::h32 root_hash = hmap::hasher::h32_empty;
    };

    struct log_record_metrics
    {
        off_t vpath_offset;          // Offset of the stored vpath relative to log record begin offset.
        off_t payload_offset = 0;    // Payload offset relative to log record begin offset.
        off_t block_data_offset = 0; // Block data offset relative to log record begin offset.
        size_t total_size = 0;       // Total size of the record including block alignment padding bytes.
        size_t unpadded_size = 0;    // Total unpadded size of the record exluding block alignment padding bytes.
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

    // Holds information about an already appended fs operation.
    struct fs_operation_summary
    {
        std::string vpath;            // The vpath the operation applied to.
        log_record_header rh;         // The log record header that got written to the log file.
        std::vector<uint8_t> payload; // The operation payload.

        void update(std::string_view vpath, const log_record_header &rh, const iovec *payload)
        {
            this->vpath = vpath;
            this->rh = rh;

            if (payload)
            {
                this->payload.resize(payload->iov_len);
                memcpy(this->payload.data(), payload->iov_base, payload->iov_len);
            }
            else
            {
                this->payload.clear();
            }
        }
    };

    class audit_logger
    {
    private:
        bool moved = false;
        bool initialized = false; // Indicates that the instance has been initialized properly.
        const LOG_MODE mode = LOG_MODE::RO;
        std::string_view log_file_path;
        int fd = 0;                                  // The log file fd used throughout the session.
        off_t eof = 0;                               // End of file (End offset of log file).
        struct log_header header = {};               // The log file header loaded into memory.
        struct flock session_lock = {};              // Session lock placed on the log file.
        std::optional<fs_operation_summary> last_op; // Keeps track of the last-performed operation during this session to aid optimizations.

        int init();
        int write_data_bufs(const iovec *data_bufs, const int data_buf_count, const off_t begin_offset);

    public:
        int init_log_header();
        static int create(std::optional<audit_logger> &logger, const LOG_MODE mode, std::string_view log_file_path);
        audit_logger(const LOG_MODE mode, std::string_view log_file_path);
        int get_fd();
        const log_header &get_header();
        void print_log();
        int set_lock(struct flock &lock, const LOCK_TYPE type);
        int release_lock(struct flock &lock);
        int read_header();
        int commit_header();
        off_t append_log(log_record_header &log_record, std::string_view vpath, const FS_OPERATION operation, const iovec *payload_buf = NULL,
                         const iovec *data_bufs = NULL, const int data_buf_count = 0);
        int read_log_at(const off_t offset, off_t &next_offset, log_record &record);
        int read_log_record_buf_at(const off_t offset, off_t &next_offset, std::string &buf);
        int read_payload(std::vector<uint8_t> &payload, const log_record &record);
        int purge_log(const log_record &record);
        int update_log_record_hash(const off_t log_rec_start_offset, const hmap::hasher::h32 root_hash, log_record_header &rh);
        int overwrite_last_log_record_bytes(const off_t payload_write_offset, const off_t data_write_offset,
                                            const iovec *payload_buf, const iovec *data_bufs, const int data_buf_count,
                                            const size_t new_block_data_len, log_record_header &rh);
        int truncate_log_file(const off_t log_record_offset);
        std::optional<fs_operation_summary> &get_last_operation();
        static const log_record_metrics get_metrics(const log_record_header &rh);
        ~audit_logger();
    };

} // namespace hpfs::audit

#endif