#ifndef _HPFS_AUDIT_LOGGER_INDEX_
#define _HPFS_AUDIT_LOGGER_INDEX_

#include <string>
#include <optional>
#include <sys/stat.h>
#include "audit.hpp"
#include "../hmap/hasher.hpp"
#include "../vfs/virtual_filesystem.hpp"
#include "../hmap/tree.hpp"

namespace hpfs::audit::logger_index
{
    struct index_context
    {
        int fd = -1;              // The index file fd used throughout the session.
        off_t eof = 0;            // End of file (End offset of index file).
        bool initialized = false; // Indicates that the index has been initialized properly.
        std::string index_file_path;
        /* Because fuse is multithreaded and reading and writing processed by mulitple threads for several page blocks,
        We use two local buffers to collect read and write logs and serve the reading logas and appending logs from these buffers.
        This log read cannot be done from multiple threads by hpcore */
        std::string read_buf;  // Tempory buffer to keep reading result.
        std::string write_buf; // Tempory buffer to collect writing logs.
    };

    extern index_context index_ctx;

    int init(std::string_view file_path);

    void deinit();

    int update_log_index(const uint64_t seq_no);

    int read_last_root_hash(hmap::hasher::h32 &root_hash);

    int get_last_index_data(off_t &offset, hmap::hasher::h32 &root_hash);

    uint64_t get_last_seq_no();

    int read_offset(off_t &offset, const uint64_t seq_no);

    int read_hash(hmap::hasher::h32 &hash, const uint64_t seq_no);

    int read_log_records(std::string &buf, const uint64_t min_seq_no, const uint64_t max_seq_no = 0, const uint64_t max_size = 0);

    int append_log_records(const char *buf, const size_t size);

    int persist_log_record(audit::audit_logger &logger, vfs::virtual_filesystem &virt_fs, hmap::tree::hmap_tree &htree, const uint64_t seq_no, const audit::FS_OPERATION op,
                           const std::string &vpath, std::string_view payload, std::string_view block_data, off_t &log_offset, hmap::hasher::h32 &root_hash);

    int index_check_read(std::string_view query, char *buf, size_t *size, const off_t offset);

    int index_check_write(std::string_view query, const char *buf, size_t *size, const off_t offset);

    int index_check_open(std::string_view query);

    int index_check_flush(std::string_view query);

    int index_check_getattr(std::string_view query, struct stat *stbuf);

    int index_check_truncate(std::string_view query);

    int truncate_log_and_index_file(const uint64_t seq_no);

    int get_log_offset_from_index_file(off_t &log_offset, const uint64_t seq_no);

    off_t get_data_offset_of_index_file(const uint64_t seq_no);

} // namespace hpfs::audit::logger_index

#endif