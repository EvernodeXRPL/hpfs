#ifndef _HPFS_AUDIT_LOGGER_INDEX_
#define _HPFS_AUDIT_LOGGER_INDEX_

#include <string>
#include <optional>
#include <sys/stat.h>
#include "../hmap/hasher.hpp"

namespace hpfs::audit::logger_index
{
    int init(std::string_view file_path);

    void deinit();

    int update_log_index();

    int read_last_root_hash(hmap::hasher::h32 &root_hash);

    uint64_t get_last_seq_no();

    int index_check_write(std::string_view query);

    int index_check_open(std::string_view query);

    int index_check_getattr(std::string_view query, struct stat *stbuf);

    int index_check_truncate(const char *path);

    int truncate_log_and_index_file(const uint64_t seq_no);

    int get_log_offset_from_index_file(off_t &log_offset, const uint64_t seq_no);

    off_t get_data_offset_of_index_file(const uint64_t seq_no);
    
} // namespace hpfs::audit::logger_index

#endif