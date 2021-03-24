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
} // namespace hpfs::audit::logger_index

#endif