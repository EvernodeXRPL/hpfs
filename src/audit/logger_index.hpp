#ifndef _HPFS_AUDIT_LOGGER_INDEX_
#define _HPFS_AUDIT_LOGGER_INDEX_

#include <string>
#include <optional>
#include "../hmap/hasher.hpp"

namespace hpfs::audit::logger_index
{
    int init(std::string_view file_path);

    void deinit();

    int update_log_index();

    int read_last_root_hash(hmap::hasher::h32 &root_hash);

    uint64_t get_last_seq_no();

    int handle_log_index_control(std::string_view query);
} // namespace hpfs::audit::logger_index

#endif