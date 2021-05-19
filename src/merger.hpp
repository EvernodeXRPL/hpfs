#ifndef _HPFS_MERGER_
#define _HPFS_MERGER_

#include <vector>
#include "audit/audit.hpp"

namespace hpfs::merger
{
    int init();
    void deinit();
    void signal_handler(int signum);
    void merger_loop();
    int merge_log_front(hpfs::audit::audit_logger &logger);
    int merge_log_record(const hpfs::audit::log_record &record, const std::vector<uint8_t> payload);
} // namespace merger

#endif