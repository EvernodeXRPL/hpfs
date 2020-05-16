#ifndef _HPFS_MERGER_
#define _HPFS_MERGER_

#include <vector>
#include "logger.hpp"

namespace merger
{
    int init();
    bool log_has_records();
    int merge_log_record(const logger::log_record &record, const std::vector<uint8_t> payload);
} // namespace merger

#endif