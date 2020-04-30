#include <fcntl.h>
#include "hpfs.hpp"
#include "logger.hpp"

namespace logger
{

constexpr const char *LOGFILE = "log.hpfs";

int fd = 0;
uint64_t first_offset = 0, last_offset = 0; // First and last record offsets.

int init()
{
    // Open or create the log file.
    //open()
}

int append_log(const char *path, const FS_OPERATION operation, const payload_def payload)
{

}

} // namespace logger