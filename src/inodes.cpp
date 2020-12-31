#include <sys/types.h>
#include <atomic>

namespace hpfs::inodes
{
    std::atomic<ino_t> next_ino = 2;

    ino_t next()
    {
        return next_ino++;
    }
} // namespace hpfs::inodes