#ifndef _HPFS_SESSION_
#define _HPFS_SESSION_

#include <optional>

namespace hpfs::session
{
    struct fs_session
    {
    };

    void start();
    void stop();
    const std::optional<fs_session> &get();

} // namespace hpfs::session

#endif