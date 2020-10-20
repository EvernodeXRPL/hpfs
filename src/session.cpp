#include <optional>
#include "session.hpp"

namespace hpfs::session
{
    std::optional<fs_session> default_session;

    void start()
    {
        default_session.emplace(fs_session{});
    }

    void stop()
    {
        default_session.reset();
    }

    const std::optional<fs_session> &get()
    {
        return default_session;
    }
} // namespace hpfs::session