#include <plog/Log.h>
#include "hpfs.hpp"

namespace tracelog
{
    constexpr size_t MAX_TRACE_FILESIZE = 10 * 1024 * 1024; // 10MB
    constexpr size_t MAX_TRACE_FILECOUNT = 10;

    void init()
    {
        plog::Severity level;
        if (hpfs::ctx.trace_level == hpfs::TRACE_LEVEL::DEBUG)
            level = plog::Severity::debug;
        else if (hpfs::ctx.trace_level == hpfs::TRACE_LEVEL::INFO)
            level = plog::Severity::info;
        else if (hpfs::ctx.trace_level == hpfs::TRACE_LEVEL::WARN)
            level = plog::Severity::warning;
        else
            level = plog::Severity::error;

        std::string pid_str = std::to_string(getpid());
        std::string trace_file;
        trace_file.append(hpfs::ctx.fs_dir).append("/trace/").append(pid_str).append(".log");

        plog::init(level, trace_file.c_str(), MAX_TRACE_FILESIZE, MAX_TRACE_FILECOUNT);
    }

} // namespace tracelog