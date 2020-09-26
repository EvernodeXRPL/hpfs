#include <plog/Log.h>
#include <plog/Appenders/ColorConsoleAppender.h>
#include "hpfs.hpp"
#include "util.hpp"

namespace tracelog
{
    constexpr size_t MAX_TRACE_FILESIZE = 10 * 1024 * 1024; // 10MB
    constexpr size_t MAX_TRACE_FILECOUNT = 10;

    // Trace log category indicators for different hpfs run modes.
    constexpr const char *TRACE_RW = "][fsw] ";
    constexpr const char *TRACE_RO = "][fsr] ";
    constexpr const char *TRACE_MERGE = "][fsm] ";
    constexpr const char *TRACE_RDLOG = "][fsl] ";
    const char *CURRENT_TRACE_CATEGORY;

    class hpfs_plog_formatter;
    static plog::ConsoleAppender<hpfs_plog_formatter> consoleAppender;

    // Custom formatter adopted from:
    // https://github.com/SergiusTheBest/plog/blob/master/include/plog/Formatters/TxtFormatter.h
    class hpfs_plog_formatter
    {
    public:
        static plog::util::nstring header()
        {
            return plog::util::nstring();
        }

        static inline const char *severity_to_string(plog::Severity severity)
        {
            switch (severity)
            {
            case plog::Severity::fatal:
                return "fat";
            case plog::Severity::error:
                return "err";
            case plog::Severity::warning:
                return "wrn";
            case plog::Severity::info:
                return "inf";
            case plog::Severity::debug:
                return "dbg";
            case plog::Severity::verbose:
                return "ver";
            default:
                return "def";
            }
        }

        static plog::util::nstring format(const plog::Record &record)
        {
            tm t;
            plog::util::localtime_s(&t, &record.getTime().time); // local time

            plog::util::nostringstream ss;
            ss << t.tm_year + 1900 << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_mon + 1 << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_mday << PLOG_NSTR(" ");
            ss << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_hour << PLOG_NSTR(":") << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_min << PLOG_NSTR(":") << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_sec << PLOG_NSTR(" ");
            ss << PLOG_NSTR("[") << severity_to_string(record.getSeverity()) << PLOG_NSTR(CURRENT_TRACE_CATEGORY);
            ss << record.getMessage() << PLOG_NSTR("\n");

            return ss.str();
        }
    };

    int init()
    {
        if (hpfs::ctx.trace_level == hpfs::TRACE_LEVEL::NONE)
            return 0;

        if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW)
            CURRENT_TRACE_CATEGORY = TRACE_RW;
        else if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO)
            CURRENT_TRACE_CATEGORY = TRACE_RO;
        else if (hpfs::ctx.run_mode == hpfs::RUN_MODE::MERGE)
            CURRENT_TRACE_CATEGORY = TRACE_MERGE;
        else
            CURRENT_TRACE_CATEGORY = TRACE_RDLOG;

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
        trace_file
            .append(hpfs::ctx.trace_dir)
            .append("/")
            .append(std::to_string(hpfs::ctx.run_mode))
            .append("_")
            .append(pid_str)
            .append(".log");

        plog::init<hpfs_plog_formatter>(level, trace_file.c_str(), MAX_TRACE_FILESIZE, MAX_TRACE_FILECOUNT)
            .addAppender(&consoleAppender);

        return 0;
    }

} // namespace tracelog