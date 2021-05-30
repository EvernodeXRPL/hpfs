#include <iostream>
#include <vector>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <sys/sendfile.h>
#include <thread>
#include "merger.hpp"
#include "util.hpp"
#include "hpfs.hpp"
#include "audit/audit.hpp"
#include "tracelog.hpp"

namespace hpfs::merger
{
    constexpr useconds_t CHECK_INTERVAL = 100000; // 100ms.

    // The log file size threshold to trigger priority merge.
    constexpr size_t PRIORITY_MERGE_SIZE_THRESHOLD = 100 * 1024 * 1024; // 100MB

    bool should_stop = false;
    std::thread merger_thread;
    std::optional<hpfs::audit::audit_logger> audit_logger;

    int init()
    {
        if (!ctx.merge_enabled)
            return 0;

        if (hpfs::audit::audit_logger::create(audit_logger, audit::LOG_MODE::MERGE, ctx.log_file_path) == -1)
            return -1;

        merger_thread = std::thread(merger_loop);
        return 0;
    }

    void deinit()
    {
        if (!ctx.merge_enabled)
            return;

        should_stop = true;
        if (merger_thread.joinable())
            merger_thread.join();
    }

    void merger_loop()
    {
        util::mask_signal();
        LOG_INFO << "Log merger started.";
        uint16_t counter = 10;
        hpfs::audit::audit_logger &logger = audit_logger.value();

        while (!should_stop)
        {
            if (counter == 10)
            {
                size_t merged_count = 0; // No. of records that were merged during this cycle.

                while (!should_stop)
                {
                    // We lock the header and release it only after the merge is complete.
                    flock header_lock;
                    if (logger.set_lock(header_lock, hpfs::audit::LOCK_TYPE::MERGE_LOCK) == -1 ||
                        logger.read_header() == -1)
                        break;

                    // Normally we merge one record at a time. This allows any RW/RO sessions to jump in, essentially making
                    // the merge operation a lower priority than RW/RO sessions.
                    // However, if the log file has grown too large, we merge ALL records instead of one at a time.
                    const audit::log_header &header = logger.get_header();
                    const bool priority_merge = ((header.last_record - header.first_record) >= PRIORITY_MERGE_SIZE_THRESHOLD);

                    // Keep processing the oldest record of the log as long as it succeeds.
                    // Result  0 = There was no log record to process.
                    // Result  1 = There was a log record and it was succesfully merged.
                    // Result -1 = There was an error when processing log front.
                    int merge_result = 0;
                    while ((merge_result = merge_log_front(logger)) == 1)
                    {
                        if (merged_count == 0)
                        {
                            if (priority_merge)
                                LOG_WARNING << "Started priority merge...";
                            else
                                LOG_INFO << "Started merging records...";
                        }

                        merged_count++;
                        if (!priority_merge || should_stop) // Stop after merging one log record if we are not doing priority merge.
                            break;
                    }

                    logger.release_lock(header_lock);

                    // Go back to idle sleep if there were no records or on error.
                    if (merge_result != 1)
                    {
                        if (merged_count > 0)
                            LOG_INFO << "Switching to idle. " << merged_count << " records were merged.";
                        break;
                    }
                }

                counter = 0;
            }

            // We perform idle sleep for small time windows due to lesser delay in handling interrupts.
            usleep(CHECK_INTERVAL);
            counter++;
        }

        audit_logger.reset();
        LOG_INFO << "Log merge stopped.";
    }

    /**
     * Merges the oldest log record to the seed.
     * @return 0 when no log records found. 1 on successful merge. -1 on failure.
     */
    int merge_log_front(hpfs::audit::audit_logger &logger)
    {
        off_t offset = 0;
        hpfs::audit::log_record record;

        // Read the first log record (oldest).
        if (logger.read_log_at(offset, offset, record) == -1)
            return -1;

        if (offset == -1) // Offset would be set to -1 if no records were read.
            return 0;

        // Reaching here means a log record has been read.

        std::vector<uint8_t> payload;
        if (logger.read_payload(payload, record) == -1 || // Read any associated payload.
            merge_log_record(record, payload) == -1 ||    // Merge the record with the seed.
            logger.purge_log(record) == -1)               // Purge the log record and update the header.
        {
            LOG_ERROR << errno << ": Error merging log record.";
            return -1;
        }

        return 1;
    }

    /**
     * Physically merges the specified log record with the seed.
     */
    int merge_log_record(const hpfs::audit::log_record &record, const std::vector<uint8_t> payload)
    {
        LOG_DEBUG << "Merging log record... [" << record.vpath << " op:" << record.operation << "]";

        hpfs::audit::audit_logger &logger = audit_logger.value();
        const std::string seed_path_str = std::string(hpfs::ctx.seed_dir).append(record.vpath);
        const char *seed_path = seed_path_str.c_str();

        switch (record.operation)
        {
        case hpfs::audit::FS_OPERATION::MKDIR:
        {
            const mode_t mode = *(mode_t *)payload.data();
            if (mkdir(seed_path, mode) == -1)
            {
                LOG_ERROR << errno << ": Error in log merge mkdir. " << seed_path;
                return -1;
            }
            break;
        }

        case hpfs::audit::FS_OPERATION::RMDIR:
            if (rmdir(seed_path) == -1)
            {
                LOG_ERROR << errno << ": Error in log merge rmdir. " << seed_path;
                return -1;
            }
            break;

        case hpfs::audit::FS_OPERATION::RENAME:
        {
            const char *to_vpath = (char *)payload.data();
            const std::string to_seed_path = std::string(hpfs::ctx.seed_dir).append(to_vpath);
            if (rename(seed_path, to_seed_path.c_str()) == -1)
            {
                LOG_ERROR << errno << ": Error in log merge rename. " << seed_path;
                return -1;
            }
            break;
        }

        case hpfs::audit::FS_OPERATION::UNLINK:
            if (unlink(seed_path) == -1)
            {
                LOG_ERROR << errno << ": Error in log merge unlink. " << seed_path;
                return -1;
            }
            break;

        case hpfs::audit::FS_OPERATION::CREATE:
        {
            const mode_t mode = S_IFREG | *(mode_t *)payload.data();
            int res = creat(seed_path, mode);
            if (res == -1)
            {
                LOG_ERROR << errno << ": Error in log merge creat. " << seed_path;
                return -1;
            }
            close(res);
            break;
        }

        case hpfs::audit::FS_OPERATION::WRITE:
        {
            const hpfs::audit::op_write_payload_header wh = *(hpfs::audit::op_write_payload_header *)payload.data();

            int seed_fd = open(seed_path, O_RDWR);
            if (seed_fd <= 0)
            {
                LOG_ERROR << errno << ": Error in log merge open for write. " << seed_path;
                return -1;
            }

            // Copy data from directly from log file to seed file.
            lseek(seed_fd, wh.offset, SEEK_SET);
            off_t read_offset = record.block_data_offset + wh.data_offset_in_block;
            if (sendfile(seed_fd, logger.get_fd(), &read_offset, wh.size) != wh.size)
            {
                close(seed_fd);
                LOG_ERROR << errno << ": Error in log merge sendfile. " << seed_path;
                return -1;
            }

            close(seed_fd);

            break;
        }

        case hpfs::audit::FS_OPERATION::TRUNCATE:
        {
            const hpfs::audit::op_truncate_payload_header th = *(hpfs::audit::op_truncate_payload_header *)payload.data();
            if (truncate(seed_path, th.size) == -1)
            {
                LOG_ERROR << errno << ": Error in log merge truncate. " << seed_path;
                return -1;
            }
            break;
        }

        case hpfs::audit::FS_OPERATION::CHMOD:
        {
            const mode_t mode = *(mode_t *)payload.data();
            if (chmod(seed_path, mode) == -1)
            {
                LOG_ERROR << errno << ": Error in log merge chmod. " << seed_path;
                return -1;
            }
            break;
        }
        }

        LOG_DEBUG << "Merge record complete.";
        return 0;
    }

} // namespace hpfs::merger