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
#include "audit.hpp"
#include "tracelog.hpp"

namespace hpfs::merger
{
    constexpr useconds_t CHECK_INTERVAL = 100000; // 100ms.
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
        uint16_t counter = 0;

        while (!should_stop)
        {
            usleep(CHECK_INTERVAL);
            counter++;
            if (counter == 10)
            {
                int result = 0;
                do
                {
                    // Keep processing the oldest record of the log as long as it succeeds.

                    // Result  0 = There was no log record to process.
                    // Result  1 = There was a log record and it was succesfully merged.
                    // Result -1 = There was an error when processing log front.
                    result = merge_log_front();

                } while (!should_stop && result == 1);

                if (result == -1)
                {
                    LOG_ERROR << "Error when merging log front.";
                    break;
                }

                counter = 0;
            }
        }

        audit_logger.reset();
        LOG_INFO << "Log merge stopped.";
    }

    /**
     * Merges the oldest log record to the seed.
     * @return 0 when no log records found. 1 on successful merge. -1 on failure.
     */
    int merge_log_front()
    {
        hpfs::audit::audit_logger &logger = audit_logger.value();

        flock header_lock;
        off_t offset = 0;
        hpfs::audit::log_record record;

        // In the following code, we lock the header and release it only after a record is merged.
        // If an error is encountered mid way, then also we release the header lock.

        // Acquire header lock.
        if (logger.set_lock(header_lock, hpfs::audit::LOCK_TYPE::MERGE_LOCK) == -1)
            return -1;

        // Read the header and first log record (oldest).
        if (logger.read_header() == -1 ||
            logger.read_log_at(offset, offset, record) == -1)
        {
            // Release the lock on error.
            logger.release_lock(header_lock);
            return -1;
        }

        if (offset == -1) // Offset would be set to -1 if no records were read.
        {
            logger.release_lock(header_lock);
            return 0;
        }

        // Reaching here means a log record has been read.

        std::vector<uint8_t> payload;
        if (logger.read_payload(payload, record) == -1 || // Read any associated payload.
            merge_log_record(record, payload) == -1 ||    // Merge the record with the seed.
            logger.purge_log(record) == -1)               // Purge the log record and update the header.
        {
            LOG_ERROR << errno << ": Error merging log record.";

            // Release the lock on error.
            logger.release_lock(header_lock);
            return -1;
        }

        logger.release_lock(header_lock);
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
        }

        LOG_DEBUG << "Merge record complete.";
        return 0;
    }

} // namespace hpfs::merger