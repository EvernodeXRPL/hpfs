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
#include "logger.hpp"
#include "tracelog.hpp"

namespace merger
{
    constexpr useconds_t CHECK_INTERVAL = 1000000; // 1 second.
    bool should_stop = false;
    std::thread merger_thread;

    int init()
    {
        signal(SIGINT, &signal_handler);

        merger_thread = std::thread(merger_loop);
        merger_thread.join();
        return 0;
    }

    void signal_handler(int signum)
    {
        LOG_WARNING << "Interrupt signal (" << signum << ") received.\n";

        should_stop = true;
        if (merger_thread.joinable())
            merger_thread.join();

        LOG_WARNING << "hpfs exiting due to interrupt.";
        exit(signum);
    }

    void merger_loop()
    {
        util::mask_signal();
        LOG_INFO << "hpfs merge process started.";

        while (!should_stop)
        {
            // Keep processing the oldest record of the log as long as it succeeds.

            // Result  0 = There was no log record to process.
            // Result  1 = There was a log record and it was succesfully merged.
            // Result -1 = There was an error when processing log front.
            const int result = merge_log_front();

            if (result == 0) // If no records, wait for some time until log file has records.
            {
                usleep(CHECK_INTERVAL);
            }
            else if (result == -1)
            {
                LOG_ERROR << "Error when merging log front.";
                break;
            }
        }

        LOG_INFO << "hpfs merge process stopped.";
    }

    /**
     * Merges the oldest log record to the seed.
     * @return 0 when no log records found. 1 on successful merge. -1 on failure.
     */
    int merge_log_front()
    {
        flock header_lock;
        off_t offset = 0;
        logger::log_record record;

        // In the following code, we lock the header and release it only after a record is merged.
        // If an error is encountered mid way, then also we release the header lock.

        // Acquire header lock.
        if (logger::set_lock(header_lock, logger::LOCK_TYPE::MERGE_LOCK) == -1)
            return -1;

        // Read the header and first log record (oldest).
        if (logger::read_header() == -1 ||
            logger::read_log_at(offset, offset, record) == -1)
        {
            // Release the lock on error.
            logger::release_lock(header_lock);
            return -1;
        }

        if (offset == -1) // Offset would be set to -1 if no records were read.
        {
            logger::release_lock(header_lock);
            return 0;
        }

        // Reaching here means a log record has been read.

        std::vector<uint8_t> payload;
        if (logger::read_payload(payload, record) == -1 || // Read any associated payload.
            merge_log_record(record, payload) == -1 ||     // Merge the record with the seed.
            logger::purge_log(record) == -1)               // Purge the log record and update the header.
        {
            LOG_ERROR << errno << ": Error merging log record.";

            // Release the lock on error.
            logger::release_lock(header_lock);
            return -1;
        }

        logger::release_lock(header_lock);
        return 1;
    }

    /**
     * Physically merges the specified log record with the seed.
     */
    int merge_log_record(const logger::log_record &record, const std::vector<uint8_t> payload)
    {
        LOG_DEBUG << "Merging log record... [" << record.vpath << " op:" << record.operation << "]";

        const std::string seed_path_str = std::string(hpfs::ctx.seed_dir).append(record.vpath);
        const char *seed_path = seed_path_str.c_str();

        switch (record.operation)
        {
        case logger::FS_OPERATION::MKDIR:
        {
            const mode_t mode = *(mode_t *)payload.data();
            if (mkdir(seed_path, mode) == -1)
                return -1;
            break;
        }

        case logger::FS_OPERATION::RMDIR:
            if (rmdir(seed_path) == -1)
                return -1;
            break;

        case logger::FS_OPERATION::RENAME:
        {
            const char *to_vpath = (char *)payload.data();
            const std::string to_seed_path = std::string(hpfs::ctx.seed_dir).append(to_vpath);
            if (rename(seed_path, to_seed_path.c_str()) == -1)
                return -1;
            break;
        }

        case logger::FS_OPERATION::UNLINK:
            if (unlink(seed_path) == -1)
                return -1;
            break;

        case logger::FS_OPERATION::CREATE:
        {
            const mode_t mode = S_IFREG | *(mode_t *)payload.data();
            if (creat(seed_path, mode) == -1)
                return -1;
            break;
        }

        case logger::FS_OPERATION::WRITE:
        {
            const logger::op_write_payload_header wh = *(logger::op_write_payload_header *)payload.data();

            int seed_fd = open(seed_path, O_RDWR);
            if (seed_fd <= 0)
                return -1;

            // Copy data from directly from log file to seed file.
            lseek(seed_fd, wh.offset, SEEK_SET);
            off_t read_offset = record.block_data_offset + wh.data_offset_in_block;
            if (sendfile(seed_fd, logger::fd, &read_offset, wh.size) != wh.size)
            {
                close(seed_fd);
                return -1;
            }

            close(seed_fd);

            break;
        }

        case logger::FS_OPERATION::TRUNCATE:
        {
            const logger::op_truncate_payload_header th = *(logger::op_truncate_payload_header *)payload.data();
            if (truncate(seed_path, th.size) == -1)
                return -1;
            break;
        }
        }

        LOG_DEBUG << "Merge record complete.";

        return 0;
    }

} // namespace merger