#include <iostream>
#include <vector>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/sendfile.h>
#include "merger.hpp"
#include "hpfs.hpp"
#include "logger.hpp"
#include "tracelog.hpp"

namespace merger
{
    constexpr useconds_t CHECK_INTERVAL = 1000000; // 1 second.

    int init()
    {
        LOG_INFO << "hpfs merge process started.";

        while (true)
        {
            if (usleep(CHECK_INTERVAL) == -1)
                return -1;

            while (true)
            {
                // Keep processing the oldest record of the log until it fails.
                if (process_log_front() == -1)
                    break;
            }
        }

        return 0;
    }

    /**
     * Merges the oldest log record to the seed.
     * @return 0 on success. -1 on failure or no more log records available.
     */
    int process_log_front()
    {
        flock header_lock;
        off_t off = 0;
        logger::log_record record;
        std::vector<uint8_t> payload;

        // Process the oldest log record within a lock.

        if (logger::set_lock(header_lock, logger::LOCK_TYPE::MERGE_LOCK) == -1)
            return -1;

        if (logger::read_header() == -1 ||               // Read the latest header information.
            logger::header.first_record == 0 ||          // No records in log file.
            logger::read_log_at(off, off, record) == -1) // Read the first log record (oldest).
        {
            // There no records to read.
            
            logger::release_lock(header_lock);
            return -1;
        }

        if (logger::read_payload(payload, record) == -1 || // Read any associated payload.
            merge_log_record(record, payload) == -1 ||     // Merge the record with the seed.
            logger::purge_log(record) == -1)               // Purge the log record and update the header.
        {
            LOG_ERROR << errno << ": Error merging log record.";

            // Release the header lock even if something fails.
            logger::release_lock(header_lock);
            return -1;
        }

        return logger::release_lock(header_lock);
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
            return mkdir(seed_path, mode);
            break;
        }

        case logger::FS_OPERATION::RMDIR:
            return rmdir(seed_path);
            break;

        case logger::FS_OPERATION::RENAME:
        {
            const char *to_vpath = (char *)payload.data();
            const std::string to_seed_path = std::string(hpfs::ctx.seed_dir).append(to_vpath);
            return rename(seed_path, to_seed_path.c_str());
            break;
        }

        case logger::FS_OPERATION::UNLINK:
            return unlink(seed_path);
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
            return truncate(seed_path, th.size);
            break;
        }
        }

        LOG_DEBUG << "Merge complete.";

        return 0;
    }

} // namespace merger