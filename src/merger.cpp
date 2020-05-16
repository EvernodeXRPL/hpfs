#include <unistd.h>
#include <iostream>
#include <vector>
#include "merger.hpp"
#include "logger.hpp"
#include "util.hpp"

namespace merger
{
    constexpr useconds_t CHECK_INTERVAL = 1000000; // 1 second.

    int init()
    {
        while (true)
        {
            if (usleep(CHECK_INTERVAL) == -1)
                return -1;

            if (log_has_records())
            {
                std::cout << "Has records...\n";

                off_t log_offset = 0;

                do
                {
                    logger::log_record record;
                    if (logger::read_log_at(log_offset, log_offset, record) == -1)
                        return -1;

                    if (log_offset == -1) // No log record was read. We are at end of log.
                        break;

                    std::vector<uint8_t> payload;
                    if (logger::read_payload(payload, record) == -1 ||
                        merge_log_record(record, payload) == -1 ||
                        logger::purge_log(record) == -1)
                        return -1;

                } while (log_offset > 0);
            }
            else
            {
                std::cout << "no records...\n";
            }
        }
    }

    bool log_has_records()
    {
        logger::log_header lh;
        flock header_lock;

        if (util::set_lock(logger::fd, header_lock, false, 0, sizeof(lh)) == -1 ||
            logger::read_header(lh) == -1 ||
            util::release_lock(logger::fd, header_lock) == -1)
            return false;

        return lh.first_record > 0;
    }

    int merge_log_record(const logger::log_record &record, const std::vector<uint8_t> payload)
    {
        switch (record.operation)
        {
        case logger::FS_OPERATION::MKDIR:
        {
            const mode_t mode = S_IFDIR | *(mode_t *)payload.data();
            break;
        }

        case logger::FS_OPERATION::RMDIR:
            break;

        case logger::FS_OPERATION::RENAME:
        {
            const char *to_vpath = (char *)payload.data();
            break;
        }

        case logger::FS_OPERATION::UNLINK:
            break;

        case logger::FS_OPERATION::CREATE:
        {
            const mode_t mode = S_IFREG | *(mode_t *)payload.data();
            break;
        }

        case logger::FS_OPERATION::WRITE:
        {
            const logger::op_write_payload_header wh = *(logger::op_write_payload_header *)payload.data();

            break;
        }

        case logger::FS_OPERATION::TRUNCATE:
        {
            const logger::op_truncate_payload_header th = *(logger::op_truncate_payload_header *)payload.data();

            break;
        }
        }

        return 0;
    }

} // namespace merger