#include <iostream>
#include <vector>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/sendfile.h>
#include "merger.hpp"
#include "hpfs.hpp"
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

                    flock header_lock;

                    std::vector<uint8_t> payload;
                    if (util::set_lock(logger::fd, header_lock, true, 0, sizeof(logger::log_header)) == -1 ||
                        logger::read_payload(payload, record) == -1 ||
                        merge_log_record(record, payload) == -1 ||
                        logger::purge_log(record) == -1 ||
                        util::release_lock(logger::fd, header_lock) == -1)
                    {
                        std::cerr << errno << "\n";
                        return -1;
                    }

                } while (log_offset > 0);
            }
            else
            {
                std::cout << "no records...\n";
            }
        }

        return 0;
    }

    bool log_has_records()
    {
        flock header_lock;

        if (util::set_lock(logger::fd, header_lock, false, 0, sizeof(logger::log_header)) == -1 ||
            logger::read_header() == -1 ||
            util::release_lock(logger::fd, header_lock) == -1)
            return false;

        return logger::header.first_record > 0;
    }

    int merge_log_record(const logger::log_record &record, const std::vector<uint8_t> payload)
    {
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
            std::cout << seed_path << " to " << to_seed_path << " rename\n";

            return rename(seed_path, to_seed_path.c_str());
            break;
        }

        case logger::FS_OPERATION::UNLINK:
            std::cout << seed_path << " unlink\n";
            return unlink(seed_path);
            break;

        case logger::FS_OPERATION::CREATE:
        {
            std::cout << seed_path << " create\n";
            const mode_t mode = S_IFREG | *(mode_t *)payload.data();
            if (creat(seed_path, mode) == -1)
                return -1;
            break;
        }

        case logger::FS_OPERATION::WRITE:
        {
            std::cout << seed_path << " write\n";
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
            std::cout << seed_path << " truncate\n";
            const logger::op_truncate_payload_header th = *(logger::op_truncate_payload_header *)payload.data();
            return truncate(seed_path, th.size);
            break;
        }
        }

        return 0;
    }

} // namespace merger