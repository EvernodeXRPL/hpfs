#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <string>
#include <vector>
#include <bitset>
#include <iostream>
#include <vector>
#include "hpfs.hpp"
#include "logger.hpp"
#include "util.hpp"
#include "tracelog.hpp"

namespace logger
{
    constexpr const char *LOG_FILE_NAME = "log.hpfs";
    constexpr int FILE_PERMS = 0644;
    constexpr uint16_t HPFS_VERSION = 1;

    int fd = 0;         // The loag file fd uses throughout the session.
    off_t eof = 0;      // End of file (End offset of log file).
    log_header header;  // The log file header loaded into memory.
    flock session_lock; // Session lock placed on the log file.

    int init()
    {
        if (load_log_file() == -1)
            return -1;

        // RW sessions acquire a read lock on first byte of the log file.
        // This is to prevent merge operation from running when any RO/RW sessions are live.
        if ((hpfs::ctx.run_mode == hpfs::RUN_MODE::RW ||
             hpfs::ctx.run_mode == hpfs::RUN_MODE::RO) &&
            set_lock(session_lock, LOCK_TYPE::SESSION_LOCK) == -1)
        {
            LOG_ERROR << errno << ": eror acquiring RO/RW session lock.";
            return -1;
        }

        LOG_INFO << "Initialized log file.";
        return 0;
    }

    void deinit()
    {
        // In ReadWrite session, mark the eof offset as last checkpoint.
        if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW && eof > header.last_checkpoint)
        {
            header.last_checkpoint = eof;

            flock header_lock;
            if (set_lock(header_lock, LOCK_TYPE::UPDATE_LOCK) != -1)
            {
                commit_header();
                release_lock(header_lock);
            }
        }

        if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW ||
            hpfs::ctx.run_mode == hpfs::RUN_MODE::RO)
            release_lock(session_lock);

        close(fd);

        LOG_INFO << "Log file deinit complete.";
    }

    int load_log_file()
    {
        const std::string log_file_path = std::string(hpfs::ctx.fs_dir).append("/").append(LOG_FILE_NAME);

        // Open or create the log file.
        const int res = open(log_file_path.c_str(), O_CREAT | O_RDWR, FILE_PERMS);
        if (res == -1)
        {
            LOG_ERROR << errno << ": log file open error.";
            return -1;
        }
        fd = res;

        flock header_lock;

        // Acquire header rw lock.
        if (set_lock(header_lock, LOCK_TYPE::UPDATE_LOCK) == -1)
        {
            LOG_ERROR << errno << ": Error acquiring header write lock.";
            return -1;
        }

        struct stat st;
        if (fstat(fd, &st) == -1)
        {
            LOG_ERROR << errno << ": Error in stat of log file.";
            return -1;
        }

        if (st.st_size == 0) // If file is empty, write the initial header.
        {
            memset(&header, 0, sizeof(header));
            header.version = HPFS_VERSION;
            if (commit_header() == -1)
            {
                LOG_ERROR << errno << ": Error when writing header.";
                return -1;
            }

            eof = sizeof(header);
        }
        else
        {
            if (read_header() == -1)
            {
                LOG_ERROR << errno << ": Error when reading header.";
                return -1;
            }

            eof = st.st_size;
        }

        // Release header rw lock.
        if (release_lock(header_lock) == -1)
        {
            LOG_ERROR << errno << ": Error when releasing header write lock.";
            return -1;
        }

        return 0;
    }

    void print_log()
    {
        std::cout << "first:" << std::to_string(header.first_record)
                  << " last:" << std::to_string(header.last_record)
                  << " last_chk:" << std::to_string(header.last_checkpoint)
                  << " eof:" << std::to_string(eof) << "\n";

        off_t log_offset = 0;
        uint64_t total_records = 0;

        do
        {
            logger::log_record record;
            if (logger::read_log_at(log_offset, log_offset, record) == -1)
            {
                std::cerr << errno << ": Error occured when reading log.\n";
                return;
            }

            if (log_offset == -1) // No log record was read. We are at end of log.
                break;

            total_records++;
            std::cout << "ts:" << std::to_string(record.timestamp)
                      << ", op:" << std::to_string(record.operation)
                      << ", " << record.vpath
                      << ", payload_len: " << std::to_string(record.payload_len)
                      << ", blkdata_off: " << std::to_string(record.block_data_offset)
                      << ", blkdata_len: " << std::to_string(record.block_data_len)
                      << "\n";

        } while (log_offset > 0);

        std::cout << "Total records: " << std::to_string(total_records) << "\n";
    }

    off_t get_eof()
    {
        return eof;
    }

    int set_lock(struct flock &lock, const LOCK_TYPE type)
    {
        if (type == LOCK_TYPE::SESSION_LOCK)
            return util::set_lock(fd, lock, false, 0, 1); // Read lock first byte.
        else if (type == LOCK_TYPE::UPDATE_LOCK)
            return util::set_lock(fd, lock, true, 1, 1); // Write lock second byte.
        else if (type == LOCK_TYPE::MERGE_LOCK)
            return util::set_lock(fd, lock, true, 0, 2); // Write lock inclusive of both bytes above

        return -1;
    }

    int release_lock(struct flock &lock)
    {
        return util::release_lock(fd, lock);
    }

    int read_header()
    {
        if (pread(fd, &header, sizeof(header), 0) < sizeof(header))
        {
            LOG_ERROR << errno << ": Error when reading header.";
            return -1;
        }

        return 0;
    }

    int commit_header()
    {
        if (pwrite(fd, &header, sizeof(header), 0) < sizeof(header))
        {
            LOG_ERROR << errno << ": Error when updating header.";
            return -1;
        }

        LOG_DEBUG << "Header updated. first:" << header.first_record
                  << " last:" << header.last_checkpoint
                  << " lastchk:" << header.last_checkpoint;
        return 0;
    }

    int append_log(std::string_view vpath, const FS_OPERATION operation, const iovec *payload_buf,
                   const iovec *block_bufs, const int block_buf_count)
    {
        log_record_header rh;
        rh.timestamp = util::epoch();
        rh.operation = operation;
        rh.vpath_len = vpath.length();
        rh.payload_len = payload_buf ? payload_buf->iov_len : 0;
        rh.block_data_len = 0;
        rh.block_data_padding_len = 0;

        if (block_bufs != NULL && block_buf_count > 0)
        {
            // Block data must start at the next clean block after log header data and payload.
            const off_t record_end_offset = eof + sizeof(rh) + rh.vpath_len + rh.payload_len;
            const off_t block_data_offset = util::get_block_end(record_end_offset);
            rh.block_data_padding_len = block_data_offset - record_end_offset;

            for (int i = 0; i < block_buf_count; i++)
            {
                iovec block_buf = block_bufs[i];
                rh.block_data_len += block_buf.iov_len;
            }
        }

        // Total record length.
        const size_t record_len = sizeof(rh) + rh.vpath_len + rh.payload_len +
                                  rh.block_data_padding_len + rh.block_data_len;

        // Log record buffer collection that will be written to the file.
        std::vector<iovec> record_bufs;
        record_bufs.push_back({&rh, sizeof(rh)});                      // Header
        record_bufs.push_back({(void *)vpath.data(), vpath.length()}); // Vpath

        if (payload_buf)
            record_bufs.push_back(*payload_buf);

        // Append log record at current end of file.
        if (ftruncate(fd, eof + record_len) == -1 || // Extend the file size to fit entire record.
            pwritev(fd, record_bufs.data(), record_bufs.size(), eof) == -1)
        {
            LOG_ERROR << errno << ": Error appending log record.";
            return -1;
        }

        // Punch hole for block data padding.
        if (rh.block_data_padding_len > 0)
        {
            const off_t block_data_padding_start = eof + sizeof(rh) + rh.vpath_len + rh.payload_len;
            if (fallocate(fd,
                          FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                          block_data_padding_start,
                          rh.block_data_padding_len) == -1)
            {
                LOG_ERROR << errno << ": Error in punch hole of block data padding.";
                return -1;
            }
        }

        // Append block data bufs.
        if (block_bufs != NULL && block_buf_count > 0)
        {
            off_t write_offset = eof + record_len - rh.block_data_len;
            for (int i = 0; i < block_buf_count; i++)
            {
                iovec block_buf = block_bufs[i];
                if (block_buf.iov_base && pwrite(fd, block_buf.iov_base, block_buf.iov_len, write_offset) == -1)
                {
                    LOG_ERROR << errno << ": Error in writing block data.";
                    return -1;
                }
                else if (!block_buf.iov_base && fallocate(fd,
                                                          FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                                                          write_offset,
                                                          block_buf.iov_len) == -1)
                {
                    LOG_ERROR << errno << ": Error in punch hole of block data.";
                    return -1;
                }

                write_offset += block_buf.iov_len;
            }
        }

        // Update log file header.
        if (header.first_record == 0)
            header.first_record = eof;
        header.last_record = eof;

        flock header_lock;
        if (set_lock(header_lock, LOCK_TYPE::UPDATE_LOCK) == -1 ||
            commit_header() == -1 ||
            release_lock(header_lock) == -1)
        {
            LOG_ERROR << errno << ": Error updating header during append log.";
            return -1;
        }

        // Calculate new end of file.
        eof += record_len;

        LOG_DEBUG << "Appended log record."
                  << " ts:" << std::to_string(rh.timestamp)
                  << ", op:" << std::to_string(rh.operation)
                  << ", " << vpath
                  << ", payload_len: " << std::to_string(rh.payload_len)
                  << ", blkdata_len: " << std::to_string(rh.block_data_len);
        return 0;
    }

    int read_log_at(const off_t offset, off_t &next_offset, log_record &record)
    {
        if (header.first_record == 0 || offset > header.last_record)
        {
            next_offset = -1;
            return 0;
        }

        const off_t read_offset = offset == 0 ? header.first_record : offset;

        log_record_header rh;
        lseek(fd, read_offset, SEEK_SET);
        if (read(fd, &rh, sizeof(rh)) < sizeof(rh))
        {
            LOG_ERROR << errno << ": Error reading log file.";
            return -1;
        }

        record.offset = read_offset;
        record.size = sizeof(rh) + rh.vpath_len + rh.payload_len + rh.block_data_padding_len + rh.block_data_len;
        record.timestamp = rh.timestamp;
        record.operation = rh.operation;
        record.payload_len = rh.payload_len;
        record.payload_offset = record.offset + sizeof(rh) + rh.vpath_len;
        record.block_data_len = rh.block_data_len;
        record.block_data_offset = rh.block_data_len == 0
                                       ? 0
                                       : record.payload_offset + rh.payload_len + rh.block_data_padding_len;

        std::string vpath;
        vpath.resize(rh.vpath_len);
        if (read(fd, vpath.data(), rh.vpath_len) < rh.vpath_len)
        {
            LOG_ERROR << errno << ": Error reading log file.";
            return -1;
        }

        record.vpath = std::move(vpath);

        if (record.offset + record.size == eof)
            next_offset = 0;
        else
            next_offset = record.offset + record.size;

        return 0;
    }

    int read_payload(std::vector<uint8_t> &payload, const log_record &record)
    {
        if (record.payload_len > 0)
        {
            payload.resize(record.payload_len);
            if (pread(fd, payload.data(), record.payload_len, record.payload_offset) < record.payload_len)
            {
                LOG_ERROR << errno << ": Error reading log record payload.";
                return -1;
            }
        }

        return 0;
    }

    int purge_log(const log_record &record)
    {
        LOG_DEBUG << "Purging log record... [ts:" << record.timestamp << " path:" << record.vpath << " op:" << record.operation << "]";

        if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                      record.offset, record.size) == -1)
        {
            LOG_ERROR << errno << ": fallocate error in purging log record.";
            return -1;
        }

        if (record.offset == header.last_record) // This was the last remaining record
        {
            header.first_record = 0;
            header.last_record = 0;
            header.last_checkpoint = 0;
        }
        else
        {
            // Shift the first record offset forward.
            header.first_record = record.offset + record.size;
        }

        if (commit_header() == -1)
        {
            LOG_ERROR << errno << ": Error when updating header after purge.";
            return -1;
        }

        LOG_DEBUG << "Purge record complete.";

        return 0;
    }

} // namespace logger