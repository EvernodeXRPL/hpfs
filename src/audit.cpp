#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <string>
#include <vector>
#include <bitset>
#include <iostream>
#include <vector>
#include <optional>
#include "util.hpp"
#include "tracelog.hpp"
#include "audit.hpp"
#include "hpfs.hpp"

namespace hpfs::audit
{
    constexpr int FILE_PERMS = 0644;
    constexpr uint16_t HPFS_VERSION = 1;

    std::optional<audit_logger> audit_logger::create(const hpfs::RUN_MODE run_mode, std::string_view log_file_path)
    {
        std::optional<audit_logger> logger = std::optional<audit_logger>(audit_logger(run_mode, log_file_path));
        if (logger->init() == -1)
            logger.reset();

        return logger;
    }

    audit_logger::audit_logger(const hpfs::RUN_MODE run_mode, std::string_view log_file_path) : run_mode(run_mode),
                                                                                                log_file_path(log_file_path)
    {
    }

    audit_logger::audit_logger(audit_logger &&old) : initialized(old.initialized),
                                                     run_mode(old.run_mode),
                                                     log_file_path(old.log_file_path),
                                                     fd(old.fd),
                                                     eof(old.eof),
                                                     header(std::move(old.header)),
                                                     session_lock(std::move(old.session_lock))
    {
        old.moved = true;
    }

    int audit_logger::init()
    {
        // Open or create the log file.
        const int res = open(hpfs::ctx.log_file_path.c_str(), O_CREAT | O_RDWR, FILE_PERMS);
        if (res == -1)
        {
            LOG_ERROR << errno << ": log file open error.";
            return -1;
        }
        fd = res;

        // RW sessions acquire a read lock on first byte of the log file.
        // This is to prevent merge operation from running when any RO/RW sessions are live.
        if ((run_mode == hpfs::RUN_MODE::RW || run_mode == hpfs::RUN_MODE::RO) &&
            set_lock(session_lock, LOCK_TYPE::SESSION_LOCK) == -1)
        {
            close(fd);
            LOG_ERROR << errno << ": error acquiring RO/RW session lock.";
            return -1;
        }

        if (init_log_header() == -1)
        {
            release_lock(session_lock);
            close(fd);
            return -1;
        }

        LOG_DEBUG << "Initialized log file. first:" << header.first_record
                  << " last:" << header.last_record
                  << " lastchk:" << header.last_checkpoint;
        initialized = true;
        return 0;
    }

    int audit_logger::get_fd()
    {
        return fd;
    }

    const log_header &audit_logger::get_header()
    {
        return header;
    }

    int audit_logger::audit_logger::init_log_header()
    {
        flock header_lock;

        // Acquire header rw lock in order to read/initialize the header.
        if (set_lock(header_lock, LOCK_TYPE::UPDATE_LOCK) == -1)
        {
            LOG_ERROR << errno << ": Error acquiring header write lock.";
            return -1;
        }

        struct stat st;
        if (fstat(fd, &st) == -1)
        {
            release_lock(header_lock);
            LOG_ERROR << errno << ": Error in stat of log file.";
            return -1;
        }

        if (st.st_size == 0) // If file is empty, write the initial header.
        {
            memset(&header, 0, sizeof(header));
            header.version = HPFS_VERSION;
            if (commit_header() == -1)
            {
                release_lock(header_lock);
                LOG_ERROR << errno << ": Error when writing header.";
                return -1;
            }

            eof = BLOCK_END(sizeof(header));
            if (ftruncate(fd, eof) == -1)
            {
                LOG_ERROR << errno << ": Error when truncating file with header.";
                return -1;
            }
        }
        else
        {
            if (read_header() == -1)
            {
                release_lock(header_lock);
                LOG_ERROR << errno << ": Error when reading header.";
                return -1;
            }

            eof = st.st_size;
        }

        // At this point we have read/initialized the log header safely. Release header rw lock.
        if (release_lock(header_lock) == -1)
        {
            LOG_ERROR << errno << ": Error when releasing header write lock.";
            return -1;
        }

        return 0;
    }

    void audit_logger::print_log()
    {
        std::cout << "first:" << std::to_string(header.first_record)
                  << " last:" << std::to_string(header.last_record)
                  << " last_chk:" << std::to_string(header.last_checkpoint)
                  << " eof:" << std::to_string(eof) << "\n";

        off_t log_offset = 0;
        uint64_t total_records = 0;

        do
        {
            log_record record;
            if (read_log_at(log_offset, log_offset, record) == -1)
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

    int audit_logger::set_lock(struct flock &lock, const LOCK_TYPE type)
    {
        if (type == LOCK_TYPE::SESSION_LOCK)
            return util::set_lock(fd, lock, false, 0, 1); // Read lock first byte.
        else if (type == LOCK_TYPE::UPDATE_LOCK)
            return util::set_lock(fd, lock, true, 1, 1); // Write lock second byte.
        else if (type == LOCK_TYPE::MERGE_LOCK)
            return util::set_lock(fd, lock, true, 0, 2); // Write lock inclusive of both bytes above

        return -1;
    }

    int audit_logger::release_lock(struct flock &lock)
    {
        return util::release_lock(fd, lock);
    }

    int audit_logger::read_header()
    {
        if (pread(fd, &header, sizeof(header), 0) < sizeof(header))
        {
            LOG_ERROR << errno << ": Error when reading header.";
            return -1;
        }

        return 0;
    }

    int audit_logger::commit_header()
    {
        if (pwrite(fd, &header, sizeof(header), 0) < sizeof(header))
        {
            LOG_ERROR << errno << ": Error when updating header.";
            return -1;
        }

        LOG_DEBUG << "Header updated. first:" << header.first_record
                  << " last:" << header.last_record
                  << " lastchk:" << header.last_checkpoint;
        return 0;
    }

    int audit_logger::append_log(std::string_view vpath, const FS_OPERATION operation, const iovec *payload_buf,
                                 const iovec *block_bufs, const int block_buf_count)
    {
        log_record_header rh;
        rh.timestamp = util::epoch();
        rh.operation = operation;
        rh.vpath_len = vpath.length();
        rh.payload_len = payload_buf ? payload_buf->iov_len : 0;
        rh.block_data_len = 0;

        // Calculate total record length including block alignment padding.
        const size_t record_len_upto_payload = sizeof(rh) + rh.vpath_len + rh.payload_len;
        const size_t record_len = BLOCK_END(record_len_upto_payload) + rh.block_data_len;

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

        // Append block data bufs.

        if (block_bufs != NULL && block_buf_count > 0)
        {
            for (int i = 0; i < block_buf_count; i++)
            {
                iovec block_buf = block_bufs[i];
                rh.block_data_len += block_buf.iov_len;
            }
        }

        // Block data must start at the next clean block after log header data and payload.
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

    /**
     * Reads the log record indicated by the offset if a record exists at that offset.
     * @param offset Log record offset to be read. If 0 current first record will be read.
     * @param next_offset Indicates the offset of next log record if read succesful or -1 if
     *                    no record available at 'offset'.
     * @param record Contains the log record if read successful.
     * @return 0 on successful read or no record available. -1 on error.
     */
    int audit_logger::read_log_at(const off_t offset, off_t &next_offset, log_record &record)
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

        // Calculate total record length including block alignment padding.
        const size_t record_len_upto_payload = sizeof(rh) + rh.vpath_len + rh.payload_len;
        record.size = BLOCK_END(record_len_upto_payload) + rh.block_data_len;

        record.timestamp = rh.timestamp;
        record.operation = rh.operation;
        record.payload_len = rh.payload_len;
        record.payload_offset = record.offset + sizeof(rh) + rh.vpath_len;
        record.block_data_len = rh.block_data_len;
        record.block_data_offset = rh.block_data_len == 0
                                       ? 0
                                       : record.offset + record.size - record.block_data_len;

        std::string vpath;
        vpath.resize(rh.vpath_len);
        if (read(fd, vpath.data(), rh.vpath_len) < rh.vpath_len)
        {
            LOG_ERROR << errno << ": Error reading log file.";
            return -1;
        }

        record.vpath.swap(vpath);

        if (record.offset + record.size == eof)
            next_offset = 0;
        else
            next_offset = record.offset + record.size;

        return 0;
    }

    int audit_logger::read_payload(std::vector<uint8_t> &payload, const log_record &record)
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

    int audit_logger::purge_log(const log_record &record)
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

    audit_logger::~audit_logger()
    {
        if (initialized && !moved)
        {
            // In ReadWrite session, mark the eof offset as last checkpoint (if there are records).
            if (run_mode == hpfs::RUN_MODE::RW && eof > header.last_checkpoint && header.last_record > 0)
            {
                header.last_checkpoint = eof;

                flock header_lock;
                if (set_lock(header_lock, LOCK_TYPE::UPDATE_LOCK) != -1)
                {
                    commit_header();
                    release_lock(header_lock);
                }
            }

            if (run_mode == hpfs::RUN_MODE::RW ||
                run_mode == hpfs::RUN_MODE::RO)
                release_lock(session_lock);

            close(fd);

            LOG_INFO << "Log file deinit complete.";
        }
    }

} // namespace hpfs::audit