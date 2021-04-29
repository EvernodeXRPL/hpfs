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
#include "../util.hpp"
#include "../tracelog.hpp"
#include "audit.hpp"
#include "../hpfs.hpp"
#include "../version.hpp"

namespace hpfs::audit
{
    constexpr int FILE_PERMS = 0644;

    int audit_logger::create(std::optional<audit_logger> &logger, const LOG_MODE mode, std::string_view log_file_path)
    {
        logger.emplace(mode, log_file_path);
        if (logger->init() == -1)
        {
            logger.reset();
            return -1;
        }

        return 0;
    }

    audit_logger::audit_logger(const LOG_MODE mode, std::string_view log_file_path) : mode(mode),
                                                                                      log_file_path(log_file_path)
    {
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
        // This is to prevent merge or sync operation from running when any RO/RW sessions are live.
        if ((mode == LOG_MODE::RW || mode == LOG_MODE::RO) &&
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
            // Add the version header at the start of the file.
            if (pwrite(fd, version::HP_VERSION_BYTES, version::VERSION_BYTES_LEN, 0) < version::VERSION_BYTES_LEN)
            {
                LOG_ERROR << "Error adding version header to the log file";
                release_lock(header_lock);
                return -1;
            }
            memset(&header, 0, sizeof(header));
            if (commit_header() == -1)
            {
                release_lock(header_lock);
                LOG_ERROR << errno << ": Error when writing header.";
                return -1;
            }

            eof = BLOCK_END(version::VERSION_BYTES_LEN + sizeof(header));
            if (ftruncate(fd, eof) == -1)
            {
                release_lock(header_lock);
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
                      << ", root_hash: " << record.root_hash
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
            return util::set_lock(fd, lock, true, 0, 2); // Write lock inclusive of both bytes above.
        else if (type == LOCK_TYPE::SYNC_LOCK)
            return util::set_lock(fd, lock, true, 0, 2); // Write lock inclusive of both bytes above.

        return -1;
    }

    int audit_logger::release_lock(struct flock &lock)
    {
        return util::release_lock(fd, lock);
    }

    int audit_logger::read_header()
    {
        if (pread(fd, &header, sizeof(header), version::VERSION_BYTES_LEN) < sizeof(header))
        {
            LOG_ERROR << errno << ": Error when reading header.";
            return -1;
        }

        return 0;
    }

    int audit_logger::commit_header()
    {
        // Log header is after the hpfs version header.
        if (pwrite(fd, &header, sizeof(header), version::VERSION_BYTES_LEN) < sizeof(header))
        {
            LOG_ERROR << errno << ": Error when updating header.";
            return -1;
        }

        LOG_DEBUG << "Header updated. first:" << header.first_record
                  << " last:" << header.last_record
                  << " lastchk:" << header.last_checkpoint;
        return 0;
    }

    /**
     * Append the new log record at the end of the log file
     * @return Returns the offset at the start of the new log record. 0 if error.
    */
    off_t audit_logger::append_log(log_record_header &rh, std::string_view vpath, const FS_OPERATION operation, const iovec *payload_buf,
                                   const iovec *block_bufs, const int block_buf_count)
    {
        rh = {};
        rh.timestamp = util::epoch();
        rh.operation = operation;
        rh.vpath_len = vpath.length();
        rh.payload_len = payload_buf ? payload_buf->iov_len : 0;
        rh.block_data_len = 0;

        // Calculate total block data length.
        if (block_bufs != NULL && block_buf_count > 0)
        {
            for (int i = 0; i < block_buf_count; i++)
            {
                iovec block_buf = block_bufs[i];
                rh.block_data_len += block_buf.iov_len;
            }
        }

        const log_record_metrics lm = get_metrics(rh);

        // Log record buffer collection that will be written to the file.
        std::vector<iovec> record_bufs;
        record_bufs.push_back({&rh, sizeof(rh)});                      // Header
        record_bufs.push_back({(void *)vpath.data(), vpath.length()}); // Vpath

        if (payload_buf)
            record_bufs.push_back(*payload_buf);

        // Append log record at current end of file.
        if (ftruncate(fd, eof + lm.total_size) == -1 || // Extend the file size to fit entire record.
            pwritev(fd, record_bufs.data(), record_bufs.size(), eof) == -1)
        {
            LOG_ERROR << errno << ": Error appending log record.";
            return 0;
        }

        // Append block data bufs.
        // Block data must start at the next clean block after log header data and payload.
        if (block_bufs != NULL && block_buf_count > 0)
        {
            off_t write_offset = eof + lm.block_data_offset;
            for (int i = 0; i < block_buf_count; i++)
            {
                iovec block_buf = block_bufs[i];
                if (block_buf.iov_base && pwrite(fd, block_buf.iov_base, block_buf.iov_len, write_offset) == -1)
                {
                    LOG_ERROR << errno << ": Error in writing block data.";
                    return 0;
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
            return 0;
        }

        // Saving the starting offset of the log record.
        const off_t log_rec_start_offset = eof;
        // Calculate new end of file.
        eof += lm.total_size;

        LOG_DEBUG << "Appended log record."
                  << " ts:" << std::to_string(rh.timestamp)
                  << ", op:" << std::to_string(rh.operation)
                  << ", " << vpath
                  << ", payload_len: " << std::to_string(rh.payload_len)
                  << ", blkdata_len: " << std::to_string(rh.block_data_len);

        if (!last_op)
            last_op = fs_operation_summary{};
        last_op->update(vpath, rh, payload_buf);

        return log_rec_start_offset;
    }

    /**
     * Overwrite the provided buffers for operation payload and data buffers of the last log record
     * @param payload_write_offset Offset to write payload buffer relative to log record offset.
     * @param data_write_offset Offset to write data buffers relative to log record offset.
     * @param payload_buf Operation payload buffer to be written. New payload size is assumed to be same as existing one.
     * @param data_bufs Data buffer collection to be written.
     * @param data_buf_count No. of data buffers.
     * @param new_block_data_len The new block data length to be written into log record header. Ignored if 0.
     * @param rh The log record header of the log record being modified.
     * @return 0 on success. -1 on error.
     */
    int audit_logger::overwrite_last_log_record_bytes(const off_t payload_write_offset, const off_t data_write_offset,
                                                      const iovec *payload_buf, const iovec *data_bufs, const int data_buf_count,
                                                      const size_t new_block_data_len, log_record_header &rh)
    {
        if (header.last_record == 0)
            return -1;

        // If specified, we need to overwrite the block data len stored in the log record.
        if (new_block_data_len > 0 && new_block_data_len > rh.block_data_len)
        {
            // Update the eof because the log file is going to expand (new block data is bigger than the existing).
            eof += (new_block_data_len - rh.block_data_len);

            rh.block_data_len = new_block_data_len;
            if (pwrite(fd, &rh, sizeof(rh), header.last_record) == -1)
            {
                LOG_ERROR << errno << ": Error during overwriting log record when writing header at " << header.last_record;
                return -1;
            }
        }

        if (pwrite(fd, payload_buf->iov_base, payload_buf->iov_len, (header.last_record + payload_write_offset)) == -1)
        {
            LOG_ERROR << errno << ": Error when overwriting payload buffer at " << (header.last_record + payload_write_offset);
            return -1;
        }

        if (data_bufs != NULL && data_buf_count > 0)
        {
            off_t write_offset = (header.last_record + data_write_offset);

            for (int i = 0; i < data_buf_count; i++)
            {
                iovec block_buf = data_bufs[i];
                if (block_buf.iov_base && pwrite(fd, block_buf.iov_base, block_buf.iov_len, write_offset) == -1)
                {
                    LOG_ERROR << errno << ": Error when overwriting data buffers (" << data_buf_count << ") at " << write_offset;
                    return -1;
                }

                write_offset += block_buf.iov_len;
            }
        }

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

        const log_record_metrics lm = get_metrics(rh);

        record.offset = read_offset;
        record.size = lm.total_size;
        record.timestamp = rh.timestamp;
        record.operation = rh.operation;
        record.payload_len = rh.payload_len;
        record.payload_offset = record.offset + lm.payload_offset;
        record.block_data_len = rh.block_data_len;
        record.block_data_offset = rh.block_data_len == 0 ? 0 : (record.offset + lm.block_data_offset);
        record.root_hash = rh.root_hash;

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

    /**
     * Reads the log record indicated by the offset as a buffer if a record exists at that offset.
     * @param offset Log record offset to be read. If 0 current first record will be read.
     * @param next_offset Indicates the offset of next log record if read succesful or -1 if
     *                    no record available at 'offset'. If the record is the last record then next_offset is 0.
     * @param buf Contains the log record buffer if read successful.
     * @return 0 on successful read or no record available. -1 on error.
     */
    int audit_logger::read_log_record_buf_at(const off_t offset, off_t &next_offset, std::string &buf)
    {
        if (header.first_record == 0 || offset > header.last_record)
        {
            next_offset = -1;
            return 0;
        }

        const off_t read_offset = offset == 0 ? header.first_record : offset;

        // Reading the log header.
        log_record_header rh;
        if (pread(fd, &rh, sizeof(log_record_header), read_offset) < sizeof(log_record_header))
        {
            LOG_ERROR << errno << ": Error reading log record header from log file.";
            return -1;
        }

        const log_record_metrics lm = get_metrics(rh);

        buf.resize(lm.unpadded_size);
        memcpy(buf.data(), &rh, sizeof(rh));
        size_t buf_offset = sizeof(rh);

        // Reading the vpath.
        if (pread(fd, buf.data() + buf_offset, rh.vpath_len, read_offset + lm.vpath_offset) < rh.vpath_len)
        {
            LOG_ERROR << errno << ": Error reading log record vpath from log file.";
            return -1;
        }
        buf_offset += rh.vpath_len;

        // Reading the payload.
        if (rh.payload_len > 0)
        {
            if (pread(fd, buf.data() + buf_offset, rh.payload_len, read_offset + lm.payload_offset) < rh.payload_len)
            {
                LOG_ERROR << errno << ": Error reading log record payload from log file.";
                return -1;
            }
            buf_offset += rh.payload_len;
        }

        // Reading the block data.
        if (rh.block_data_len > 0)
        {
            if (pread(fd, buf.data() + buf_offset, rh.block_data_len, read_offset + lm.block_data_offset) < rh.block_data_len)
            {
                LOG_ERROR << errno << ": Error reading log record block data from log file.";
                return -1;
            }
            buf_offset += rh.block_data_len;
        }

        next_offset = read_offset + lm.total_size;
        // If there's no more log records next offset is 0.
        if (next_offset == eof)
            next_offset = 0;

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

    int audit_logger::update_log_record_hash(const off_t log_rec_start_offset, const hmap::hasher::h32 root_hash, log_record_header &rh)
    {
        if (header.first_record == 0 || log_rec_start_offset > header.last_record)
        {
            return 0;
        }
        // Return if the given hash is an empty hash.
        if (root_hash == hmap::hasher::h32_empty)
            return -1;

        // Replace root hash with the new root hash.
        rh.root_hash = root_hash;

        if (pwrite(fd, &rh, sizeof(rh), log_rec_start_offset) == -1)
        {
            LOG_ERROR << errno << ": Error updating log record.";
            return -1;
        }

        return 0;
    }

    /**
     * Truncate log file upto the given log file offset value.
     * @param log_record_offset The offset where the log file is truncated. This log record will be kept and rest well be truncated.
     * @return Returns 0 on success and -1 on error.
    */
    int audit_logger::truncate_log_file(const off_t log_record_offset)
    {
        if (mode != LOG_MODE::LOG_SYNC)
            return -1;

        if (header.first_record == 0)
        {
            LOG_ERROR << "Invalid log record offset for truncation";
            return -1;
        }

        flock truncate_lock;
        // This is a blocking call. This will wait until running RO and RW sessions exits.
        if (set_lock(truncate_lock, LOCK_TYPE::SYNC_LOCK) == -1)
        {
            LOG_ERROR << "Error acquiring log file sync lock.";
            return -1;
        }

        off_t truncate_offset;
        // We are removing all the log records.
        if (log_record_offset == 0)
        {
            truncate_offset = header.first_record;
            header.first_record = 0;
            header.last_checkpoint = 0;
            header.last_record = 0;
        }
        else
        {
            // We keep the log record positioned in log_record_offset and truncate the rest.
            log_record log_record;
            if (read_log_at(log_record_offset, truncate_offset, log_record) == -1)
            {
                LOG_ERROR << "Error reading log record at offset: " << log_record_offset;
                release_lock(truncate_lock);
                return -1;
            }
            // Update new last record offset.
            header.last_record = log_record_offset;
            // Update last checkpoint if the check point was in a truncated offset.
            if (header.last_checkpoint > header.last_record)
                header.last_checkpoint = header.last_record;
        }

        if (truncate_offset <= 0 || truncate_offset > eof)
        {
            LOG_ERROR << "Invalid log record offset for truncation";
            release_lock(truncate_lock);
            return -1;
        }
        else if (truncate_offset == eof) // If truncate offset is eof, No need of truncation.
        {
            release_lock(truncate_lock);
            return 0;
        }

        if (ftruncate(fd, truncate_offset) == -1) // Truncate the file.
        {
            LOG_ERROR << errno << ": Error truncating log file at offset: " << truncate_offset;
            release_lock(truncate_lock);
            return -1;
        }

        eof = truncate_offset;

        if (commit_header() == -1)
        {
            LOG_ERROR << errno << ": Error updating header during truncate log.";
            release_lock(truncate_lock);
            return -1;
        }

        // Release aquired lock after truncate operation finishes.
        release_lock(truncate_lock);
        return 0;
    }

    std::optional<fs_operation_summary> &audit_logger::get_last_operation()
    {
        return last_op;
    }

    /**
     * Returns usefull offsets relative to the begning of the log record.
     */
    const log_record_metrics audit_logger::get_metrics(const log_record_header &rh)
    {
        // Log record stored structure.
        // [header][vpath][operation payload][padding bytes][block data]

        log_record_metrics lm;
        lm.vpath_offset = sizeof(rh);
        lm.payload_offset = lm.vpath_offset + rh.vpath_len;

        const off_t payload_end_offset = lm.payload_offset + rh.payload_len;
        const off_t payload_end_padded_offset = BLOCK_END(payload_end_offset);

        lm.block_data_offset = rh.block_data_len == 0 ? 0 : payload_end_padded_offset;
        lm.unpadded_size = payload_end_offset + rh.block_data_len;
        lm.total_size = payload_end_padded_offset + rh.block_data_len;
        return lm;
    }

    audit_logger::~audit_logger()
    {
        if (initialized && !moved)
        {
            // In ReadWrite session, mark the eof offset as last checkpoint (if there are records).
            if (mode == LOG_MODE::RW && eof > header.last_checkpoint && header.last_record > 0)
            {
                header.last_checkpoint = eof;

                flock header_lock;
                if (set_lock(header_lock, LOCK_TYPE::UPDATE_LOCK) != -1)
                {
                    commit_header();
                    release_lock(header_lock);
                }
            }

            if (mode == LOG_MODE::RW ||
                mode == LOG_MODE::RO)
                release_lock(session_lock);

            close(fd);

            LOG_INFO << "Log file deinit complete.";
        }
    }

} // namespace hpfs::audit