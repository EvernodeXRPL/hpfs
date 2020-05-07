#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <vector>
#include <bitset>
#include <iostream>
#include <vector>
#include "hpfs.hpp"
#include "logger.hpp"
#include "util.hpp"

namespace logger
{

constexpr const char *LOG_FILE_NAME = "log.hpfs";
constexpr int FILE_PERMS = 0644;
constexpr uint16_t HPFS_VERSION = 1;

int fd = 0;
off_t eof = 0; // End of file (End offset of log file).
log_header header;
flock ro_checkpoint_lock; // Checkpoint lock placed by ReadOnly session.

int init()
{
    const std::string log_file_path = std::string(hpfs::ctx.fs_dir).append("/").append(LOG_FILE_NAME);
    flock header_lock;

    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW) // ReadWrite session.
    {
        // Open or create the log file.
        const int res = open(log_file_path.c_str(), O_CREAT | O_RDWR, FILE_PERMS);
        if (res == -1)
            return -1;
        fd = res;

        // Acquire header lock.
        if (set_lock(header_lock, true, 0, sizeof(header)) == -1)
            return -1;

        struct stat st;
        if (fstat(fd, &st) == -1)
            return -1;

        if (st.st_size == 0) // If file is empty, write the initial header.
        {
            memset(&header, 0, sizeof(header));
            header.version = HPFS_VERSION;
            if (commit_header(header) == -1)
                return -1;

            eof = sizeof(header);
        }
        else
        {
            if (read_header(header) == -1)
                return -1;

            eof = st.st_size;
        }

        // Release header lock.
        if (release_lock(header_lock) == -1)
            return -1;
    }
    else // Non-RW sessions are only interested in reading the log file header if exists.
    {
        // Open the file if exists.
        const int flags = hpfs::ctx.run_mode == hpfs::RUN_MODE::RO ? O_RDONLY : O_RDWR;
        const int res = open(log_file_path.c_str(), flags);
        if (res == -1 && errno != ENOENT)
            return -1;

        if (res > 0)
        {
            fd = res;

            // Read the header with read-lock.
            if (set_lock(header_lock, false, 0, sizeof(header)) == -1 || read_header(header) == -1)
                return -1;

            // Get total file size and release the read lock.
            struct stat st;
            if (fstat(fd, &st) == -1 || release_lock(header_lock) == -1)
                return -1;
            eof = st.st_size;
        }
    }

    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO && header.last_checkpoint > header.first_record)
    {
        // In read only session, place a 1-byte read lock on the record ending with last checkpoint.
        // This would prevent any merge process from purging beyond that record.
        if (set_lock(ro_checkpoint_lock, false, header.last_checkpoint - 1, 1) == -1)
            return -1;
    }

    return 0;
}

void deinit()
{
    if (fd > 0)
    {
        // In ReadWrite session, mark the eof offset as last checkpoint.
        if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW && eof > header.last_checkpoint)
        {
            header.last_checkpoint = eof;
            commit_header(header);
        }
        else if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO)
        {
            release_lock(ro_checkpoint_lock);
        }

        close(fd);
    }
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
            break;

        total_records++;
        std::cout << "ts:" << std::to_string(record.timestamp) << ", "
                  << "op:" << std::to_string(record.operation) << ", "
                  << record.vpath << ", "
                  << "payload_len: " << std::to_string(record.payload_len) << "\n";

    } while (log_offset > 0);

    std::cout << "Total records: " << std::to_string(total_records) << "\n";
}

off_t get_eof()
{
    return eof;
}

int set_lock(struct flock &lock, bool is_rwlock, const off_t start, const off_t len)
{
    if (fd == 0)
        return 0;
    return util::set_lock(fd, lock, is_rwlock, start, len);
}

int release_lock(struct flock &lock)
{
    if (fd == 0)
        return 0;
    return util::release_lock(fd, lock);
}

int read_header(log_header &lh)
{
    if (fd == 0)
    {
        lh = header;
        return 0;
    }

    if (pread(fd, &lh, sizeof(lh), 0) < sizeof(lh))
        return -1;
    return 0;
}

int commit_header(log_header &lh)
{
    if (pwrite(fd, &lh, sizeof(lh), 0) < sizeof(lh))
        return -1;
    return 0;
}

int append_log(std::string_view vpath, const FS_OPERATION operation, const std::vector<iovec> &payload_bufs)
{
    // Calculate total payload length.
    uint64_t payload_len = 0;
    for (const iovec &buf : payload_bufs)
        payload_len += buf.iov_len;

    log_record_header rh;
    rh.timestamp = util::epoch();
    rh.operation = operation;
    rh.vpath_len = vpath.length();
    rh.payload_len = payload_len;

    // Log record buffer collection that will be written to the file.
    std::vector<iovec> record_bufs;
    record_bufs.reserve(2 + payload_bufs.size());
    record_bufs.push_back({&rh, sizeof(rh)});
    record_bufs.push_back({(void *)vpath.data(), vpath.length()});
    for (const iovec &buf : payload_bufs) // Append payload buffers.
        record_bufs.push_back(buf);

    const size_t record_len = sizeof(rh) + rh.vpath_len + rh.payload_len;

    // Acquire locks.
    flock header_lock, record_lock;
    if (set_lock(header_lock, true, 0, sizeof(header)) == -1 ||
        set_lock(record_lock, true, eof, sizeof(record_len) == -1))
        return -1;

    // Append log record at current end of file.
    if (pwritev(fd, record_bufs.data(), record_bufs.size(), eof) < record_len)
        return -1;

    // Update log file header.
    if (header.first_record == 0)
        header.first_record = eof;
    header.last_record = eof;
    commit_header(header);

    // Release locks.
    if (release_lock(header_lock) == -1 || release_lock(record_lock) == -1)
        return -1;

    // Calculate new end of file.
    eof += record_len;

    return 0;
}

int append_log(std::string_view vpath, const FS_OPERATION operation, const iovec &payload_buf)
{
    std::vector<iovec> bufs;
    bufs.push_back(payload_buf);
    return append_log(vpath, operation, bufs);
}

int append_log(std::string_view vpath, const FS_OPERATION operation)
{
    std::vector<iovec> bufs;
    return append_log(vpath, operation, bufs);
}

int read_log_at(const off_t offset, off_t &next_offset, log_record &record)
{
    if (header.first_record == 0 || offset > header.last_record)
        return -1;

    const off_t read_offset = offset == 0 ? header.first_record : offset;

    log_record_header rh;
    lseek(fd, read_offset, SEEK_SET);
    if (read(fd, &rh, sizeof(rh)) < sizeof(rh))
        return -1;

    record.offset = read_offset;
    record.size = sizeof(rh) + rh.vpath_len + rh.payload_len;
    record.timestamp = rh.timestamp;
    record.operation = rh.operation;
    record.payload_len = rh.payload_len;
    record.payload_offset = sizeof(rh) + rh.vpath_len;

    std::string vpath;
    vpath.resize(rh.vpath_len);
    if (read(fd, vpath.data(), rh.vpath_len) < rh.vpath_len)
        return -1;
    record.vpath = std::move(vpath);

    if (record.offset + record.size == eof)
        next_offset = 0;
    else
        next_offset = record.offset + record.size;

    return 0;
}

int read_payload(std::vector<uint8_t> &payload, const log_record &record)
{
    payload.resize(record.payload_len);
    if (read(fd, payload.data(), record.payload_len) < record.payload_len)
        return -1;

    return 0;
}

} // namespace logger