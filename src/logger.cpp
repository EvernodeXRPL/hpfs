#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <vector>
#include <bitset>
#include <iostream>
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

    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RO && header.last_checkpoint > 0)
    {
        // In read only session, place a 1-byte read lock on the last checkpoint record.
        // This would prevent any merge process from purging beyond the checkpoint record.
        if (set_lock(ro_checkpoint_lock, false, header.last_checkpoint, 1) == -1)
            return -1;
    }

    return 0;
}

void deinit()
{
    if (fd > 0)
    {
        // In ReadWrite session, mark the last record as last checkpoint.
        if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW && header.last_record > header.last_checkpoint)
        {
            header.last_checkpoint = header.last_record;
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
                  << record.path << ", "
                  << "payload_len: " << std::to_string(record.payload_len) << "\n";

    } while (log_offset > 0);

    std::cout << "Total records: " << std::to_string(total_records) << "\n";
}

off_t get_eof_offset()
{
    return eof;
}

int set_lock(flock &lock, bool is_rwlock, const off_t start, const off_t len)
{
    lock.l_type = is_rwlock ? F_WRLCK : F_RDLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = start,
    lock.l_len = len;
    return fcntl(fd, F_SETLKW, &lock);
}

int release_lock(flock &lock)
{
    lock.l_type = F_UNLCK;
    return fcntl(fd, F_SETLKW, &lock);
}

int read_header(log_header &lh)
{
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

int append_log(std::string_view path, const FS_OPERATION operation, const log_record_payload payload)
{
    // The buffers that will be written as the log record.
    uint8_t buf_count = 2 + payload.buf_count;
    iovec bufs[buf_count];

    // Append payload buffers into log record buffers, while calculating total payload length.
    uint64_t payload_len = 0;
    for (int i = 0; i < payload.buf_count; i++)
    {
        payload_len += payload.bufs[i].iov_len;
        bufs[2 + i] = payload.bufs[i];
    }

    log_record_header rh;
    rh.timestamp = util::epoch();
    rh.operation = operation;
    rh.path_len = path.length();
    rh.payload_len = payload_len;

    bufs[0].iov_base = &rh;
    bufs[0].iov_len = sizeof(rh);
    bufs[1].iov_base = (void *)path.data();
    bufs[1].iov_len = path.length();

    const size_t record_len = sizeof(rh) + rh.path_len + rh.payload_len;

    // Acquire locks.
    flock header_lock, record_lock;
    if (set_lock(header_lock, true, 0, sizeof(header)) == -1 ||
        set_lock(record_lock, true, eof, sizeof(record_len) == -1))
        return -1;

    // Append log record at current end of file.
    if (pwritev(fd, bufs, buf_count, eof) < record_len)
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
    record.size = sizeof(rh) + rh.path_len + rh.payload_len;
    record.timestamp = rh.timestamp;
    record.operation = rh.operation;
    record.payload_len = rh.payload_len;
    record.payload_offset = sizeof(rh) + rh.path_len;

    std::string path;
    path.resize(rh.path_len);
    if (read(fd, path.data(), rh.path_len) < rh.path_len)
        return -1;
    record.path = std::move(path);

    if (record.offset + record.size == eof)
        next_offset = 0;
    else
        next_offset = record.offset + record.size;

    return 0;
}

int read_payload(std::string &payload, const log_record &record)
{
    payload.resize(record.payload_len);
    if (read(fd, payload.data(), record.payload_len) < record.payload_len)
        return -1;

    return 0;
}

} // namespace logger