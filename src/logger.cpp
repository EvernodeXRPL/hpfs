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
off_t first_offset = 0, last_offset = 0; // First and last record offsets.
off_t last_checkpoint_offset = 0;
off_t eof_offset = 0; // Total length of file.

int init()
{
    const std::string log_file_path = std::string(hpfs::ctx.fs_dir).append("/").append(LOG_FILE_NAME);

    log_header header;
    memset(&header, 0, sizeof(header));

    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW) // ReadWrite session.
    {
        // Open or create the log file.
        const int res = open(log_file_path.c_str(), O_CREAT | O_RDWR, FILE_PERMS);
        if (res == -1)
            return -1;
        fd = res;

        struct stat st;
        if (fstat(fd, &st) == -1)
            return -1;

        if (st.st_size == 0) // If file is empty, write the header.
        {
            header.version = 1;
            if (write(fd, &header, sizeof(header)) == -1)
                return -1;

            eof_offset = sizeof(header);
        }
    }
    else // ReadOnly session or Merge session
    {
        // Open the file if exists.
        const int flags = hpfs::ctx.run_mode == hpfs::RUN_MODE::RO ? O_RDONLY : O_RDWR;
        const int res = open(log_file_path.c_str(), flags);
        if (res == -1 && errno != ENOENT)
            return -1;

        if (res > 0)
            fd = res;
    }

    // Read-lock the header.
    flock lock{F_RDLCK, SEEK_SET, 0, sizeof(header)};
    if (fd > 0 && fcntl(fd, F_SETLKW, &lock) == -1)
        return -1;

    if (fd > 0 && eof_offset == 0)
    {
        // Read the header.
        if (pread(fd, &header, sizeof(header), 0) == -1)
            return -1;

        first_offset = header.first_offset;
        last_offset = header.last_offset;
        last_checkpoint_offset = header.last_checkpoint_offset;

        struct stat st;
        if (fstat(fd, &st) == -1)
            return -1;
        eof_offset = st.st_size;
    }

    if (last_checkpoint_offset > 0 && hpfs::ctx.run_mode == hpfs::RUN_MODE::RO)
    {
        // In read only session, place a 1-byte read lock on the last checkpoint record.
        // This would prevent any merge process from purging beyond the checkpoint record.
        flock lock{F_RDLCK, SEEK_SET, last_checkpoint_offset, 1};
        if (fcntl(fd, F_SETLKW, &lock) == -1)
            return -1;
    }

    // Release the header read lock.
    lock.l_type = F_UNLCK;
    if (fd > 0 && fcntl(fd, F_SETLKW, &lock) == -1)
        return -1;

    return 0;
}

void deinit()
{
    if (fd > 0)
    {
        // In ReadWrite session, mark the last record as last checkpoint.
        if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW && last_offset > last_checkpoint_offset)
            update_header(first_offset, last_offset, last_offset);

        close(fd);
    }
}

void print_log()
{
    std::cout << "first_offset:" << std::to_string(first_offset)
              << " last_offset:" << std::to_string(last_offset)
              << " last_chk_offset:" << std::to_string(last_checkpoint_offset)
              << " eof:" << std::to_string(eof_offset) << "\n";

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

off_t get_first_offset()
{
    return first_offset;
}

off_t get_last_offset()
{
    return last_offset;
}

off_t get_last_checkpoint_offset()
{
    return last_checkpoint_offset;
}

int update_header(const off_t first_offset, const off_t last_offset, const off_t last_checkpoint_offset)
{
    // Set lock
    flock lock{F_WRLCK, SEEK_SET, 0, sizeof(log_header)};
    if (fcntl(fd, F_SETLKW, &lock) == -1)
        return -1;

    log_header header{HPFS_VERSION, first_offset, last_offset, last_checkpoint_offset};
    if (pwrite(fd, &header, sizeof(header), 0) == -1)
        return -1;

    // Release lock
    lock.l_type = F_UNLCK;
    if (fcntl(fd, F_SETLKW, &lock) == -1)
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

    log_record_header header;
    memset(&header, 0, sizeof(header));

    header.timestamp = util::epoch();
    header.operation = operation;
    header.path_len = path.length();
    header.payload_len = payload_len;

    bufs[0].iov_base = &header;
    bufs[0].iov_len = sizeof(header);
    bufs[1].iov_base = (void *)path.data();
    bufs[1].iov_len = path.length();

    // Append log record at current end of file.
    if (pwritev(fd, bufs, buf_count, eof_offset) == -1)
        return -1;

    if (first_offset == 0)
        first_offset = eof_offset;
    last_offset = eof_offset;

    update_header(first_offset, last_offset, last_checkpoint_offset);

    // Calculate new end of file.
    eof_offset += sizeof(header) + path.length() + payload_len;

    return 0;
}

int read_log_at(const off_t offset, off_t &next_offset, log_record &record)
{
    if (first_offset == 0 || offset > last_offset)
        return -1;

    const off_t read_offset = offset == 0 ? first_offset : offset;

    log_record_header header;
    memset(&header, 0, sizeof(header));

    lseek(fd, read_offset, SEEK_SET);
    if (read(fd, &header, sizeof(header)) < sizeof(header))
        return -1;

    record.offset = read_offset;
    record.size = sizeof(header) + header.path_len + header.payload_len;
    record.timestamp = header.timestamp;
    record.operation = header.operation;
    record.payload_len = header.payload_len;
    record.payload_offset = sizeof(header) + header.path_len;

    std::string path;
    path.resize(header.path_len);
    if (read(fd, path.data(), header.path_len) < header.path_len)
        return -1;
    record.path = std::move(path);

    if (record.offset + record.size == eof_offset)
        next_offset = 0;
    else
        next_offset = record.offset + record.size;

    return 0;
}

} // namespace logger