#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
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

    // Log file header: [version][first offset][last offset][last checkpoint offset]
    uint8_t header_buf[sizeof(uint16_t) + (sizeof(off_t) * 3)];

    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW) // ReadWrite session.
    {
        // Open or create the log file.
        const int res = open(log_file_path.c_str(), O_CREAT | O_RDWR | O_APPEND, FILE_PERMS);
        if (res == -1)
            return -1;
        fd = res;

        struct stat st;
        if (fstat(fd, &st) == -1)
            return -1;

        if (st.st_size == 0) // If file is empty, write the header.
        {
            *header_buf = HPFS_VERSION;
            *(off_t *)(header_buf + sizeof(uint16_t)) = first_offset;
            *(off_t *)(header_buf + sizeof(uint16_t) + sizeof(off_t)) = last_offset;
            *(off_t *)(header_buf + sizeof(uint16_t) + sizeof(off_t) * 2) = last_checkpoint_offset;
            if (write(fd, header_buf, sizeof(header_buf)) == -1)
                return -1;

            eof_offset = sizeof(header_buf);
        }
    }
    else // ReadOnly session or Merge session
    {
        // Open the file if exists.
        const int flags = hpfs::ctx.run_mode == hpfs::RUN_MODE::RO ? O_RDONLY : O_RDWR;
        const int res = open(log_file_path.c_str(), flags);
        if (res == -1 && errno != EACCES) // We assume 'EACCES' means file does not exist.
            return -1;

        if (res > 0)
            fd = res;
    }

    if (fd > 0 && eof_offset == 0)
    {
        // Read the header.
        if (read(fd, header_buf, sizeof(header_buf)) == -1)
            return -1;

        first_offset = *(off_t *)(header_buf + sizeof(uint16_t));
        last_offset = *(off_t *)(header_buf + sizeof(uint16_t) + sizeof(off_t));
        last_checkpoint_offset = *(off_t *)(header_buf + sizeof(uint16_t) + sizeof(off_t) * 2);

        struct stat st;
        if (fstat(fd, &st) == -1)
            return -1;
        eof_offset = st.st_size;
    }

    std::cout << "first_offset:" << std::to_string(first_offset)
              << " last_offset:" << std::to_string(last_offset)
              << " last_checkpoint_offset:" << std::to_string(last_checkpoint_offset)
              << " eof_offset:" << std::to_string(eof_offset) << "\n";

    return 0;
}

void deinit()
{
    if (fd > 0)
        close(fd);
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

int write_with_lock(const void *buf, const off_t buf_len, const off_t offset)
{
    // Set lock
    flock lock{F_WRLCK, SEEK_SET, offset, buf_len};
    if (fcntl(fd, F_SETLKW, &lock) == -1)
        return -1;

    if (pwrite(fd, &buf, buf_len, offset) == -1)
        return -1;

    // Release lock
    lock.l_type = F_UNLCK;
    if (fcntl(fd, F_SETLKW, &lock) == -1)
        return -1;

    return 0;
}

int update_first_offset(const off_t new_offset)
{
    // Write the first record offset at designated log file position.
    const off_t modification_offset = sizeof(uint16_t);
    if (write_with_lock(&new_offset, sizeof(new_offset), modification_offset) == -1)
        return -1;

    first_offset = new_offset;
    return 0;
}

int update_last_offset(const off_t new_offset)
{
    // Write the last record offset at designated log file position.
    const off_t modification_offset = sizeof(uint16_t) + sizeof(off_t);
    if (write_with_lock(&new_offset, sizeof(new_offset), modification_offset) == -1)
        return -1;

    last_offset = new_offset;
    return 0;
}

int update_last_checkpoint_offset(const off_t new_offset)
{
    // Write the last checkpoint offset at designated log file position.
    const off_t modification_offset = sizeof(uint16_t) + (sizeof(off_t) * 2);
    if (write_with_lock(&new_offset, sizeof(new_offset), modification_offset) == -1)
        return -1;

    last_checkpoint_offset = new_offset;
    return 0;
}

int append_log(std::string_view path, const FS_OPERATION operation, const log_record_payload payload)
{
    uint8_t header[8 + 1 + 2]; // timestamp + flags + path_len;
    *header = util::epoch();   // Timestamp
    *(header + 8) = 0;         // Flags
    *(header + 8 + 1) = (uint16_t)path.length();

    // The buffers that will be written as the loog record.
    uint8_t buf_count = 3 + payload.buf_count;
    iovec bufs[buf_count];

    // Append payload buffers into log record buffers. while calculating total payload length.
    uint64_t payload_len = 0;
    for (int i = 0; i < payload.buf_count; i++)
    {
        payload_len += payload.bufs[i].iov_len;
        bufs[3 + i] = payload.bufs[i];
    }

    bufs[0].iov_base = header;
    bufs[0].iov_len = sizeof(header);
    bufs[1].iov_base = (void *)path.data();
    bufs[1].iov_len = path.length();
    bufs[2].iov_base = &payload_len;
    bufs[2].iov_len = sizeof(payload_len);

    if (pwritev(fd, bufs, buf_count, eof_offset) == -1)
        return -1;

    update_last_offset(eof_offset);
    eof_offset += sizeof(header) + path.length() + sizeof(payload_len) + payload_len;

    return 0;
}

int read_log_at(const off_t offset, off_t &next_offset, log_record &record)
{
}

} // namespace logger