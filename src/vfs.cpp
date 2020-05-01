#include <fcntl.h>
#include "hpfs.hpp"
#include "logger.hpp"

namespace vfs
{

int playback_file(std::string_view path)
{
    off_t vfile_log_offset = 0; // TODO: Track virtual file status.
    const off_t scan_upto = hpfs::ctx.run_mode == hpfs::RUN_MODE::RO ? logger::get_last_checkpoint_offset() : logger::get_last_offset();
    if (vfile_log_offset == scan_upto) // Return if the virtual file is already updated.
        return 0;

    // Attempt to construct virtual file from seed file (if exist).
    const std::string seed_file_path = std::string(hpfs::ctx.seed_dir).append(path);
    const int res = open(seed_file_path.c_str(), O_RDONLY);
    if (res == -1 && errno != EACCES) // We assume 'EACCES' means file does not exist.
        return -1;

    if (res > 0)
    {
        const int seed_fd = res;
        // TODO: Map the seed file to memory (mmap)
    }

    // Loop through any unapplied log records and apply any changes to the virtual file.
    off_t log_offset = vfile_log_offset;
    if (scan_upto == 0)
        return 0;

    do
    {
        logger::log_record record;
        if (logger::read_log_at(log_offset, log_offset, record) == -1)
            break;

        if (record.path == path)
        {
            // TODO: Apply log record change to virtual file.
        }

        vfile_log_offset = record.offset;

    } while (log_offset > 0 && log_offset <= scan_upto);
}

int create(const char *path, mode_t mode)
{
    return 0;
}

int read(const char *path, char *buf, size_t size, off_t offset)
{
}

int write(const char *path, const char *buf, size_t size, off_t offset)
{
    if (hpfs::ctx.run_mode == hpfs::RUN_MODE::RW)
    {
        // Payload: [size][offset][buf]
        uint8_t header_buf[sizeof(size_t) + sizeof(off_t)];
        *header_buf = size;
        *(header_buf + sizeof(size_t)) = offset;

        iovec bufs[2];
        bufs[0].iov_base = header_buf;
        bufs[0].iov_len = sizeof(header_buf);
        bufs[1].iov_base = (void *)buf;
        bufs[1].iov_len = size;

        return logger::append_log(path, logger::FS_OPERATION::WRITE, {bufs, 2});
    }
    return -1;
}

} // namespace vfs