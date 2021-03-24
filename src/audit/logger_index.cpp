
#include "logger_index.hpp"
#include "audit.hpp"
#include "../tracelog.hpp"
#include "../util.hpp"

namespace hpfs::audit::logger_index
{
    constexpr int FILE_PERMS = 0644;
    constexpr uint16_t HPFS_VERSION = 1;
    constexpr const char *INDEX_UPDATE_FILE = "/::hpfs.log.update.index";

    int fd = 0;    // The index file fd used throughout the session.
    off_t eof = 0; // End of file (End offset of index file).
    std::optional<audit::audit_logger> logger;

    /**
     * Initialize the log index file.
     * @param file_path Path of the log index file.
     * @return Returns 0 on success, -1 on error.
    */
    int init(std::string_view file_path)
    {
        // First initialize the logger.
        if (audit::audit_logger::create(logger, audit::LOG_MODE::RO, ctx.log_file_path) == -1)
        {
            LOG_ERROR << errno << ": Error in opening log file.";
            logger.reset();
            return -1;
        }

        // Open or create the index file.
        const int res = open(file_path.data(), O_CREAT | O_RDWR | O_APPEND, FILE_PERMS);
        if (res == -1)
        {
            LOG_ERROR << errno << ": Error in opening index file.";
            logger.reset();
            return -1;
        }
        fd = res;

        struct stat st;
        if (fstat(fd, &st) == -1)
        {
            LOG_ERROR << errno << ": Error in stat of index file.";
            close(fd);
            logger.reset();
            return -1;
        }

        eof = st.st_size;

        return 0;
    }

    void deinit()
    {
        close(fd);
        logger.reset();
    }

    /**
     * Updates the log index file with the offset of last log record and it's root hash.
     * @return Return 0 on success, -1 on error.
    */
    int update_log_index()
    {
        // If logger isn't initialized show error.
        if (!logger.has_value())
        {
            LOG_ERROR << errno << ": Logger isn't opened.";
            return -1;
        }

        // First read the header from the log file.
        if (logger->read_header() == -1)
        {
            LOG_ERROR << errno << ": Error in reading the log header.";
            return -1;
        }

        // Read the log header to get the offset.
        const hpfs::audit::log_header header = logger->get_header();

        // Read the last log record
        off_t next_offset;
        hpfs::audit::log_record log_record;
        if (logger->read_log_at(header.last_record, next_offset, log_record) == -1)
        {
            LOG_ERROR << errno << ": Error in reading last log record.";
            return -1;
        }

        // Offset of current log is the last log records offset. So it is taken from the log header.
        // Convert offset to big endian before persisting to the disk.
        uint8_t offset[8];
        util::uint64_to_bytes(offset, header.last_record);

        struct iovec iov_vec[2];
        iov_vec[0].iov_base = offset;
        iov_vec[0].iov_len = sizeof(offset);

        iov_vec[1].iov_base = &(log_record.state_hash);
        iov_vec[1].iov_len = sizeof(log_record.state_hash);

        if (writev(fd, iov_vec, 2) == -1)
        {
            LOG_ERROR << errno << ": Error writing to log index file.";
            return -1;
        }

        eof += sizeof(offset) + sizeof(log_record.state_hash);
        return 0;
    }

    /**
     * Reading the last root hash from the index file.
     * @param root_hash Last root hash.
     * @return Returns 0 on success, -1 on error.
    */
    int read_last_root_hash(hmap::hasher::h32 &root_hash)
    {
        // If index file is empty return empty hash.
        if (eof == 0)
        {
            root_hash = hmap::hasher::h32_empty;
            return 0;
        }

        // Read from the end of the file.
        if (pread(fd, &root_hash, sizeof(root_hash), eof - sizeof(root_hash)) < sizeof(root_hash))
        {
            LOG_ERROR << errno << ": Error when reading index file.";
            return -1;
        }

        return 0;
    }

    /**
     * Get the last sequence number from the index.
     * @return Returns the seqence number, 0 if file is empty.
    */
    uint64_t get_last_seq_no()
    {
        return eof / (sizeof(uint64_t) + sizeof(hmap::hasher::h32));
    }

    /**
     * Handles the log index control signals.
     * @param query Query provided from the outside.
     * @return Returns 0 on success, -1 error, 1 if invalid control.
    */
    int handle_log_index_control(std::string_view query)
    {
        if (query == INDEX_UPDATE_FILE)
            return update_log_index();
        
        return 1;
    }

} // namespace hpfs::logger_index