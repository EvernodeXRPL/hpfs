
#include "logger_index.hpp"
#include "audit.hpp"
#include "../tracelog.hpp"
#include "../util.hpp"

/**
 * Log index file keeps offset and the root_hash of log records.
 * Format - [log1_off,log1_r_hash,log2_off,log2_r_hash,log3_off,log3_r_hash...]
 * When a ledger created in the hpcore, offset and root_hash of the latest hpfs log is recorded.
 * So, the corresponding offsets for the ledger logs can be obtained from the index file by the ledger seq number.
*/
namespace hpfs::audit::logger_index
{
    constexpr int FILE_PERMS = 0644;
    constexpr const char *INDEX_UPDATE_QUERY = "/::hpfs.index";
    constexpr const char *INDEX_FILENAME_FULLSTOP = "/::hpfs.index.";

    int fd = -1;              // The index file fd used throughout the session.
    off_t eof = 0;            // End of file (End offset of index file).
    bool initialized = false; // Indicates that the index has been initialized properly.
    std::string index_file_path;
    std::optional<audit::audit_logger> logger;

    /**
     * Initialize the log index file.
     * @param file_path Path of the log index file.
     * @return Returns 0 on success, -1 on error.
    */
    int init(std::string_view file_path)
    {
        // Don't initialize if the merge is enabled.
        // No point of storing log offsets if the merger is purging the log records.
        if (ctx.merge_enabled)
            return 0;

        // First initialize the logger.
        if (audit::audit_logger::create(logger, audit::LOG_MODE::LOG_SYNC, ctx.log_file_path) == -1)
        {
            LOG_ERROR << errno << ": Error in opening log file.";
            logger.reset();
            return -1;
        }

        // Open or create the index file.
        fd = open(file_path.data(), O_CREAT | O_RDWR, FILE_PERMS);
        if (fd == -1)
        {
            LOG_ERROR << errno << ": Error in opening index file.";
            logger.reset();
            return -1;
        }

        struct stat st;
        if (fstat(fd, &st) == -1)
        {
            LOG_ERROR << errno << ": Error in stat of index file.";
            close(fd);
            fd = -1;
            logger.reset();
            return -1;
        }
        eof = st.st_size;
        initialized = true;
        return 0;
    }

    void deinit()
    {
        if (fd != -1)
        {
            close(fd);
            fd = -1;
        }
        logger.reset();
        initialized = false;
    }

    /**
     * Updates the log index file with the offset of last log record and it's root hash.
     * @return Return 0 on success, -1 on error.
    */
    int update_log_index()
    {
        // If logger isn't initialized show error.
        if (!initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
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

        // Read the last log record.
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

        iov_vec[1].iov_base = &(log_record.root_hash);
        iov_vec[1].iov_len = sizeof(log_record.root_hash);

        if (pwritev(fd, iov_vec, 2, eof) == -1)
        {
            LOG_ERROR << errno << ": Error writing to log index file.";
            return -1;
        }

        eof += sizeof(offset) + sizeof(log_record.root_hash);
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
     * @param query Query passed from the outside.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_write(std::string_view query)
    {
        // If logger index isn't initialized return no entry.
        if (query == INDEX_UPDATE_QUERY)
            return initialized ? update_log_index() : -ENOENT;

        return 1;
    }

    /**
     * Handles the log index control signals.
     * @param query Query passed from the outside.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_open(std::string_view query)
    {
        // If logger index isn't initialized return no entry.
        if (query == INDEX_UPDATE_QUERY)
            return initialized ? 0 : -ENOENT;

        return 1;
    }

    /**
     * Checks getattr requests for any index-related metadata activity.
     * @param query Query passed from the outside.
     * @param stbuf Stat to be populated if this is a index control
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int index_check_getattr(std::string_view query, struct stat *stbuf)
    {
        if (query == INDEX_UPDATE_QUERY)
        {
            // If logger index isn't initialized return no entry.
            if (!initialized)
                return -ENOENT;

            if (fstat(fd, stbuf) == -1)
            {
                LOG_ERROR << errno << ": Error in stat of index file.";
                return -1;
            }
            return 0;
        }
        else if (strncmp(query.data(), INDEX_FILENAME_FULLSTOP, 14) == 0)
        {
            // If logger index isn't initialized return no entry.
            if (!initialized)
                return -ENOENT;

            // Given path should contain a sequnce number.
            if (query.size() > 14)
            {
                if (fstat(fd, stbuf) == -1)
                {
                    LOG_ERROR << errno << ": Error in stat of index file.";
                    return -1;
                }
                return 0;
            }
            else
                return 1;
        }

        return 1;
    }

    /**
     * Checks truncate requests for index file related truncate operations. If so it is interpreted for log file and index file truncates.
     * @return 0 if request succesfully was interpreted by index truncate control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int index_check_truncate(const char *path)
    {
        std::string path_str(path);
        if (strncmp(path_str.c_str(), INDEX_FILENAME_FULLSTOP, 14) == 0)
        {
            // Given path should create a sequnce number.
            if (path_str.size() > 14)
            {
                uint64_t seq_no;
                if (util::stoull(path_str.substr(14), seq_no) == -1 ||
                    truncate_log_and_index_file(seq_no) == -1)
                {
                    LOG_ERROR << "Error truncating log file.";
                    return -1;
                };
                return 0;
            }
            else
                return 1;
        }
        else
            return 1;
    }

    /**
     * Truncate the log file including the log record from the given seq_no.
     * @param seq_no Sequence number to start truncating. (Inclusive)
     * @return Returns 0 on success and -1 on error or on merge enabled mode.
    */
    int truncate_log_and_index_file(const uint64_t seq_no)
    {
        // If logger isn't initialized, show an error.
        if (!initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
            return -1;
        }

        if (seq_no == 0)
        {
            LOG_ERROR << "Sequence number should be greater than zero for log truncation. Given: " << std::to_string(seq_no);
            return -1;
        }

        off_t log_offset;
        off_t prev_ledger_log_offset = 0;
        if (get_log_offset_from_index_file(log_offset, seq_no) == -1 ||
            (seq_no > 1 && get_log_offset_from_index_file(prev_ledger_log_offset, seq_no - 1) == -1)) // Get log record offset of previous record only if seq number is greater than 1.
        {
            LOG_ERROR << "Error getting log offset from index file";
            return -1;
        }

        if (log_offset < 1)
        {
            LOG_ERROR << "Offset must be greater than zero to truncate the log file.";
            return -1;
        }

        const off_t end_of_index = get_data_offset_of_index_file(seq_no);
        if (logger->truncate_log_file(log_offset, prev_ledger_log_offset) == -1 ||
            ftruncate(fd, end_of_index) == -1)
        {
            LOG_ERROR << "Error truncating log and index file";
            return -1;
        }

        eof = end_of_index;
        return 0;
    }

    /**
     * Gets the log record offset corresponding to the given seq_no from the index file.
     * @param log_offset Log offset of the given sequence number.
     * @param seq_no Sequnce number to obtain the relevant log record offset.
     * @return Returns 0 on success and -1 on error.
    */
    int get_log_offset_from_index_file(off_t &log_offset, const uint64_t seq_no)
    {
        uint8_t offset_bytes[8];
        off_t data_offset = get_data_offset_of_index_file(seq_no);
        if (pread(fd, offset_bytes, 8, data_offset) < 8)
        {
            LOG_ERROR << errno << ": Error getting log offset for seq_no: " << std::to_string(seq_no);
            return -1;
        }
        log_offset = util::uint64_from_bytes(offset_bytes);
        return 0;
    }

    /**
     * Gives the offset of the index file to the given sequence number.
     * @param seq_no Sequence number.
     * @return Returns the offset of the data of the given sequence number.
    */
    off_t get_data_offset_of_index_file(const uint64_t seq_no)
    {
        return (seq_no - 1) * (sizeof(uint64_t) + sizeof(hmap::hasher::h32));
    }

} // namespace hpfs::audit::logger_index