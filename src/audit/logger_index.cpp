
#include "logger_index.hpp"
#include "../tracelog.hpp"
#include "../util.hpp"
#include "../version.hpp"

/**
 * Log index file keeps offset and the root_hash of log records.
 * Format - [log1_off,log1_r_hash,log2_off,log2_r_hash,log3_off,log3_r_hash...]
 * When a ledger created in the hpcore, offset and root_hash of the latest hpfs log is recorded.
 * So, the corresponding offsets for the ledger logs can be obtained from the index file by the ledger seq number.
*/
namespace hpfs::audit::logger_index
{
    constexpr const int FILE_PERMS = 0644;
    constexpr const char *INDEX_UPDATE_QUERY = "/::hpfs.index";
    constexpr const int INDEX_UPDATE_QUERY_LEN = 13;
    constexpr const char *INDEX_UPDATE_QUERY_FULLSTOP = "/::hpfs.index.";
    constexpr const int INDEX_UPDATE_QUERY_FULLSTOP_LEN = 14;
    constexpr const char *INDEX_READ_QUERY_FULLSTOP = "/::hpfs.index.read.";
    constexpr const int INDEX_READ_QUERY_FULLSTOP_LEN = 19;
    constexpr const char *INDEX_WRITE_QUERY_FULLSTOP = "/::hpfs.index.write.";
    constexpr const int INDEX_WRITE_QUERY_FULLSTOP_LEN = 20;

    // Max read log size for one call. Set to 4MB.
    constexpr uint64_t MAX_LOG_READ_SIZE = 4 * 1024 * 1024;

    index_context index_ctx;

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

        // Open or create the index file.
        if (!util::is_file_exists(file_path))
        {
            // Create new file and include version header.
            index_ctx.fd = open(file_path.data(), O_CREAT | O_RDWR, FILE_PERMS);
            if (index_ctx.fd != -1)
            {
                if (write(index_ctx.fd, version::HP_VERSION_BYTES, version::VERSION_BYTES_LEN) < version::VERSION_BYTES_LEN)
                {
                    LOG_ERROR << errno << ": Error adding version header to the hpfs index file";
                    close(index_ctx.fd);
                    return -1;
                }
            }
        }
        else
        {
            // Open an already existing file.
            index_ctx.fd = open(file_path.data(), O_RDWR);
        }
        if (index_ctx.fd == -1)
        {
            LOG_ERROR << errno << ": Error in opening index file.";
            return -1;
        }

        struct stat st;
        if (fstat(index_ctx.fd, &st) == -1)
        {
            LOG_ERROR << errno << ": Error in stat of index file.";
            close(index_ctx.fd);
            index_ctx.fd = -1;
            return -1;
        }
        index_ctx.eof = st.st_size;
        index_ctx.initialized = true;
        return 0;
    }

    void deinit()
    {
        if (index_ctx.fd != -1)
        {
            close(index_ctx.fd);
            index_ctx.fd = -1;
        }
        index_ctx.initialized = false;
    }

    /**
     * Updates the log index file with the offset of last log record and it's root hash.
     * @param seq_no Seq number to update the index.
     * @return Return 0 on success, -1 on error.
    */
    int update_log_index(const uint64_t seq_no)
    {
        // If logger isn't initialized show error.
        if (!index_ctx.initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
            return -1;
        }

        std::optional<audit::audit_logger> logger;

        // First initialize the logger.
        if (audit::audit_logger::create(logger, audit::LOG_MODE::LOG_SYNC_READ, ctx.log_file_path) == -1)
        {
            LOG_ERROR << "Error initializing log.";
            return -1;
        }

        // Read the log header to get the offset.
        const hpfs::audit::log_header header = logger->get_header();

        // Check whether previous index data are populated.
        // If not populate them with last updated data.
        const std::uint64_t last_seq_no = get_last_seq_no();
        const size_t missing_count = seq_no - last_seq_no - 1;
        struct iovec iov_vec[(missing_count + 1) * 2];

        uint8_t offset_be[8];
        off_t next_offset;
        log_record log_record;

        if (missing_count > 0)
        {
            off_t prev_offset;
            hmap::hasher::h32 prev_root_hash;
            if (last_seq_no == 0)
            {
                // If index is empty populate the first log record's root hash and offset.
                prev_offset = header.first_record;
                if (logger->read_log_at(prev_offset, next_offset, log_record) == -1)
                {
                    LOG_ERROR << "Error in reading the first log at offset " << prev_offset;
                    return -1;
                }
                prev_root_hash = log_record.root_hash;
            }
            else if (get_last_index_data(prev_offset, prev_root_hash) == -1)
                return -1;

            util::uint64_to_bytes(offset_be, prev_offset);

            // Populate missing data
            for (int i = 0; i < missing_count; i++)
            {
                iov_vec[i * 2].iov_base = offset_be;
                iov_vec[i * 2].iov_len = sizeof(offset_be);

                iov_vec[(i * 2) + 1].iov_base = &(prev_root_hash);
                iov_vec[(i * 2) + 1].iov_len = sizeof(prev_root_hash);
            }
        }

        // Read the last log record.
        if (logger->read_log_at(header.last_record, next_offset, log_record) == -1)
        {
            LOG_ERROR << "Error in reading the last log at offset " << header.last_record;
            return -1;
        }

        // Offset of current log is the last log records offset. So it is taken from the log header.
        // Convert offset to big endian before persisting to the disk.
        util::uint64_to_bytes(offset_be, header.last_record);

        iov_vec[missing_count * 2].iov_base = offset_be;
        iov_vec[missing_count * 2].iov_len = sizeof(offset_be);

        iov_vec[(missing_count * 2) + 1].iov_base = &(log_record.root_hash);
        iov_vec[(missing_count * 2) + 1].iov_len = sizeof(log_record.root_hash);

        if (pwritev(index_ctx.fd, iov_vec, (missing_count + 1) * 2, index_ctx.eof) == -1)
        {
            LOG_ERROR << errno << ": Error writing to log index file.";
            return -1;
        }

        index_ctx.eof += (sizeof(offset_be) + sizeof(log_record.root_hash)) * (missing_count + 1);

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
        if (index_ctx.eof == version::VERSION_BYTES_LEN)
        {
            root_hash = hmap::hasher::h32_empty;
            return 0;
        }

        // Read from the end of the file.
        if (pread(index_ctx.fd, &root_hash, sizeof(root_hash), index_ctx.eof - sizeof(root_hash)) < sizeof(root_hash))
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
        return (index_ctx.eof - version::VERSION_BYTES_LEN) / (sizeof(uint64_t) + sizeof(hmap::hasher::h32));
    }

    /**
     * Get the last log offset and root hash from the index.
     * @param offset Offset of the last log record.
     * @param root_hash Root hash of the last log record.
     * @return Returns -1 on error otherwise 0.
    */
    int get_last_index_data(off_t &offset, hmap::hasher::h32 &root_hash)
    {
        const uint64_t seq_no = get_last_seq_no();
        if (get_log_offset_from_index_file(offset, seq_no) == -1 || read_last_root_hash(root_hash) == -1)
        {
            LOG_ERROR << "Error reading last offset and root hash from the index file.";
            return -1;
        }

        return 0;
    }

    /**
     * Read the log offset of a given position.
     * @param offset Log offset if requested position is equal to the eof offset will be 0.
     * @param pos Position of the log.
     * @return Returns -1 on error otherwise 0.
    */
    int read_offset(off_t &offset, const uint64_t seq_no)
    {
        // If logger isn't initialized show error.
        if (!index_ctx.initialized)
        {
            LOG_ERROR << "Index hasn't been initialized properly.";
            return -1;
        }
        else if (index_ctx.eof == version::VERSION_BYTES_LEN)
        {
            LOG_ERROR << "Index hasn't been updated.";
            return -1;
        }

        // Calculate offset position of the index.
        const uint64_t index_offset = get_data_offset_of_index_file(seq_no);
        // If the offset is end of file set offset 0.
        if (index_offset == index_ctx.eof)
        {
            offset = 0;
            return 0;
        }
        else if (index_offset > index_ctx.eof)
        {
            LOG_ERROR << "Invalid index offset.";
            return -1;
        }

        // Reading the offset value as big endian by calculating the offset position from the sequence number.
        uint8_t be_offset[8];
        if (pread(index_ctx.fd, &be_offset, sizeof(be_offset), index_offset) < sizeof(be_offset))
        {
            LOG_ERROR << errno << ": Error reading log index file. " << seq_no;
            return -1;
        }

        // Convert big endian value to unit64_t;
        offset = util::uint64_from_bytes(be_offset);
        return 0;
    }

    /**
     * Read the hash of a given position.
     * @param hash Hash at the given position.
     * @param seq_no Sequence number of the log.
     * @return Returns -1 on error otherwise 0.
    */
    int read_hash(hmap::hasher::h32 &hash, const uint64_t seq_no)
    {
        // If logger isn't initialized show error.
        if (!index_ctx.initialized)
        {
            LOG_ERROR << "Index hasn't been initialized properly.";
            return -1;
        }
        else if (index_ctx.eof == version::VERSION_BYTES_LEN)
        {
            LOG_ERROR << "Index hasn't been updated.";
            return -1;
        }

        // Calculate offset position of the index.
        const uint64_t index_offset = get_data_offset_of_index_file(seq_no);
        if (index_offset >= index_ctx.eof)
            return -1;

        // Reading the hash value.
        if (pread(index_ctx.fd, &hash, sizeof(hmap::hasher::h32), index_offset + sizeof(hmap::hasher::h32)) < sizeof(hmap::hasher::h32))
        {
            LOG_ERROR << errno << ": Error reading log index file. " << seq_no;
            return -1;
        }

        return 0;
    }

    /**
     * Reading the log records withing min and max seq_no range and populate them along with the seq_no.
     * @param buf Buffer to populate logs. Layout [00000000][log record][00000000][log record]....[seq_no1][log record][00000000][log record]....
     * @param min_seq_no Minimum seq_no to start scanning. If 0 start from the first record of the log.
     * @param max_seq_no Maximum seq_no to stop scanning. If 0 give the records until the end.
     * @param max_size Max buffer size to read. If 0 buffer size is unlimited.
     * @return Returns 0 on success, -1 on error.
    */
    int read_log_records(std::string &buf, const uint64_t min_seq_no, const uint64_t max_seq_no, const uint64_t max_size)
    {
        // If logger isn't initialized show error.
        if (!index_ctx.initialized)
        {
            LOG_ERROR << "Index hasn't been initialized properly.";
            return -1;
        }
        else if (index_ctx.eof == version::VERSION_BYTES_LEN)
        {
            LOG_ERROR << "Index hasn't been updated.";
            return -1;
        }
        else if (max_seq_no > 0 && min_seq_no > max_seq_no)
        {
            LOG_ERROR << "min_seq_no cannot be greated than max_seq_no.";
            return -1;
        }

        std::optional<audit::audit_logger> logger;

        // First initialize the logger.
        if (audit::audit_logger::create(logger, audit::LOG_MODE::LOG_SYNC_READ, ctx.log_file_path) == -1)
        {
            LOG_ERROR << "Error initializing log.";
            return -1;
        }

        off_t current_offset, max_offset;

        // If min seq no is 0 start collecting from the very begining.
        if (min_seq_no == 0)
            current_offset = 0;

        // If max seq mo is 0 collect until the end.
        if (max_seq_no == 0)
            max_offset = 0;

        if (min_seq_no > 0 && read_offset(current_offset, min_seq_no) == -1 ||
            max_seq_no > 0 && read_offset(max_offset, max_seq_no) == -1)
            return -1;

        uint64_t seq_no = min_seq_no;
        off_t next_seq_no_offset;
        // Take the offset of the frontmost seq_no log record.
        // if current offset is 0 take the 1st seq_no log record's offset.
        if (current_offset == 0)
        {
            if (read_offset(next_seq_no_offset, ++seq_no) == -1)
                return -1;
        }
        else
            next_seq_no_offset = current_offset;

        // Tempory buffer to keep the intermidiate records bitween seq_no log records.
        // To ensure endpoint of the main buf is a seq_no log record.
        std::string record_buf;

        // Reserving space for response header [resonding_max_seq_no].
        buf.resize(sizeof(uint64_t));

        // Loop until the max_offset
        while (max_offset == 0 || current_offset <= max_offset)
        {
            record_buf.resize(record_buf.length() + sizeof(seq_no));
            // If we reached to a seq_no log record
            const bool is_seq_no_log = (next_seq_no_offset == current_offset);
            // If this is a seq_no log append seq_no to the buf and read the next seq_no record's offset.
            // otherwise it's 0.
            if (is_seq_no_log)
            {
                memcpy(record_buf.data() + record_buf.length() - sizeof(seq_no), &seq_no, sizeof(seq_no));
                // Loop seq_no and get log offset from index until we receive a different log offset.
                // So, while loop will end with seq_no of the next log record.
                while (next_seq_no_offset != 0 && next_seq_no_offset == current_offset)
                {
                    if (read_offset(next_seq_no_offset, ++seq_no) == -1)
                        return -1;
                }
            }
            else
                memset(record_buf.data() + record_buf.length() - sizeof(seq_no), 0, sizeof(seq_no));

            // Read the log record buf at current offset.
            // After reading current_offset would be next records offset.
            std::string log_record;
            if (logger->read_log_record_buf_at(current_offset, current_offset, log_record) == -1)
                return -1;
            record_buf.append(log_record);
            log_record.clear();

            // If reached the max_size limit break from the loop before adding the temp buffer to main and return the collected buffer.
            if (max_size != 0 && (buf.length() + record_buf.length()) > (max_size - sizeof(uint64_t)))
                break;

            // If the current log record is a seq_no log record, Append collected to the main buffer and clean the temp buffer.
            // Buffer layout - [00000000][log record][00000000][log record]....[seq_no1][log record][00000000][log record]...
            if (is_seq_no_log)
            {
                buf.append(record_buf);
                record_buf.resize(0);
            }

            // If we reached to the eof of the log file, break from the loop and return collected.
            if (current_offset == 0)
            {
                // If we reach this point, it means requested max is beyond our log (Ex: max_seq_no = 0).
                // So we append collected all the records up until to the end of our log.
                buf.append(record_buf);
                record_buf.clear();
                break;
            }
        }

        // Setting the last scanned seq number as the max seq number header of the response.
        memcpy(buf.data(), &(--seq_no), sizeof(seq_no));

        return 0;
    }

    /**
     * Appending log records to hpfs log file.
     * @param buf Buffer to append.
     * @param size Size of the buffer.
     * @return Returns 1 if success, 0 if joining point check failed, otherwise -1 on error.
    */
    int append_log_records(const char *buf, const size_t size)
    {
        // If logger isn't initialized show error.
        if (!index_ctx.initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
            return -1;
        }

        std::optional<audit::audit_logger> logger;
        std::optional<vfs::virtual_filesystem> virt_fs;
        std::optional<hmap::tree::hmap_tree> htree;

        // First initialize the logger virtual fs and htree.
        if (audit::audit_logger::create(logger, audit::LOG_MODE::LOG_SYNC_WRITE, ctx.log_file_path) == -1 ||
            vfs::virtual_filesystem::create(virt_fs, false, ctx.seed_dir, logger.value()) == -1 ||
            hmap::tree::hmap_tree::create(htree, virt_fs.value()) == -1)
        {
            LOG_ERROR << "Error initializing log, virtual fs and htree.";
            return -1;
        }

        uint64_t last_seq_no = get_last_seq_no();
        hmap::hasher::h32 last_root_hash;
        if (read_last_root_hash(last_root_hash) == -1)
            return -1;

        off_t offset = 0;
        bool first_record = true;
        off_t prev_offset;
        hmap::hasher::h32 prev_root_hash;
        uint64_t prev_seq_no;

        // Reading the max seq number header value.
        const uint64_t *received_max_seq_no = (const uint64_t *)std::string_view(buf + offset, 8).data();
        offset += 8;

        while (offset < size)
        {
            const uint64_t *seq_no = (const uint64_t *)std::string_view(buf + offset, 8).data();
            offset += 8;
            const log_record_header *rh = (const log_record_header *)std::string_view(buf + offset, sizeof(log_record_header)).data();
            offset += sizeof(log_record_header);
            const std::string vpath(buf + offset, rh->vpath_len);
            offset += rh->vpath_len;
            std::string_view payload(buf + offset, rh->payload_len);
            offset += rh->payload_len;
            std::string_view block_data(buf + offset, rh->block_data_len);
            offset += rh->block_data_len;

            // We might receive responses which are now outdated. So those will be skipped here.
            // So if the first log record's seq no in the log buffer isn't match with our last seq no in the index, we skip the response.
            // If index file is empty don't check for joining point. Append all records.
            if (first_record)
            {
                if (index_ctx.eof == version::VERSION_BYTES_LEN)
                {
                    // Read the log header to get the offset.
                    const hpfs::audit::log_header header = logger->get_header();

                    prev_seq_no = 0;
                    prev_offset = header.first_record;
                    off_t next_offset;
                    log_record log_record;
                    if (logger->read_log_at(prev_offset, next_offset, log_record) == -1)
                    {
                        LOG_ERROR << "Error reading log at offset " << prev_offset;
                        return -1;
                    }
                    prev_root_hash = log_record.root_hash;
                }
                else
                {
                    if (*seq_no != last_seq_no || rh->root_hash != last_root_hash)
                    {
                        LOG_DEBUG << "Invalid joining point in the received log response. Received seq no " << *seq_no << " Last seq no " << last_seq_no;
                        return 0;
                    }

                    prev_seq_no = *seq_no;
                    prev_root_hash = rh->root_hash;
                    if (get_log_offset_from_index_file(prev_offset, *seq_no) == -1)
                    {
                        LOG_ERROR << "Error getting offset from index file seq no " << *seq_no;
                        return -1;
                    }

                    first_record = false;
                    continue;
                }
            }
            first_record = false;

            off_t log_offset;
            hmap::hasher::h32 log_root_hash;
            if ((rh->root_hash != hmap::hasher::h32_empty && rh->operation != 0) &&
                persist_log_record(logger.value(), virt_fs.value(), htree.value(), *seq_no, rh->operation, vpath, payload, block_data, log_offset, log_root_hash) == -1)
            {
                LOG_ERROR << "Error persisting log record. seq no " << *seq_no;
                return -1;
            }

            // If this is a seq no log record, update the log index file.
            if (*seq_no != 0)
            {
                uint8_t offset_be[8];

                const size_t missing_count = *seq_no - prev_seq_no - 1;
                struct iovec iov_vec[(missing_count + 1) * 2];

                // Update index with last log records offset and root hash.
                util::uint64_to_bytes(offset_be, prev_offset);
                for (int i = 0; i < missing_count; i++)
                {
                    iov_vec[i * 2].iov_base = offset_be;
                    iov_vec[i * 2].iov_len = sizeof(offset_be);

                    iov_vec[(i * 2) + 1].iov_base = &(prev_root_hash);
                    iov_vec[(i * 2) + 1].iov_len = sizeof(prev_root_hash);
                }

                // Update index for the current log record.
                util::uint64_to_bytes(offset_be, log_offset);
                iov_vec[missing_count * 2].iov_base = offset_be;
                iov_vec[missing_count * 2].iov_len = sizeof(offset_be);

                iov_vec[(missing_count * 2) + 1].iov_base = &(log_root_hash);
                iov_vec[(missing_count * 2) + 1].iov_len = sizeof(log_root_hash);

                if (pwritev(index_ctx.fd, iov_vec, (missing_count + 1) * 2, index_ctx.eof) == -1)
                {
                    LOG_ERROR << errno << ": Error writing to log index file.";
                    return -1;
                }

                index_ctx.eof += (sizeof(offset_be) + sizeof(log_root_hash)) * (missing_count + 1);

                prev_seq_no = *seq_no;
                prev_offset = log_offset;
                prev_root_hash = log_root_hash;
            }
        }

        // If there're no log records for the seq numbers < max_seq_no.
        // Update the index for them with the offset and root hash value of last log record.
        last_seq_no = get_last_seq_no();
        if (*received_max_seq_no > last_seq_no)
        {
            off_t prev_offset;
            hmap::hasher::h32 prev_root_hash;
            if (get_last_index_data(prev_offset, prev_root_hash) == -1)
                return -1;

            uint8_t offset_be[8];
            util::uint64_to_bytes(offset_be, prev_offset);

            const size_t missing_count = *received_max_seq_no - last_seq_no;
            struct iovec iov_vec[missing_count * 2];

            // Populate missing data with last log record's offset and root hash.
            for (int i = 0; i < missing_count; i++)
            {
                iov_vec[i * 2].iov_base = offset_be;
                iov_vec[i * 2].iov_len = sizeof(offset_be);

                iov_vec[(i * 2) + 1].iov_base = &(prev_root_hash);
                iov_vec[(i * 2) + 1].iov_len = sizeof(prev_root_hash);
            }

            if (pwritev(index_ctx.fd, iov_vec, missing_count * 2, index_ctx.eof) == -1)
            {
                LOG_ERROR << errno << ": Error writing to log index file.";
                return -1;
            }

            index_ctx.eof += (sizeof(offset_be) + sizeof(prev_root_hash)) * missing_count;
        }

        return 1;
    }

    /**
     * Persisting log records and updating the hashes.
     * @param logger Logger instance.
     * @param virt_fs Virtual file system instance.Seq no of the log record
     * @param htree Hash tree instance.
     * @param seq_no Seq no of the log record.
     * @param op File operation.
     * @param vpath Vpath in the log record.
     * @param paylod Payload in the log record.
     * @param block_data Block data in the log record.
     * @param log_offset Offset of the newly added log record.
     * @param root_hash Updated root hash after persisting the log record.
     * @return Returns 0 on success, -1 on error.
    */
    int persist_log_record(audit::audit_logger &logger, vfs::virtual_filesystem &virt_fs, hmap::tree::hmap_tree &htree, const uint64_t seq_no, const audit::FS_OPERATION op,
                           const std::string &vpath, std::string_view payload, std::string_view block_data, off_t &log_offset, hmap::hasher::h32 &root_hash)
    {
        const iovec payload_vec{(void *)payload.data(), payload.size()};
        std::vector<iovec> block_data_vec;
        block_data_vec.push_back({(void *)block_data.data(), block_data.size()});

        vfs::vnode *vn = NULL;
        if (virt_fs.get_vnode(vpath, &vn) == -1)
        {
            LOG_ERROR << "Error fetching the vnode " << vpath << " " << op;
            return -1;
        }
        if (op == audit::FS_OPERATION::MKDIR || op == audit::FS_OPERATION::CREATE)
        {
            if (vn)
                return -EEXIST;
        }
        else if (!vn)
            return -ENOENT;

        log_record_header rh;
        log_offset = logger.append_log(rh, vpath, op, &payload_vec,
                                       block_data_vec.data(), block_data_vec.size());

        if (log_offset == 0)
        {
            LOG_ERROR << "Error appending logs.";
            return -1;
        }

        // After appending the log records and calculating the hash. Update the vfs.
        if (virt_fs.build_vfs() == -1)
        {
            LOG_ERROR << "Error building the virtual file system.";
            return -1;
        }

        switch (op)
        {
        case (audit::FS_OPERATION::MKDIR):
        case (audit::FS_OPERATION::CREATE):
        {
            if (htree.apply_vnode_create(vpath) == -1)
                return -1;
            break;
        }
        case (audit::FS_OPERATION::WRITE):
        {
            const op_write_payload_header *wh = (const op_write_payload_header *)payload.data();
            if (htree.apply_vnode_data_update(vpath, *vn, wh->offset, wh->size) == -1)
                return -1;
            break;
        }
        case (audit::FS_OPERATION::TRUNCATE):
        {
            const hpfs::audit::op_truncate_payload_header *th = (const op_truncate_payload_header *)payload.data();
            if (htree.apply_vnode_data_update(vpath, *vn, MIN(th->size, vn->st.st_size), MAX(0, th->size - vn->st.st_size)) == -1)
                return -1;
            break;
        }
        case (audit::FS_OPERATION::CHMOD):
        {
            if (htree.apply_vnode_metadata_update(vpath, *vn) == -1)
                return -1;
            break;
        }
        case (audit::FS_OPERATION::RMDIR):
        case (audit::FS_OPERATION::UNLINK):
        {
            if (htree.apply_vnode_delete(vpath) == -1)
                return -1;
            break;
        }
        case (audit::FS_OPERATION::RENAME):
        {
            const std::string to_vpath(std::string_view((char *)payload.data(), payload.size() - 1));
            if (htree.apply_vnode_rename(vpath, to_vpath, !S_ISREG(vn->st.st_mode)) == -1)
                return -1;
            break;
        }
        default:
            return -1;
        }

        root_hash = htree.get_root_hash();

        // Update the log record with root hash.
        if (logger.update_log_record_hash(log_offset, root_hash, rh) == -1)
            return -1;

        return 0;
    }

    /**
     * Checks request for any read.
     * @param query Query passed from the outside.
     * @param buf Data buffer to be returned.
     * @param size Read size.
     * @param offset Read offset.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_read(std::string_view query, char *buf, size_t *size, const off_t offset)
    {
        if (query.length() > INDEX_READ_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_READ_QUERY_FULLSTOP, INDEX_READ_QUERY_FULLSTOP_LEN) == 0)
        {
            // If logger index isn't initialized return no entry.
            if (!index_ctx.initialized)
                return -ENOENT;

            // Serve the read requests with the read buffer which is populated in the file open.
            if (offset >= index_ctx.read_buf.length()) // If the requested offset is beyond our buffer size.
                *size = 0;
            else if (index_ctx.read_buf.length() - offset <= *size) // If we are sending the last page.
            {
                *size = index_ctx.read_buf.length() - offset;
                memcpy(buf, index_ctx.read_buf.c_str() + offset, *size);
            }
            else
                memcpy(buf, index_ctx.read_buf.c_str() + offset, *size);
            return 0;
        }

        return 1;
    }

    /**
     * Checks request for any write.
     * @param query Query passed from the outside.
     * @param buf Buffer to be written.
     * @param size Write size.
     * @param offset Write offset.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_write(std::string_view query, const char *buf, size_t *size, const off_t offset)
    {
        if (query.length() > INDEX_WRITE_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_WRITE_QUERY_FULLSTOP, INDEX_WRITE_QUERY_FULLSTOP_LEN) == 0)
        {
            // If logger index isn't initialized return no entry.
            if (!index_ctx.initialized)
                return -ENOENT;

            // Populated the write buffer with receiving data until we reach the end of the buffer.
            if (offset >= index_ctx.write_buf.length()) // If the requested offset is beyond our buffer size.
                *size = 0;
            else if (index_ctx.write_buf.length() - offset <= *size) // If we are writing the last page.
            {
                *size = index_ctx.write_buf.length() - offset;
                memcpy(index_ctx.write_buf.data() + offset, buf, *size);
            }
            else
                memcpy(index_ctx.write_buf.data() + offset, buf, *size);

            return 0;
        }
        else if (query.length() > INDEX_UPDATE_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_UPDATE_QUERY_FULLSTOP, INDEX_UPDATE_QUERY_FULLSTOP_LEN) == 0)
        {
            // If logger index isn't initialized return no entry.
            if (!index_ctx.initialized)
                return -ENOENT;

            // Split the query by '.'.
            const std::vector<std::string> params = util::split_string(query, ".");
            uint64_t seq_no;
            if (params.size() != 3 || util::stoull(params.at(2).data(), seq_no) == -1)
            {
                LOG_ERROR << "Index update parameter error: Invalid parameters";
                return -1;
            }

            if (update_log_index(seq_no) == -1)
            {
                LOG_ERROR << "Error updating log index: Seq no " << seq_no;
                return -1;
            }

            return 0;
        }

        return 1;
    }

    /**
     * Checks open request for the index.
     * @param query Query passed from the outside.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_open(std::string_view query)
    {
        // We are populating read buffer at the open operation of the ::hpfs.index.read file.
        if (query.length() > INDEX_READ_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_READ_QUERY_FULLSTOP, INDEX_READ_QUERY_FULLSTOP_LEN) == 0)
        {
            // If logger index isn't initialized return no entry.
            if (!index_ctx.initialized)
                return -ENOENT;

            // Split the query by '.'.
            const std::vector<std::string> params = util::split_string(query, ".");
            uint64_t min_seq_no, max_seq_no;
            if (params.size() != 5 ||
                util::stoull(params.at(3).data(), min_seq_no) == -1 ||
                util::stoull(params.at(4).data(), max_seq_no) == -1)
            {
                LOG_ERROR << "Log read parameter error: Invalid parameters";
                return -1;
            }

            if (read_log_records(index_ctx.read_buf, min_seq_no, max_seq_no, MAX_LOG_READ_SIZE) == -1)
            {
                LOG_ERROR << "Error reading logs: Seq no from " << min_seq_no << " to " << max_seq_no;
                return -1;
            }

            return 0;
        }
        // We are allocating the write buffer at the open operation of the ::hpfs.index.write file.
        else if (query.length() > INDEX_WRITE_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_WRITE_QUERY_FULLSTOP, INDEX_WRITE_QUERY_FULLSTOP_LEN) == 0)
        {
            // If logger index isn't initialized return no entry.
            if (!index_ctx.initialized)
                return -ENOENT;

            // Split the query by '.'.
            const std::vector<std::string> params = util::split_string(query, ".");
            size_t buf_len;

            if (params.size() != 4 || util::stoull(params.at(3).data(), buf_len) == -1)
            {
                LOG_ERROR << "Log write parameter error: Invalid parameters";
                return -1;
            }

            // Resize the write buffer to the buffer length.
            index_ctx.write_buf.resize(buf_len);

            return 0;
        }
        else if (query.length() > INDEX_UPDATE_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_UPDATE_QUERY_FULLSTOP, INDEX_UPDATE_QUERY_FULLSTOP_LEN) == 0)
            return index_ctx.initialized ? 0 : -ENOENT;

        return 1;
    }

    /**
     * Flush the index file after closing. Temporary read buf will be cleaned and write buffer will be appended.
     * @param query Query passed from the outside.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_flush(std::string_view query)
    {
        // Resize the read buffer to 0 when releasing the ::hpfs.index.read file.
        if (query.length() > INDEX_READ_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_READ_QUERY_FULLSTOP, INDEX_READ_QUERY_FULLSTOP_LEN) == 0)
        {
            index_ctx.read_buf.resize(0);
            return 0;
        }
        // Append the collected write buffer and resize the write buffer to 0 when releasing the ::hpfs.index.write file.
        else if (query.length() > INDEX_WRITE_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_WRITE_QUERY_FULLSTOP, INDEX_WRITE_QUERY_FULLSTOP_LEN) == 0)
        {
            const int res = append_log_records(index_ctx.write_buf.c_str(), index_ctx.write_buf.length());
            if (res == -1 || res == 0)
            {
                if (res == -1)
                    LOG_ERROR << "Error appending logs";
                index_ctx.write_buf.resize(0);
                return -1;
            }
            index_ctx.write_buf.resize(0);
            return 0;
        }

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
        if (query.length() > INDEX_UPDATE_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_UPDATE_QUERY_FULLSTOP, INDEX_UPDATE_QUERY_FULLSTOP_LEN) == 0)
        {
            // If logger index isn't initialized return no entry.
            if (!index_ctx.initialized)
                return -ENOENT;
            if (fstat(index_ctx.fd, stbuf) == -1)
            {
                LOG_ERROR << errno << ": Error in stat of index file.";
                return -1;
            }
            stbuf->st_size = MAX_LOG_READ_SIZE;
            return 0;
        }

        return 1;
    }

    /**
     * Checks truncate requests for index file related truncate operations. If so it is interpreted for log file and index file truncates.
     * @param query passed from the outside.
     * @return 0 if request succesfully was interpreted by index truncate control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
     */
    int index_check_truncate(std::string_view query)
    {
        // Given path should create a sequnce number.
        if (query.length() > INDEX_UPDATE_QUERY_FULLSTOP_LEN && strncmp(query.data(), INDEX_UPDATE_QUERY_FULLSTOP, INDEX_UPDATE_QUERY_FULLSTOP_LEN) == 0)
        {
            // If logger index isn't initialized, return no entry.
            if (!index_ctx.initialized)
                return -ENOENT;

            // Split the query by '.'.
            const std::vector<std::string> params = util::split_string(query, ".");
            uint64_t seq_no;
            if (params.size() != 3 || util::stoull(params.at(2).data(), seq_no) == -1)
            {
                LOG_ERROR << "Index truncate parameter error: Invalid parameters";
                return -1;
            }

            if (truncate_log_and_index_file(seq_no) == -1)
            {
                LOG_ERROR << "Error truncating log and index file.";
                return -1;
            }
            return 0;
        }

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
        if (!index_ctx.initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
            return -1;
        }

        std::optional<audit::audit_logger> logger;
        std::optional<vfs::virtual_filesystem> virt_fs;
        std::optional<hmap::tree::hmap_tree> htree;

        // First initialize the logger virtual fs and htree.
        if (audit::audit_logger::create(logger, audit::LOG_MODE::LOG_SYNC_WRITE, ctx.log_file_path) == -1 ||
            vfs::virtual_filesystem::create(virt_fs, false, ctx.seed_dir, logger.value()) == -1 ||
            hmap::tree::hmap_tree::create(htree, virt_fs.value()) == -1)
        {
            LOG_ERROR << "Error initializing log, virtual fs and htree.";
            return -1;
        }

        off_t log_offset;
        if (seq_no == 0)
            log_offset = 0;
        else if (get_log_offset_from_index_file(log_offset, seq_no) == -1)
        {
            LOG_ERROR << "Error getting log offset from index file";
            return -1;
        }

        // If the seq no to truncate is 0 we set the index truncating offset to version header length;
        const off_t end_of_index = seq_no > 0 ? get_data_offset_of_index_file(seq_no + 1) : version::VERSION_BYTES_LEN;

        // Truncate index file.
        if (ftruncate(index_ctx.fd, end_of_index) == -1)
        {
            LOG_ERROR << errno << " Error truncating index file";
            return -1;
        }

        // Truncate log file.
        if (logger->truncate_log_file(log_offset) == -1)
        {
            LOG_ERROR << "Error truncating log file";
            return -1;
        }

        index_ctx.eof = end_of_index;

        // Reaching this point means truncation is successful. Delete existing hmap and re-calculate.

        hmap::hasher::h32 root_hash;
        if (virt_fs->re_build_vfs() == -1 ||            // Clear and re-build vfs.
            htree->re_build_hash_maps(root_hash) == -1) // Clear and re-build hash map.
        {
            LOG_ERROR << "Error re-calculating root hash after truncation.";
            return -1;
        }
        LOG_DEBUG << "New root hash after truncation: " << root_hash;

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
        const off_t data_offset = get_data_offset_of_index_file(seq_no);
        if (pread(index_ctx.fd, offset_bytes, 8, data_offset) < sizeof(offset_bytes))
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
        return version::VERSION_BYTES_LEN + (seq_no - 1) * (sizeof(uint64_t) + sizeof(hmap::hasher::h32));
    }

} // namespace hpfs::audit::logger_index