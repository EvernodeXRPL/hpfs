
#include "logger_index.hpp"
#include "../tracelog.hpp"
#include "../util.hpp"
#include "../version.hpp"
#include "../vfs/virtual_filesystem.hpp"
#include "../hmap/tree.hpp"

/**
 * Log index file keeps offset and the root_hash of log records.
 * Format - [log1_off,log1_r_hash,log2_off,log2_r_hash,log3_off,log3_r_hash...]
 * When a ledger created in the hpcore, offset and root_hash of the latest hpfs log is recorded.
 * So, the corresponding offsets for the ledger logs can be obtained from the index file by the ledger seq number.
*/
namespace hpfs::audit::logger_index
{
    constexpr int FILE_PERMS = 0644;
    constexpr int INDEX_UPDATE_QUERY_LEN = 13;
    constexpr int INDEX_UPDATE_QUERY_FULLSTOP_LEN = 14;
    constexpr const char *INDEX_UPDATE_QUERY = "/::hpfs.index";
    constexpr const char *INDEX_UPDATE_QUERY_FULLSTOP = "/::hpfs.index.";

    constexpr uint64_t MAX_LOG_READ_SIZE = 4 * 1024 * 1024; // 4MB

    int fd = -1;              // The index file fd used throughout the session.
    off_t eof = 0;            // End of file (End offset of index file).
    bool initialized = false; // Indicates that the index has been initialized properly.
    std::string index_file_path;
    std::optional<audit::audit_logger> logger;
    std::optional<vfs::virtual_filesystem> virt_fs;
    std::optional<hmap::tree::hmap_tree> htree;

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
        if (audit::audit_logger::create(logger, audit::LOG_MODE::LOG_SYNC, ctx.log_file_path) == -1 ||
            vfs::virtual_filesystem::create(virt_fs, false, ctx.seed_dir, logger.value()) == -1 ||
            hmap::tree::hmap_tree::create(htree, virt_fs.value()) == -1)
        {
            LOG_ERROR << errno << ": Error initializing log index.";
            logger.reset();
            return -1;
        }

        // Open or create the index file.
        if (!util::is_file_exists(file_path))
        {
            // Create new file and include version header.
            fd = open(file_path.data(), O_CREAT | O_RDWR, FILE_PERMS);
            if (fd != -1)
            {
                if (write(fd, version::HP_VERSION_BYTES, version::VERSION_BYTES_LEN) < version::VERSION_BYTES_LEN)
                {
                    LOG_ERROR << errno << ": Error adding version header to the hpfs index file";
                    close(fd);
                    logger.reset();
                    return -1;
                }
            }
        }
        else
        {
            // Open an already existing file.
            fd = open(file_path.data(), O_RDWR);
        }
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
        if (eof == version::VERSION_BYTES_LEN)
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
        return (eof - version::VERSION_BYTES_LEN) / (sizeof(uint64_t) + sizeof(hmap::hasher::h32));
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
        if (!initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
            return -1;
        }
        else if (eof == version::VERSION_BYTES_LEN)
        {
            LOG_ERROR << errno << ": Index hasn't been updated.";
            return -1;
        }

        // Calculate offset position of the index.
        const uint64_t index_offset = get_data_offset_of_index_file(seq_no);
        // If the offset is end of file set offset 0.
        if (index_offset == eof)
        {
            offset = 0;
            return 0;
        }
        else if (index_offset > eof)
            return -1;

        // Reading the offset value as big endian by calculating the offset position from the sequence number.
        uint8_t be_offset[8];
        if (pread(fd, &be_offset, sizeof(be_offset), index_offset) < sizeof(be_offset))
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
        if (!initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
            return -1;
        }
        else if (eof == version::VERSION_BYTES_LEN)
        {
            LOG_ERROR << errno << ": Index hasn't been updated.";
            return -1;
        }

        // Calculate offset position of the index.
        const uint64_t index_offset = get_data_offset_of_index_file(seq_no);
        if (index_offset >= eof)
            return -1;

        // Reading the hash value.
        if (pread(fd, &hash, sizeof(hmap::hasher::h32), index_offset + sizeof(hmap::hasher::h32)) < sizeof(hmap::hasher::h32))
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
     * @return Returns buffer length on success, -1 on error.
    */
    int read_log_records(char *buf, const uint64_t min_seq_no, const uint64_t max_seq_no, const uint64_t max_size)
    {
        // If logger isn't initialized show error.
        if (!initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
            return -1;
        }
        else if (eof == version::VERSION_BYTES_LEN)
        {
            LOG_ERROR << errno << ": Index hasn't been updated.";
            return -1;
        }
        else if (max_seq_no > 0 && min_seq_no > max_seq_no)
        {
            LOG_ERROR << errno << ": min_seq_no cannot be greated than max_seq_no.";
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
        size_t buf_length = 0;

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
                if (read_offset(next_seq_no_offset, ++seq_no) == -1)
                    return -1;
            }
            else
                memset(record_buf.data() + record_buf.length() - sizeof(seq_no), 0, sizeof(seq_no));

            // Read the log record buf at current offset.
            // After reading current_offset would be next records offset.
            std::string log_record;
            if (logger->init_log_header() == -1 || logger->read_log_record_buf_at(current_offset, current_offset, log_record) == -1)
                return -1;
            record_buf.append(log_record);
            log_record.clear();

            // If reached the max_size limit break from the loop before adding the temp buffer to main and return the collected buffer.
            if (max_size != 0 && (buf_length + record_buf.length()) > max_size)
                break;

            // If the current log record is a seq_no log record, Append collected to the main buffer and clean the temp buffer.
            // Buffer layout - [00000000][log record][00000000][log record]....[seq_no1][log record][00000000][log record]...
            if (is_seq_no_log)
            {
                memcpy(buf + buf_length, record_buf.c_str(), record_buf.length());
                buf_length += record_buf.length();
                record_buf.resize(0);
            }

            // If we reached to the eof of the log file, break from the loop and return collected.
            if (current_offset == 0)
            {
                // If we reach this point, it means requested max is beyond our log (Ex: max_seq_no = 0).
                // So we append collected all the records up until to the end of our log.
                memcpy(buf + buf_length, record_buf.c_str(), record_buf.length());
                buf_length += record_buf.length();
                record_buf.clear();
                break;
            }
        }

        return buf_length;
    }

    // This is the test function to decode the hpfs log read buffer.
    /**
     * Appending log records to hpfs log file.
     * @param buf Buffer to append.
     * @return Returns 0 on success, -1 on error.
    */
    int append_log_records(const char *buf, const size_t size)
    {
        // If logger isn't initialized show error.
        if (!initialized)
        {
            LOG_ERROR << errno << ": Index hasn't been initialized properly.";
            return -1;
        }

        if (htree->init() == -1)
        {
            LOG_ERROR << errno << ": Error initializing htree.";
        }

        uint64_t last_seq_no = get_last_seq_no();
        hmap::hasher::h32 last_root_hash;
        if (read_last_root_hash(last_root_hash) == -1)
            return -1;

        off_t offset = 0;
        bool first_record = true;
        while (offset < size)
        {
            const uint64_t seq_no = *(const uint64_t *)std::string_view(buf + offset, 8).data();
            offset += 8;

            const log_record_header rh = *(const log_record_header *)std::string_view(buf + offset, sizeof(log_record_header)).data();
            offset += sizeof(log_record_header);

            const std::string vpath = std::string(buf + offset, rh.vpath_len).data();
            offset += rh.vpath_len;
            const std::vector<uint8_t> payload(buf + offset, buf + offset + rh.payload_len);
            offset += rh.payload_len;
            const std::vector<uint8_t> block_data(buf + offset, buf + offset + rh.block_data_len);
            offset += rh.block_data_len;

            // If index file is empty don't check for joining point. Append all records.
            if (eof != version::VERSION_BYTES_LEN && first_record)
            {
                if (seq_no != last_seq_no || rh.root_hash != last_root_hash)
                {
                    LOG_ERROR << "Invalid joining point in the received log response. seq no " << seq_no << " " << last_seq_no;
                    return -1;
                }

                first_record = false;
                continue;
            }
            first_record = false;

            if (persist_log_record(seq_no, rh.operation, vpath, payload, block_data) == -1)
            {
                LOG_ERROR << "Error persisting log record. seq no " << seq_no;
                return -1;
            }
        }

        if (htree->store.persist_hash_maps() == -1)
        {
            LOG_ERROR << errno << ": Error persisting htree.";
        }

        return 0;
    }

    int persist_log_record(const uint64_t seq_no, const audit::FS_OPERATION op, const std::string &vpath, const std::vector<uint8_t> &payload, const std::vector<uint8_t> &block_data)
    {
        const iovec payload_vec{(void *)payload.data(), payload.size()};
        std::vector<iovec> block_data_vec;
        block_data_vec.push_back({(void *)block_data.data(), block_data.size()});

        log_record_header rh;
        off_t start_offset = logger->append_log(rh, vpath, op, &payload_vec,
                                                block_data_vec.data(), block_data_vec.size());

        if (start_offset == 0)
        {
            LOG_ERROR << "Error appending to the logs file.";
            return -1;
        }

        if (virt_fs->build_vfs() == -1)
        {
            LOG_ERROR << "Error building the virtual file system.";
            return -1;
        }

        switch (op)
        {
        case (audit::FS_OPERATION::MKDIR):
        case (audit::FS_OPERATION::CREATE):
        {
            if (htree && htree->apply_vnode_create(vpath) == -1)
            {
                LOG_ERROR << "Create hash: Error generating hash for " << vpath << " " << errno;
                return -1;
            }
            break;
        }
        case (audit::FS_OPERATION::WRITE):
        {
            vfs::vnode *vn = NULL;
            if (virt_fs->get_vnode(vpath, &vn) == -1)
            {
                LOG_ERROR << "Write: Error fetching the vnode " << vpath;
                return -1;
            }
            if (!vn)
            {
                LOG_ERROR << "Write: No entry " << vpath;
                return -ENOENT;
            }
            op_write_payload_header wh = *(const op_write_payload_header *)payload.data();
            if (htree && htree->apply_vnode_data_update(vpath, *vn, wh.offset, wh.size) == -1)
            {
                LOG_ERROR << "Update hash: Error generating hash for " << vpath;
                return -1;
            }
            break;
        }
        case (audit::FS_OPERATION::TRUNCATE):
        {
            vfs::vnode *vn = NULL;
            if (virt_fs->get_vnode(vpath, &vn) == -1)
            {
                LOG_ERROR << "Truncate: Error fetching the vnode " << vpath;
                return -1;
            }
            if (!vn)
            {
                LOG_ERROR << "Truncate: No entry " << vpath;
                return -ENOENT;
            }
            size_t current_size = vn->st.st_size;
            hpfs::audit::op_truncate_payload_header th = *(const op_truncate_payload_header *)payload.data();
            if (htree && htree->apply_vnode_data_update(vpath, *vn, MIN(th.size, current_size), MAX(0, th.size - current_size)) == -1)
            {
                LOG_ERROR << "Update hash: Error generating hash for " << vpath;
                return -1;
            }
            break;
        }
        case (audit::FS_OPERATION::CHMOD):
        {
            vfs::vnode *vn = NULL;
            if (virt_fs->get_vnode(vpath, &vn) == -1)
            {
                LOG_ERROR << "Chmod: Error fetching the vnode " << vpath;
                return -1;
            }
            if (!vn)
            {
                LOG_ERROR << "Chmod: No entry " << vpath;
                return -ENOENT;
            }
            if (htree && htree->apply_vnode_metadata_update(vpath, *vn) == -1)
            {
                LOG_ERROR << "Update hash: Error generating hash for " << vpath;
                return -1;
            }
            break;
        }
        case (audit::FS_OPERATION::RMDIR):
        case (audit::FS_OPERATION::UNLINK):
        {
            if (htree && htree->apply_vnode_delete(vpath) == -1)
            {
                LOG_ERROR << "Delete hash: Error generating hash for " << vpath;
                return -1;
            }
            break;
        }
        case (audit::FS_OPERATION::RENAME):
        {
            vfs::vnode *vn = NULL;
            if (virt_fs->get_vnode(vpath, &vn) == -1)
            {
                LOG_ERROR << "Rename: Error fetching the vnode " << vpath;
                return -1;
            }
            if (!vn)
            {
                LOG_ERROR << "Rename: No entry " << vpath;
                return -ENOENT;
            }

            const bool is_file = S_ISREG(vn->st.st_mode);

            std::string to_vpath(std::string_view((char *)payload.data(), payload.size() - 1));
            if (htree && htree->apply_vnode_rename(vpath, to_vpath, !is_file) == -1)
            {
                LOG_ERROR << "Rename hash: Error generating hash for " << vpath;
                return -1;
            }
            break;
        }
        default:
            return -1;
        }

        if (htree && logger->update_log_record(start_offset, htree->get_root_hash(), rh) == -1)
            return -1;

        if (seq_no != 0)
        {
            uint8_t offset[8];
            util::uint64_to_bytes(offset, start_offset);

            struct iovec iov_vec[2];
            iov_vec[0].iov_base = offset;
            iov_vec[0].iov_len = sizeof(offset);

            iov_vec[1].iov_base = &(rh.root_hash);
            iov_vec[1].iov_len = sizeof(rh.root_hash);

            if (pwritev(fd, iov_vec, 2, eof) == -1)
            {
                LOG_ERROR << errno << ": Error writing to log index file.";
                return -1;
            }

            eof += sizeof(offset) + sizeof(rh.root_hash);
        }

        return 0;
    }

    /**
     * Checks request for any read.
     * @param query Query passed from the outside.
     * @param buf Data buffer to be returned.
     * @param size Read size.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_read(std::string_view query, char *buf, size_t *size)
    {
        if (strncmp(query.data(), INDEX_UPDATE_QUERY_FULLSTOP, INDEX_UPDATE_QUERY_FULLSTOP_LEN) == 0 && query.length() > INDEX_UPDATE_QUERY_FULLSTOP_LEN)
        {
            // If logger index isn't initialized return no entry.
            if (!initialized)
                return -ENOENT;

            // Split the query by '.'.
            const std::vector<std::string> params = util::split_string(query, ".");
            if (params.size() != 4)
            {
                LOG_ERROR << "Log read parameter error: Invalid parameters";
                return -1;
            }

            uint64_t min_seq_no, max_seq_no;
            if (util::stoull(params.at(2).data(), min_seq_no) == -1 || util::stoull(params.at(3).data(), max_seq_no) == -1)
            {
                LOG_ERROR << "Log read parameter error: Invalid parameters";
                return -1;
            }

            // We send the requested size limit to collect the logs.
            const int res = read_log_records(buf, min_seq_no, max_seq_no, *size);
            if (res == -1)
                return -1;

            // Then we set the size to the actual buffer size to return back.
            *size = res;
            return 0;
        }

        return 1;
    }

    /**
     * Checks open request for any write.
     * @param query Query passed from the outside.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_write(std::string_view query, const char *buf, const size_t size)
    {
        // If logger index isn't initialized return no entry.
        if (query == INDEX_UPDATE_QUERY && query.length() == INDEX_UPDATE_QUERY_LEN)
        {
            if (!initialized)
                return -ENOENT;

            if (size <= 1)
                return update_log_index();
            else
                return append_log_records(buf, size);
        }

        return 1;
    }

    /**
     * Checks open request for any index.
     * @param query Query passed from the outside.
     * @return 0 if request succesfully was interpreted by index control. 1 if the request
     *         should be passed through to the virtual fs. <0 on error.
    */
    int index_check_open(std::string_view query)
    {
        // If logger index isn't initialized return no entry.
        if (strncmp(query.data(), INDEX_UPDATE_QUERY, INDEX_UPDATE_QUERY_LEN) == 0)
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
        // Read or truncate calls will contains paramters.
        else if (strncmp(query.data(), INDEX_UPDATE_QUERY_FULLSTOP, INDEX_UPDATE_QUERY_FULLSTOP_LEN) == 0)
        {
            // If logger index isn't initialized return no entry.
            if (!initialized)
                return -ENOENT;
            // Given path should contain a sequnce number.
            if (query.size() > INDEX_UPDATE_QUERY_FULLSTOP_LEN)
            {
                if (fstat(fd, stbuf) == -1)
                {
                    LOG_ERROR << errno << ": Error in stat of index file.";
                    return -1;
                }
                // Send maximum read size as dummy file size.
                stbuf->st_size = MAX_LOG_READ_SIZE;
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
        if (strncmp(path_str.c_str(), INDEX_UPDATE_QUERY_FULLSTOP, INDEX_UPDATE_QUERY_FULLSTOP_LEN) == 0)
        {
            // Given path should create a sequnce number.
            if (path_str.size() > 14)
            {
                uint64_t seq_no;
                if (util::stoull(path_str.substr(INDEX_UPDATE_QUERY_FULLSTOP_LEN), seq_no) == -1 ||
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
        return version::VERSION_BYTES_LEN + (seq_no - 1) * (sizeof(uint64_t) + sizeof(hmap::hasher::h32));
    }

} // namespace hpfs::audit::logger_index