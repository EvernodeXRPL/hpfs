#ifndef _HPFS_MMAPPER_
#define _HPFS_MMAPPER_
#include <sys/types.h>

namespace mmapper
{

struct fmap
{
    void *ptr;
    size_t size;
};

// Write() data block size. We choose this to be the page size for mmap() page alignment.
constexpr size_t BLOCK_SIZE = 4096;

int create(fmap &map, const int src_fd, const size_t src_size, const off_t src_offset);
int place(fmap &map, const off_t palce_at, const int src_fd, const size_t src_size, const off_t src_offset);
int unmap(fmap &map);
size_t get_blocked_size(const size_t data_size);
off_t get_start_block_offset(const off_t data_offset);
off_t get_end_block_offset(const off_t data_offset);

} // namespace mmapper

#endif