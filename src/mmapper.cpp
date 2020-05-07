#include <sys/mman.h>
#include <stdint.h>
#include <math.h>
#include "mmapper.hpp"

namespace mmapper
{

int create(fmap &map, const int src_fd, const size_t src_size, const off_t src_offset)
{
    map.ptr = mmap(NULL, src_size, PROT_READ, MAP_PRIVATE, src_fd, src_offset);
    if (map.ptr)
    {
        map.data_size = (src_size - src_offset);
        map.map_size = get_blocked_size(map.data_size);
        return 0;
    }

    return -1;
}

int place(fmap &map, const off_t at_block_offset, const int src_fd, const size_t src_size, const off_t src_offset)
{
    if (!map.ptr)
        return -1;

    if ((at_block_offset + src_size) > map.data_size)
        map.data_size = at_block_offset + src_size;

    const size_t required_map_size = get_blocked_size(map.data_size);
    if (required_map_size > map.map_size)
    {
        map.ptr = mremap(map.ptr, map.map_size, required_map_size, MREMAP_MAYMOVE);
        if (!map.ptr)
            return -1;

        map.map_size = required_map_size;
    }

    void *ptr = mmap((uint8_t*)map.ptr + at_block_offset, src_size, PROT_READ, MAP_PRIVATE, src_fd, src_offset);
    if (!ptr)
        return -1;

    return 0;
}

int shrink(fmap &map, const size_t new_src_size)
{
    map.ptr = mremap(map.ptr, map.map_size, new_src_size, 0);
    if (!map.ptr)
        return -1;

    map.data_size = new_src_size;
    map.map_size = get_blocked_size(new_src_size);
    return 0;
}

int unmap(fmap &map)
{
    if (munmap(map.ptr, map.map_size) == -1)
        return -1;
    map.ptr = NULL;
    map.map_size = 0;
    map.data_size = 0;
    return 0;
}

size_t get_blocked_size(const size_t data_size)
{
    const double div = (double)data_size / (double)BLOCK_SIZE;
    return ((size_t)ceil(div)) * BLOCK_SIZE;
}

off_t get_start_block_offset(const off_t data_offset)
{
    // Integer division.
    return (data_offset / BLOCK_SIZE) * BLOCK_SIZE;
}

off_t get_end_block_offset(const off_t data_offset)
{
    const double div = (double)data_offset / (double)BLOCK_SIZE;
    return ((off_t)ceil(div)) * BLOCK_SIZE;
}

} // namespace mmapper