#include <sys/mman.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <iostream>
#include "mmapper.hpp"

namespace mmapper
{

int create(fmap &map, const int src_fd, const size_t src_size, const off_t src_offset)
{
    map.ptr = mmap(NULL, src_size, PROT_READ, MAP_PRIVATE, src_fd, src_offset);
    if (map.ptr)
    {
        map.size = src_size;
        return 0;
    }

    return -1;
}

int place(fmap &map, const off_t palce_at, const int src_fd, const size_t src_size, const off_t src_offset)
{
    if (!map.ptr)
        return -1;

    const size_t required_map_size = palce_at + src_size;
    if (required_map_size > map.size)
    {
        map.ptr = mremap(map.ptr, map.size, required_map_size, MREMAP_MAYMOVE);
        if (!map.ptr)
            return -1;

        map.size = required_map_size;
    }

    const void *ptr = mmap((uint8_t *)map.ptr + palce_at, src_size, PROT_READ, MAP_PRIVATE | MAP_FIXED, src_fd, src_offset);
    if (!ptr)
        return -1;

    return 0;
}

int unmap(fmap &map)
{
    if (munmap(map.ptr, map.size) == -1)
        return -1;
    map.ptr = NULL;
    map.size = 0;
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