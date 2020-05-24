#ifndef _HPFS_HMAP_H32_
#define _HPFS_HMAP_H32_

#include <iostream>
#include <sstream>

namespace hmap
{

    // blake2b hash is 32 bytes which we store as 4 quad words
    // Originally from https://github.com/codetsunami/file-ptracer/blob/master/merkle.cpp
    struct h32
    {
        uint64_t data[4];

        bool operator==(const h32 rhs) const;
        bool operator!=(const h32 rhs) const;
        void operator^=(const h32 rhs);
    };

    std::ostream &operator<<(std::ostream &output, const h32 &h);
    std::stringstream &operator<<(std::stringstream &output, const h32 &h);

} // namespace hmap

#endif