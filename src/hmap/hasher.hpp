#ifndef _HPFS_HMAP_HASHER_
#define _HPFS_HMAP_HASHER_

#include <iostream>
#include <sstream>

namespace hpfs::hmap::hasher
{

    // blake2b hash is 32 bytes which we store as 4 quad words
    // Originally from https://github.com/codetsunami/file-ptracer/blob/master/merkle.cpp
    struct h32
    {
        uint64_t data[4];

        bool operator==(const h32 rhs) const;
        bool operator!=(const h32 rhs) const;
        void operator^=(const h32 rhs);
        std::string to_hex() const;
    };
    extern h32 h32_empty;

    std::ostream &operator<<(std::ostream &output, const h32 &h);
    void hash_buf(h32 &hash, std::string_view sv);
    void hash_buf(h32 &hash, const void *buf1, const size_t len1, const void *buf2, const size_t len2);

} // namespace hpfs::hmap::hasher

#endif