#include "hasher.hpp"
#include "../util.hpp"
#include <blake3.h>
#include <string.h>
#include <iomanip>

/**
 * Based on https://github.com/codetsunami/file-ptracer/blob/master/merkle.cpp
 */
namespace hpfs::hmap::hasher
{
    /**
     * Helper functions for working with 32 byte hash type h32.
     */

    h32 h32_empty;

    bool h32::operator==(const h32 rhs) const
    {
        return this->data[0] == rhs.data[0] && this->data[1] == rhs.data[1] && this->data[2] == rhs.data[2] && this->data[3] == rhs.data[3];
    }

    bool h32::operator!=(const h32 rhs) const
    {
        return this->data[0] != rhs.data[0] || this->data[1] != rhs.data[1] || this->data[2] != rhs.data[2] || this->data[3] != rhs.data[3];
    }

    void h32::operator^=(const h32 rhs)
    {
        this->data[0] ^= rhs.data[0];
        this->data[1] ^= rhs.data[1];
        this->data[2] ^= rhs.data[2];
        this->data[3] ^= rhs.data[3];
    }

    std::string h32::to_hex() const
    {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

    std::ostream &operator<<(std::ostream &output, const h32 &h)
    {
        const uint8_t *buf = reinterpret_cast<const uint8_t *>(&h);
        for (int i = 0; i < 5; i++) // Only print first 5 bytes in hex.
            output << std::hex << std::setfill('0') << std::setw(2) << (int)buf[i];

        return output;
    }

    void hash_uint32(h32 &hash, const uint32_t val)
    {
        uint8_t buf[4];
        util::uint32_to_bytes(buf, val);
        hash_buf(hash, std::string_view((const char *)buf, 4));
    }

    void hash_buf(h32 &hash, std::string_view sv)
    {
        // Initialize the hasher.
        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, sv.data(), sv.size());

        blake3_hasher_finalize(&hasher, reinterpret_cast<uint8_t *>(&hash), sizeof(h32));
    }

    void hash_buf(h32 &hash, const void *buf1, const size_t len1, const void *buf2, const size_t len2)
    {

        // Initialize the hasher.
        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        // update the hash with two buffers
        blake3_hasher_update(&hasher, buf1, len1);
        blake3_hasher_update(&hasher, buf2, len2);
        // finalize the hash
        blake3_hasher_finalize(&hasher, reinterpret_cast<uint8_t *>(&hash), sizeof(h32));
    }

} // namespace hpfs::hmap::hasher