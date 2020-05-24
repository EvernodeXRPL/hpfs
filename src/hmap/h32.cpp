#include "h32.hpp"
#include <blake2.h>
#include <string.h>

/**
 * Based on https://github.com/codetsunami/file-ptracer/blob/master/merkle.cpp
 */
namespace hmap
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

    std::ostream &operator<<(std::ostream &output, const h32 &h)
    {
        output << h.data[0] << h.data[1] << h.data[2] << h.data[3];
        return output;
    }

    std::stringstream &operator<<(std::stringstream &output, const h32 &h)
    {
        output << std::hex << h;
        return output;
    }

    int hash_buf(h32 &hash, const void *buf, const size_t len)
    {
        return blake2b(reinterpret_cast<uint8_t *>(&hash), buf, NULL, sizeof(h32), len, 0);
    }

    int hash_buf(h32 &hash, const void *buf1, const size_t len1, const void *buf2, const size_t len2)
    {
        blake2b_state b2state;
        if (blake2b_init(&b2state, sizeof(h32)) == -1 ||
            blake2b_update(&b2state, reinterpret_cast<const uint8_t *>(buf1), len1) == -1 ||
            blake2b_update(&b2state, reinterpret_cast<const uint8_t *>(buf2), len2) == -1 ||
            blake2b_final(&b2state, reinterpret_cast<uint8_t *>(&hash), sizeof(h32)) == -1)
            return -1;

        return 0;
    }

} // namespace hmap