#include "hasher.hpp"
#include <blake3.h>
#include <string.h>
#include <iomanip>

/**
 * Based on https://github.com/codetsunami/file-ptracer/blob/master/merkle.cpp
 */
namespace hmap::hasher
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
        for (int i = 0; i < sizeof(h32); i++)
            output << std::hex << std::setfill('0') << std::setw(2) << (int)buf[i];

        return output;
    }

    int hash_buf(h32 &hash, const void *buf, const size_t len)
    {
        // Initialize the hasher.
		blake3_hasher hasher;
		blake3_hasher_init(&hasher);
		blake3_hasher_update(&hasher, buf, len);

		blake3_hasher_finalize(&hasher, reinterpret_cast<uint8_t *>(&hash), sizeof(h32));

        // [todo] - need to do proper error handling since blake3 hash functions are void
        return 0;
    }

    int hash_buf(h32 &hash, const void *buf1, const size_t len1, const void *buf2, const size_t len2)
    {

        // Initialize the hasher.
		blake3_hasher hasher;
		blake3_hasher_init(&hasher);
        // update the hash with two buffers
		blake3_hasher_update(&hasher, buf1, len1);
		blake3_hasher_update(&hasher, buf2, len2);
        // finalize the hash 
		blake3_hasher_finalize(&hasher, reinterpret_cast<uint8_t *>(&hash), sizeof(h32));
        
        // [todo] - need to do proper error handling since blake3 hash functions are void
        return 0;
    }

} // namespace hmap::hasher