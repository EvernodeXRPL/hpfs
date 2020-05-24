#include "h32.hpp"

/**
 * Based on https://github.com/codetsunami/file-ptracer/blob/master/merkle.cpp
 */
namespace hmap
{

    /**
     * Helper functions for working with 32 byte hash type h32.
     */

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

} // namespace hmap