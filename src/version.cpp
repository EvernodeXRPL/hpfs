#include <iostream>
#include <string.h>
#include "version.hpp"
#include "util.hpp"
namespace version
{
    // Binary representations of the version. (populated during version init)
    uint8_t HP_VERSION_BYTES[VERSION_BYTES_LEN];

    int init()
    {
        // Generate version bytes.
        if (set_version_bytes(HP_VERSION_BYTES, HPFS_VERSION) == -1)
            return -1;

        return 0;
    }

    /**
     * Create 8 byte binary version from version string. First 6 bytes contains the 3 version components and the 
     * next 2 bytes are reserved for future use.
     * @param bytes Byte buffer to be populated with binary version data.
     * @param version Version string.
     * @return Returns -1 on error and 0 on success.
    */
    int set_version_bytes(uint8_t *bytes, std::string_view version)
    {
        memset(bytes, 0, VERSION_BYTES_LEN);

        const std::string delimeter = ".";
        size_t start = 0;
        size_t end = version.find(delimeter);

        if (end == std::string::npos)
        {
            std::cerr << "Invalid version " << version << std::endl;
            return -1;
        }

        const uint16_t major = atoi(version.substr(start, end - start).data());

        start = end + delimeter.length();
        end = version.find(delimeter, start);

        if (end == std::string::npos)
        {
            std::cerr << "Invalid version " << version << std::endl;
            return -1;
        }

        const uint16_t minor = atoi(version.substr(start, end - start).data());
        start = end + delimeter.length();
        end = version.find(delimeter, start);

        const uint16_t patch = atoi(version.substr(start).data());

        util::uint16_to_bytes(&bytes[0], major);
        util::uint16_to_bytes(&bytes[2], minor);
        util::uint16_to_bytes(&bytes[4], patch);

        return 0;
    }
}