#ifndef _HPFS_VERSION_
#define _HPFS_VERSION_

#include <string_view>

namespace version
{
    // HPFS version. Written to new files.
    constexpr const char *HPFS_VERSION = "1.0.0";

    // Version header size in bytes when serialized in binary format. (applies to hpfs version)
    // 2 bytes each for 3 version components. 2 bytes reserved.
    constexpr const size_t VERSION_BYTES_LEN = 8;

    // Binary representations of the versions. (populated during version init)
    extern uint8_t HP_VERSION_BYTES[VERSION_BYTES_LEN];

    int init();

    int set_version_bytes(uint8_t *bytes, std::string_view version);

}

#endif