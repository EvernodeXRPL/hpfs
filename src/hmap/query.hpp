#ifndef _HPFS_HMAP_QUERY_
#define _HPFS_HMAP_QUERY_

#include <string>

namespace hmap::query
{
    enum MODE
    {
        UNDEFINED = 0,
        HASH = 1,
        CHILDREN = 2
    };

    struct request
    {
        MODE mode;
        std::string vpath;
    };

    request parse_request_path(const char *request_path);
    int getattr(const request &req, struct stat *stbuf);
    int read(const request &req, char *buf, const size_t size);

} // namespace hmap::query

#endif