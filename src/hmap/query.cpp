#include <libgen.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <iostream>
#include "../util.hpp"
#include "query.hpp"
#include "hasher.hpp"
#include "hmap.hpp"

namespace hmap::query
{
    constexpr const char *HASH_REQUEST_PATTERN = "::hpfs.hmap.hash";
    constexpr const size_t HASH_REQUEST_PATTERN_LEN = 16;
    constexpr const char *CHILDREN_REQUEST_PATTERN = "::hpfs.hmap.children";
    constexpr const size_t CHILDREN_REQUEST_PATTERN_LEN = 21;

    /**
     * Match the last portion of the request path with request patterns.
     */
    request parse_request_path(const char *request_path)
    {
        request req{MODE::UNDEFINED};
        const size_t len = strlen(request_path);

        const char *hash_match_start = request_path + len - HASH_REQUEST_PATTERN_LEN;
        const char *children_match_start = request_path + len - CHILDREN_REQUEST_PATTERN_LEN;

        if (len >= HASH_REQUEST_PATTERN_LEN &&
            strcmp(hash_match_start, HASH_REQUEST_PATTERN) == 0)
        {
            req.mode = MODE::HASH;
            req.vpath = std::string_view(request_path, len - HASH_REQUEST_PATTERN_LEN);
        }
        else if (len >= CHILDREN_REQUEST_PATTERN_LEN &&
                 strcmp(hash_match_start, CHILDREN_REQUEST_PATTERN) == 0)
        {
            req.mode = MODE::CHILDREN;
            req.vpath = std::string_view(request_path, len - CHILDREN_REQUEST_PATTERN_LEN);
        }

        return req; // Retuen the request struct with 'undefined' request type.
    }

    int getattr(const request &req, struct stat *stbuf)
    {
        vnode_hmap *node_hmap;
        if (get_vnode_hmap(&node_hmap, req.vpath) == -1)
            return -1;
        if (!node_hmap)
            return -ENOENT;

        stbuf->st_mode = S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

        if (req.mode == MODE::HASH)
            stbuf->st_size = sizeof(hasher::h32);

        return 0;
    }

    int read(const request &req, char *buf, const size_t size)
    {
        vnode_hmap *node_hmap;
        std::cout << req.vpath << "\n";
        if (get_vnode_hmap(&node_hmap, req.vpath) == -1)
            return -1;
        if (!node_hmap)
            return -ENOENT;

        if (req.mode == MODE::HASH)
        {
            const size_t read_len = MIN(size, sizeof(hasher::h32));
            memcpy(buf, &node_hmap->node_hash, read_len);
            return read_len;
        }
        else
        {
            return 0;
        }
    }
} // namespace hmap::query