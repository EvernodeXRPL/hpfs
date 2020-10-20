#ifndef _HPFS_HMAP_QUERY_
#define _HPFS_HMAP_QUERY_

#include <string>
#include "hasher.hpp"
#include "tree.hpp"
#include "store.hpp"
#include "../vfs/virtual_filesystem.hpp"

namespace hpfs::hmap::query
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

    struct child_hash_node
    {
        bool is_file;
        char name[256];
        hasher::h32 node_hash;
    };

    class hmap_query
    {
        private:
            tree::hmap_tree &tree;
            vfs::virtual_filesystem &virt_fs;

        public:
        hmap_query(tree::hmap_tree &tree, vfs::virtual_filesystem &virt_fs);
        request parse_request_path(const char *request_path);
        int getattr(const request &req, struct stat *stbuf);
        int read(const request &req, char *buf, const size_t size);
        int read_file_block_hashes(const store::vnode_hmap &node_hmap, char *buf, const size_t size);
        int read_dir_children_hashes(const std::string &vpath, char *buf, const size_t size);
    };

} // namespace hpfs::hmap::query

#endif