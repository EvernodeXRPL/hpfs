#include <libgen.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <iostream>
#include "query.hpp"
#include "hasher.hpp"
#include "tree.hpp"
#include "../util.hpp"
#include "../vfs/vfs.hpp"
#include "../vfs/virtual_filesystem.hpp"
#include "../tracelog.hpp"

namespace hpfs::hmap::query
{
    constexpr const char *HASH_REQUEST_PATTERN = "::hpfs.hmap.hash";
    constexpr const size_t HASH_REQUEST_PATTERN_LEN = 16;
    constexpr const char *CHILDREN_REQUEST_PATTERN = "::hpfs.hmap.children";
    constexpr const size_t CHILDREN_REQUEST_PATTERN_LEN = 20;

    hmap_query::hmap_query(tree::hmap_tree &tree, vfs::virtual_filesystem &virt_fs) : tree(tree), virt_fs(virt_fs)
    {
    }

    /**
     * Match the last portion of the request path with request patterns.
     */
    request hmap_query::parse_request_path(const char *request_path) const
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
                 strcmp(children_match_start, CHILDREN_REQUEST_PATTERN) == 0)
        {
            req.mode = MODE::CHILDREN;
            req.vpath = std::string_view(request_path, len - CHILDREN_REQUEST_PATTERN_LEN);
        }

        return req; // Return the request struct with 'undefined' request type.
    }

    int hmap_query::getattr(const request &req, struct stat *stbuf) const
    {
        store::vnode_hmap *node_hmap;
        if (tree.get_vnode_hmap(&node_hmap, req.vpath) == -1)
        {
            LOG_ERROR << "Error in hmap query getattr tree get" << req.vpath;
            return -1;
        }
        if (!node_hmap)
            return -ENOENT;

        stbuf->st_mode = S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

        if (req.mode == MODE::HASH)
        {
            stbuf->st_size = sizeof(hasher::h32);
        }
        else // Children
        {
            // If it's a file, we take the file block hashes.
            // If it's a directory, we take the directory children node hashes.

            if (node_hmap->is_file)
            {
                stbuf->st_size = sizeof(hasher::h32) * node_hmap->block_hashes.size();
            }
            else // Is directory
            {
                // Get how many children the directory has.
                vfs::vdir_children_map dir_children;
                if (virt_fs.get_dir_children(req.vpath.c_str(), dir_children) == -1)
                {
                    LOG_ERROR << "Error in hmap query getattr vfs dir children get" << req.vpath;
                    return -1;
                }

                stbuf->st_size = sizeof(child_hash_node) * dir_children.size();
            }
        }

        return 0;
    }

    int hmap_query::read(const request &req, char *buf, const size_t size) const
    {
        store::vnode_hmap *node_hmap;
        if (tree.get_vnode_hmap(&node_hmap, req.vpath) == -1)
        {
            LOG_ERROR << "Error in hmap query read tree get" << req.vpath;
            return -1;
        }
        if (!node_hmap)
            return -ENOENT;

        if (req.mode == MODE::HASH) // Node hash
        {
            const size_t read_len = MIN(size, sizeof(hasher::h32));
            memcpy(buf, &node_hmap->node_hash, read_len);
            return read_len;
        }
        else // Children
        {
            // If it's a file, we take the file block hashes.
            // If it's a directory, we take the directory children node hashes.
            return (node_hmap->is_file)
                       ? read_file_block_hashes(*node_hmap, buf, size)
                       : read_dir_children_hashes(req.vpath, buf, size);
        }
    }

    int hmap_query::read_file_block_hashes(const store::vnode_hmap &node_hmap, char *buf, const size_t size) const
    {
        const size_t read_len = MIN(size, sizeof(hmap::hasher::h32) * node_hmap.block_hashes.size());
        memcpy(buf, node_hmap.block_hashes.data(), read_len);
        return read_len;
    }

    int hmap_query::read_dir_children_hashes(const std::string &vpath, char *buf, const size_t size) const
    {
        vfs::vdir_children_map dir_children;
        if (virt_fs.get_dir_children(vpath.c_str(), dir_children) == -1)
        {
            LOG_ERROR << "Error in hmap query dir children vfs dir children get" << vpath;
            return -1;
        }

        // This is the collection that will be written to the read buf.
        child_hash_node children_hashes[dir_children.size()];
        uint32_t idx = 0;

        for (const auto &[child_name, st] : dir_children)
        {
            std::string child_vpath = std::string(vpath);
            if (child_vpath.back() != '/')
                child_vpath.append("/");
            child_vpath.append(child_name);

            store::vnode_hmap *node_hmap;
            if (tree.get_vnode_hmap(&node_hmap, child_vpath) == -1 || !node_hmap)
            {
                LOG_ERROR << "Error in hmap query dir children tree get" << vpath;
                return -1;
            }

            children_hashes[idx].is_file = node_hmap->is_file;
            children_hashes[idx].node_hash = node_hmap->node_hash;
            strcpy(children_hashes[idx].name, child_name.c_str());
            idx++;
        }

        const size_t read_len = MIN(size, sizeof(child_hash_node) * dir_children.size());
        memcpy(buf, children_hashes, read_len);
        return read_len;
    }
} // namespace hpfs::hmap::query