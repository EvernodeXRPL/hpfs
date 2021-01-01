#include <string>
#include <string_view>
#include "../util.hpp"
#include "seed_path_tracker.hpp"

/**
 * Tracks the seed paths that have been renamed or deleted to support the vfs.
 * We need this so we can keep track of which seed dir/file is backing the virtual dir/file when they have been subjected
 * to rename or delete operations.
 * 
 * Example scenarios:
 * 
 * 1. If there was a seed dir named 'mydir' then the vfs virtual representation of 'mydir' simply 'points to' to the
 *    actual seed dir named 'mydir'. If the virtual dir was later renamed to 'mydir2' we need to remember that 'mydir2'
 *    actually points to the underlying seed dir 'mydir'.
 * 
 * 2. If we later 'delete' the above virtual dir 'mydir2' we now need to track that the underlying seed dir 'mydir' should
 *    be considered as deleted going forward (the seed dir still exists on disk until hpfs merge takes place).
 * 
 * 3. If we now create a new virtual dir named 'mydir' we need to know that the new dir is NOT backed by the original seed
 *    dir because the seed dir is considered deleted.
 * 
 * 4. The above scenarios are valid for regular files as well.
 * 
 * 5. We also attempt to handle complex rename scenarios such as a parent dir getting renamed while some children under it have
 *    also been renamed.
 */
namespace hpfs::vfs
{
    seed_path_tracker::seed_path_tracker(std::string_view seed_dir) : seed_dir(seed_dir)
    {
    }

    seed_path_tracker::seed_path_tracker(seed_path_tracker &&old) : seed_dir(old.seed_dir),
                                                                    renamed_seed_paths(std::move(old.renamed_seed_paths)),
                                                                    deleted_seed_paths(std::move(old.deleted_seed_paths))
    {
        old.moved = true;
    }

    /**
     * Returns whether the specified subpath is an ancestor of the specified full path.
     */
    bool seed_path_tracker::is_ancestor(const std::string &full_path, const std::string &sub_path)
    {
        // Either the sub path has to be a "folder" prefix of the full path or both of them has to be fully identical.
        return full_path.rfind(sub_path, 0) == 0 && (full_path.size() == sub_path.size() || full_path.at(sub_path.size()) == '/');
    }

    /**
     * Returns the actual seed path for the given vpath considering any rename operations that has taken place.
     * @returns The original seed path if renames have taken place. Returns vpath as is if no renames were found.
     */
    const std::string seed_path_tracker::resolve(const std::string &vpath_to_resolve)
    {
        if (renamed_seed_paths.empty())
            return vpath_to_resolve;

        // Consider rename mappings for this vpath or for any if its parent directories.
        // We attempt to find the longest rename match for the given vpath. (closest ancestor match)
        size_t longest_match_len = 0;
        std::string longest_match_seed_path;
        for (auto [vpath, seed_path] : renamed_seed_paths)
        {
            if (is_ancestor(vpath_to_resolve, vpath) && (vpath.size() > longest_match_len))
            {
                longest_match_len = vpath.size();
                longest_match_seed_path = seed_path;
            }
        }

        // If a match is found, replace the renamed portion of the vpath.
        // Otherwise return provided vpath as is.
        if (longest_match_len > 0)
            return (longest_match_seed_path + vpath_to_resolve.substr(longest_match_len));
        else
            return vpath_to_resolve;
    }

    /**
     * @return Whether the provided seed path has been renamed.
     */
    bool seed_path_tracker::is_renamed(const std::string &check_path)
    {
        for (auto [vpath, seed_path] : renamed_seed_paths)
        {
            if (seed_path == check_path)
                return true;
        }
        return false;
    }

    /**
     * @return Whether the provided seed path is considered deleted.
     */
    bool seed_path_tracker::is_removed(const std::string &check_path)
    {
        return deleted_seed_paths.count(check_path) == 1;
    }

    /**
     * Creates a rename mapping from the given path to the new path.
     * If we are renaming a path that has been already renamed, this applies the new rename progressively to
     * not to lose track of the original backing seed path.
     */
    void seed_path_tracker::rename(const std::string &from, const std::string &to, const bool is_dir)
    {
        // Get the proper resolved seed path for the rename source. (we do this in case this path has been renamed already)
        const std::string resolved = resolve(from);
        const std::string full_seed_path = std::string(seed_dir) + resolved;

        // Don't need to do anything if there is no such real seed path.
        if ((is_dir && !util::is_dir_exists(full_seed_path)) || (!is_dir && !util::is_file_exists(full_seed_path)))
            return;

        // Find out any children seed paths previously renamed that are effected by this rename and update them.
        // (eg. when parent dir renames, it effects children seed paths too)
        {
            // Find any children renames that are effected.
            std::unordered_map<std::string, std::string> renames_to_update;
            for (auto [vpath, seed_path] : renamed_seed_paths)
            {
                if (is_ancestor(vpath, from))
                    renames_to_update[vpath] = seed_path;
            }

            // Update effected children with the new parent vpath.
            for (auto [vpath, seed_path] : renames_to_update)
            {
                renamed_seed_paths.erase(vpath);
                const std::string new_vpath = to + vpath.substr(from.size());
                if (new_vpath != seed_path)
                    renamed_seed_paths[new_vpath] = seed_path;
            }
        }

        // Add the rename mapping (if needed).
        if (to != resolved)
            renamed_seed_paths[to] = resolved;

        return;
    }

    /**
     * Marks a seed path as being deleted.
     */
    void seed_path_tracker::remove(const std::string &vpath, const bool is_dir)
    {
        const std::string resolved = resolve(vpath);
        if (resolved.empty())
            return;

        // Don't need to do anything if there is no such real seed path.
        const std::string full_seed_path = std::string(seed_dir) + resolved;
        if ((is_dir && !util::is_dir_exists(full_seed_path)) || !is_dir && !util::is_file_exists(full_seed_path))
            return;

        // Add to deleted seed paths.
        deleted_seed_paths.emplace(resolved);

        // Undo seed dir path rename (if needed).
        if (is_dir)
        {
            for (auto itr = renamed_seed_paths.begin(); itr != renamed_seed_paths.end();)
            {
                if (is_ancestor(itr->first, vpath))
                    itr = renamed_seed_paths.erase(itr);
                else
                    itr++;
            }
        }
    }

} // namespace hpfs::vfs