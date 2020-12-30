#include <string>
#include <string_view>
#include "../util.hpp"
#include "seed_path_tracker.hpp"

namespace hpfs::vfs
{
    seed_path_tracker::seed_path_tracker(std::string_view seed_dir) : seed_dir(seed_dir)
    {
    }

    bool seed_path_tracker::is_ancestor(const std::string &full_path, const std::string &sub_path)
    {
        return full_path.rfind(sub_path, 0) == 0 && (full_path.size() == sub_path.size() || full_path.at(sub_path.size()) == '/');
    }

    const std::string seed_path_tracker::resolve(const std::string &vpath_to_resolve)
    {
        if (renamed_seed_paths.empty())
            return vpath_to_resolve;

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

        if (longest_match_len > 0)
            return (longest_match_seed_path + vpath_to_resolve.substr(longest_match_len));
        else
            return vpath_to_resolve;
    }
    
    bool seed_path_tracker::is_renamed(const std::string &seed_path)
    {
        for (auto [vpath, seed_path] : renamed_seed_paths)
        {
            if (seed_path == seed_path)
                return true;
        }
        return false;
    }

    bool seed_path_tracker::is_removed(const std::string &seed_path)
    {
        return deleted_seed_paths.count(seed_path) == 1;
    }

    void seed_path_tracker::rename(const std::string &from, const std::string &to, const bool is_dir)
    {
        const std::string resolved = resolve(from);
        if (resolved.empty())
            return;

        const std::string full_seed_path = std::string(seed_dir) + resolved;
        if ((is_dir && !util::is_dir_exists(full_seed_path)) || !is_dir && !util::is_file_exists(full_seed_path))
            return;

        {
            std::unordered_map<std::string, std::string> renames_to_update;
            for (auto [vpath, seed_path] : renamed_seed_paths)
            {
                if (is_ancestor(vpath, from))
                    renames_to_update[vpath] = seed_path;
            }
            for (auto [vpath, seed_path] : renames_to_update)
            {
                renamed_seed_paths.erase(vpath);
                const std::string new_vpath = to + vpath.substr(from.size());
                if (new_vpath != seed_path)
                    renamed_seed_paths[new_vpath] = seed_path;
            }
        }

        if (to != resolved)
            renamed_seed_paths[to] = resolved;

        return;
    }

    void seed_path_tracker::remove(const std::string &vpath, const bool is_dir)
    {
        const std::string resolved = resolve(vpath);
        if (resolved.empty())
            return;

        const std::string full_seed_path = std::string(seed_dir) + resolved;
        if ((is_dir && !util::is_dir_exists(full_seed_path)) || !is_dir && !util::is_file_exists(full_seed_path))
            return;

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