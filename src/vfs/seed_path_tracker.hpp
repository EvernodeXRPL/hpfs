#ifndef _HPFS_VFS_SEED_PATH_TRACKER_
#define _HPFS_VFS_SEED_PATH_TRACKER_

#include <unordered_map>
#include <unordered_set>

namespace hpfs::vfs
{
    class seed_path_tracker
    {
    private:
        std::string_view seed_dir;

        // Renamed seed paths (key: renamed vpath, value: original seed path)
        std::unordered_map<std::string, std::string> renamed_seed_paths;

        // Seed paths that has been vritually deleted and should not be accessible.
        std::unordered_set<std::string> deleted_seed_paths;
        bool is_ancestor(const std::string &full_path, const std::string &sub_path);

    public:
        seed_path_tracker(std::string_view seed_dir);
        const std::string resolve(const std::string &vpath_to_resolve);
        bool is_renamed(const std::string &seed_path);
        bool is_removed(const std::string &seed_path);
        void rename(const std::string &from, const std::string &to, const bool is_dir);
        void remove(const std::string &vpath, const bool is_dir);
    };

} // namespace hpfs::vfs

#endif