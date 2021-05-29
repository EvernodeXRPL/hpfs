#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <string>
#include <chrono>

// Compile and run benchmark
// g++ -std=c++17 -O3 -Wno-unused-result benchmark.cpp -o benchmark && ./benchmark

constexpr uint64_t MAX_FILE_SIZE = 64 * 1024 * 1024;
constexpr mode_t DIR_PERMS = 0754;
const std::string test_dir = "temp";
const std::string hpfs_binary = "../build/hpfs";

bool op_hpfs = false;
bool op_hmap_enabled = false;
std::string op_dir;
std::string op_title;
uint64_t op_start = 0;
pid_t hpfs_pid = 0;

int64_t get_epoch_milliseconds()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void start_hpfs(const char *mode, const bool hmap_enabled)
{
    const std::string mnt_dir = test_dir + "/mnt";
    const pid_t pid = fork();
    if (pid == 0)
    {
        // Child (hpfs)
        char *argv[] = {(char *)hpfs_binary.c_str(),
                        (char *)"fs",
                        (char *)test_dir.c_str(),
                        (char *)mnt_dir.c_str(),
                        (char *)"merge=true",
                        (char *)"trace=none",
                        NULL};
        execv(hpfs_binary.c_str(), argv);
    }
    else
    {
        // Wait for some time for hpfs to start up.
        usleep(500000); //500ms

        if (kill(pid, 0) != -1)
        {
            hpfs_pid = pid;
            const std::string session_file = mnt_dir + (hmap_enabled ? "/::hpfs.rw.hmap" : "/::hpfs.rw");
            mknod(session_file.data(), 0, 0);
        }
    }
}

void stop_hpfs()
{
    if (hpfs_pid > 0)
    {
        kill(hpfs_pid, SIGINT);
        int status;
        waitpid(hpfs_pid, &status, 0);
        hpfs_pid = 0;
    }
}

void init_test_dir()
{
    mkdir(test_dir.c_str(), DIR_PERMS);
}

void set_title(const std::string &title)
{
    op_title = title;
    std::cout << "\n"
              << op_title << "\n";
}

void start_op(const bool hpfs, const bool hmap_enabled)
{
    op_hpfs = hpfs;
    op_hmap_enabled = hmap_enabled;
    op_dir = (op_hpfs ? (test_dir + "/mnt/rw") : test_dir);

    init_test_dir();

    if (hpfs)
        start_hpfs("rw", hmap_enabled);

    op_start = get_epoch_milliseconds();
}

void finish_op()
{
    const uint64_t op_end = get_epoch_milliseconds();
    const uint64_t duration = op_end - op_start;

    std::cout << (op_hpfs ? (op_hmap_enabled ? "hpfs(hmap)" : "hpfs") : "raw")
              << ": " << duration << "ms\n";

    if (hpfs_pid > 0)
        stop_hpfs();

    // Remove dir.
    std::string rm_command = "rm -r " + test_dir;
    system(rm_command.c_str());
}

void benchmark_writes()
{
    const std::string path = op_dir + "/file";
    const int fd = open(path.c_str(), O_RDWR | O_CREAT, 0655);

    for (int i = 0; i < 5000; i++)
    {
        off_t off = rand() % MAX_FILE_SIZE;
        pwrite(fd, "Hello", 5, off);
    }
    close(fd);
}

void benchmark_reads()
{
    const std::string path = op_dir + "/file";
    const int fd = open(path.c_str(), O_RDWR | O_CREAT, 0655);

    for (int i = 0; i < 5000; i++)
    {
        off_t off = rand() % MAX_FILE_SIZE;
        char buf[5];
        pread(fd, buf, 5, off);
    }
    close(fd);
}

void benchmark_file_creations()
{
    const size_t fsize = 4096;
    for (int i = 0; i < 5000; i++)
    {
        const std::string path = op_dir + "/file" + std::to_string(i);
        const int fd = open(path.c_str(), O_RDWR | O_CREAT, 0655);
        ftruncate(fd, fsize);
        close(fd);
    }
}

int main(int argc, char **argv)
{
    srand(1);

    set_title("Random writes");
    start_op(true, false);
    benchmark_writes();
    finish_op();
    start_op(true, true);
    benchmark_writes();
    finish_op();
    start_op(false, false);
    benchmark_writes();
    finish_op();

    set_title("Random reads");

    start_op(true, false);
    benchmark_writes();
    stop_hpfs();
    start_op(true, false);
    benchmark_reads();
    finish_op();

    start_op(true, true);
    benchmark_writes();
    stop_hpfs();
    start_op(true, true);
    benchmark_reads();
    finish_op();

    start_op(false, false);
    benchmark_writes();
    start_op(false, false);
    benchmark_reads();
    finish_op();

    set_title("File creations");
    start_op(true, false);
    benchmark_file_creations();
    finish_op();
    start_op(true, true);
    benchmark_file_creations();
    finish_op();
    start_op(false, false);
    benchmark_file_creations();
    finish_op();

    return 0;
}