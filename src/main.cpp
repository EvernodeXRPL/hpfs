#include "hpfs.hpp"

int main(int argc, char **argv)
{
    if (hpfs::init(argc, argv) == -1)
        return 1;

    return 0;
}