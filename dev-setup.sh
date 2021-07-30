#!/bin/bash
# Usage ./dev-setup.sh
# hpfs build environment setup script.

set -e # exit on error

sudo apt-get update
sudo apt-get install -y build-essential

workdir=~/hpfs-setup

mkdir $workdir
pushd $workdir > /dev/null 2>&1

# CMAKE
cmake=cmake-3.16.0-rc3-Linux-x86_64
wget https://github.com/Kitware/CMake/releases/download/v3.16.0-rc3/$cmake.tar.gz
tar -zxvf $cmake.tar.gz
sudo cp -r $cmake/bin/* /usr/local/bin/
sudo cp -r $cmake/share/* /usr/local/share/
rm $cmake.tar.gz && rm -r $cmake

# libfuse
sudo apt-get install -y meson ninja-build pkg-config
wget https://github.com/libfuse/libfuse/archive/fuse-3.8.0.tar.gz
tar -zxvf fuse-3.8.0.tar.gz
pushd libfuse-fuse-3.8.0 > /dev/null 2>&1
mkdir build
pushd build > /dev/null 2>&1
meson .. && ninja
sudo ninja install
popd > /dev/null 2>&1
popd > /dev/null 2>&1
rm fuse-3.8.0.tar.gz && rm -r libfuse-fuse-3.8.0

# Blake3
git clone https://github.com/BLAKE3-team/BLAKE3.git
pushd BLAKE3/c > /dev/null 2>&1
gcc -shared -fPIC -O3 -o libblake3.so blake3.c blake3_dispatch.c blake3_portable.c \
    blake3_sse2_x86-64_unix.S blake3_sse41_x86-64_unix.S blake3_avx2_x86-64_unix.S \
    blake3_avx512_x86-64_unix.S
sudo cp blake3.h /usr/local/include/
sudo cp libblake3.so /usr/local/lib/
popd > /dev/null 2>&1
sudo rm -r BLAKE3

# Plog
wget https://github.com/SergiusTheBest/plog/archive/1.1.5.tar.gz
tar -zxvf 1.1.5.tar.gz
pushd plog-1.1.5 > /dev/null 2>&1
sudo cp -r include/plog /usr/local/include/
popd > /dev/null 2>&1
rm 1.1.5.tar.gz && rm -r plog-1.1.5

# CLI11
wget https://github.com/CLIUtils/CLI11/archive/refs/tags/v2.0.0.tar.gz
tar -zxvf v2.0.0.tar.gz
pushd CLI11-2.0.0 > /dev/null 2>&1
mkdir build
pushd build > /dev/null 2>&1
cmake ..
sudo make install/fast
popd > /dev/null 2>&1
popd > /dev/null 2>&1
rm v2.0.0.tar.gz && sudo rm -r CLI11-2.0.0

# Update linker library cache.
sudo ldconfig

# Pop workdir
popd > /dev/null 2>&1
rm -r $workdir

# Build hpfs
cmake .
make