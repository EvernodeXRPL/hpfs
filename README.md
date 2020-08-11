# hpfs
hpfs is a [FUSE](https://www.kernel.org/doc/html/latest/filesystems/fuse.html)-based userspace virtual filesystem intended for [Hot Pocket](https://github.com/HotPocketDev/core) state management. This is a checkpointing filesystem where you start with a seed (initial) state and subsequent writes will always be logged under separately tracked checkpoints without overriding seed state. Reads can be performed on any checkpoint giving the illusion of a coherent filesystem containing all the changes done up to the checkpoint. hpfs also offers the ability to merge-forward the seed state with logged writes thereby "collapsing" the logged writes onto the seed state.

### [See wiki](https://github.com/ravinsp/hpfs/wiki)

## Setup steps

#### Install CMAKE 3.16
1. Download and extract [cmake-3.16.0-rc3-Linux-x86_64.tar.gz](https://github.com/Kitware/CMake/releases/download/v3.16.0-rc3/cmake-3.16.0-rc3-Linux-x86_64.tar.gz)
2. Navigate into the extracted directory in a terminal.
3. Run `sudo cp -r bin/* /usr/local/bin/`
4. Run `sudo cp -r share/* /usr/local/share/`

#### Install libfuse
1. `sudo apt-get install -y meson ninja-build pkg-config`
2. Download [libfuse 3.8](https://github.com/libfuse/libfuse/releases/download/fuse-3.8.0/fuse-3.8.0.tar.xz) and extract.
3. `mkdir build; cd build`
4. `meson .. && ninja`
6. `sudo ninja install`

#### Install blake2
1. `sudo apt-get install -y autoconf libtool libtool-bin`
2. Clone the [blake2b library](https://github.com/BLAKE2/libb2) and follow installation instructions.

#### Install plog
1. Download and extract [plog1.1.5](https://github.com/SergiusTheBest/plog/archive/1.1.5.zip)
2. Navigate into the extracted directory in a terminal.
3. Run `sudo cp -r include/plog /usr/local/include/`

#### Run ldconfig
`sudo ldconfig`

This will update your linker library cache and avoid potential issues when running your compiled C++ program which links to newly installed libraries.

#### Build hpfs
1. Navigate to repo root.
2. Run `cmake .` (You only have to do this once)
3. Run `make` (hpfs binary will be created as `./build/hpfs`)

See [hpfs command line](https://github.com/HotPocketDev/hpfs/wiki/hpfs-command-line) for usage.