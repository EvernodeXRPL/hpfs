# hpfs
hpfs is a [FUSE](https://www.kernel.org/doc/html/latest/filesystems/fuse.html)-based userspace virtual filesystem intended for [Hot Pocket](https://github.com/HotPocketDev/core) state management. This is a checkpointing filesystem where you start with a seed (initial) state and subsequent writes will always be logged under separately tracked checkpoints without overriding seed state. Reads can be performed on any checkpoint giving the illusion of a coherent filesystem containing all the changes done up to the checkpoint. hpfs also offers the ability to merge-forward the seed state with logged writes thereby "collapsing" the logged writes onto the seed state.

## Setting up hpfs development environment
Run the setup script located at the repo root (tested on Ubuntu 20.04).
```
./dev-setup.sh
```

## Build hpfs
1. Run `cmake .` (You only have to do this once)
1. Run `make` (hpfs binary will be created as `./build/hpfs`)
1. Refer to the Wiki for instructions on running hpfs.