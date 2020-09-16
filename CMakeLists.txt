cmake_minimum_required(VERSION 3.16)
project(hpfs)

# Force build type to Release build.
set(CMAKE_BUILD_TYPE Release)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY build)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY build)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY build)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result")

add_executable(hpfs
    src/util.cpp
    src/tracelog.cpp
    src/logger.cpp
    src/vfs.cpp
    src/fuse_vfs.cpp
    src/fusefs.cpp
    src/merger.cpp
    src/hmap/hasher.cpp
    src/hmap/store.cpp
    src/hmap/query.cpp
    src/hmap/hmap.cpp
    src/hpfs.cpp
    src/main.cpp
)
target_link_libraries(hpfs
    pthread
    libfuse3.so.3
    libblake3.so)