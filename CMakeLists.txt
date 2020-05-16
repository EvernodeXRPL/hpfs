cmake_minimum_required(VERSION 3.16)
project(hpfs)

# Check for gold linker for faster linking time.
execute_process(COMMAND ${CMAKE_CXX_COMPILER} -fuse-ld=gold -Wl -ldl,--version OUTPUT_VARIABLE stdout ERROR_QUIET)
if("${stdout}" MATCHES "GNU gold")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fuse-ld=gold")
else()
    message(WARNING "GNU gold linker isn't available, using the default system linker.")
endif()

add_definitions("-std=c++17 -ldl")  #-ldl to support printing stack trace at runtime
set(CMAKE_CXX_FLAGS_DEBUG  "-g")    # ensure debug symbols are added
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY build)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY build)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY build)

add_executable(hpfs
    src/util.cpp
    src/logger.cpp
    src/vfs.cpp
    src/fuse_vfs.cpp
    src/fusefs.cpp
    src/merger.cpp
    src/hpfs.cpp
    src/main.cpp
)
target_link_libraries(hpfs
    libfuse3.so.3)