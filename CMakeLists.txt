cmake_minimum_required(VERSION 3.16)
project(distributed-kv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(kvstore
    src/storage/kv_store.cpp
    src/storage/wal.cpp
    src/storage/persistent_kv_store.cpp
    src/storage/memtable.cpp
    src/storage/sstable.cpp
    src/storage/lsm_tree.cpp
    src/network/protocol.cpp
    src/network/server.cpp
    src/network/client.cpp
)

if(WIN32)
    target_link_libraries(kvstore ws2_32)
endif()

add_executable(kv_test src/main.cpp)
target_link_libraries(kv_test kvstore)

add_executable(kv_server src/server_main.cpp)
target_link_libraries(kv_server kvstore)

add_executable(kv_client src/client_main.cpp)
target_link_libraries(kv_client kvstore)

enable_testing()
