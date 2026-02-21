cmake_minimum_required(VERSION 3.16)
project(distributed-kv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(kvstore
    src/storage/kv_store.cpp
)

add_executable(kv_test src/main.cpp)
target_link_libraries(kv_test kvstore)

enable_testing()
