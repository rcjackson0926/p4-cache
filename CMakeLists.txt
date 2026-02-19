cmake_minimum_required(VERSION 3.20)
project(p4-cache VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Compiler flags
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    add_compile_options(-O3 -Wall -Wextra -Wpedantic)
endif()

# Optional sanitizers
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
if(ENABLE_ASAN)
    add_compile_options(-g -fno-omit-frame-pointer -fsanitize=address,undefined)
    add_link_options(-fsanitize=address,undefined)
elseif(ENABLE_TSAN)
    add_compile_options(-g -fno-omit-frame-pointer -fsanitize=thread)
    add_link_options(-fsanitize=thread)
endif()

# ---- Dependencies ----

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(CURL REQUIRED)

# LMDB (manifest database)
find_library(LMDB_LIBRARY NAMES lmdb)
find_path(LMDB_INCLUDE_DIR lmdb.h)
if(NOT LMDB_LIBRARY OR NOT LMDB_INCLUDE_DIR)
    message(FATAL_ERROR "LMDB not found (install liblmdb-dev)")
endif()

# nlohmann/json
find_package(nlohmann_json CONFIG QUIET)
if(TARGET nlohmann_json::nlohmann_json)
    message(STATUS "nlohmann/json found via package config")
elseif(EXISTS "/tmp/nlohmann_json/include/nlohmann/json.hpp")
    add_library(nlohmann_json::nlohmann_json INTERFACE IMPORTED)
    set_target_properties(nlohmann_json::nlohmann_json PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "/tmp/nlohmann_json/include"
    )
    message(STATUS "nlohmann/json found via local cached headers")
else()
    include(FetchContent)
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.3
    )
    FetchContent_MakeAvailable(nlohmann_json)
    message(STATUS "nlohmann/json fetched")
endif()

# prometheus-cpp (core only — textfile serialization, no HTTP server)
include(FetchContent)
FetchContent_Declare(
    prometheus_cpp
    GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
    GIT_TAG v1.3.0
)
set(ENABLE_PUSH OFF CACHE BOOL "" FORCE)
set(ENABLE_PULL OFF CACHE BOOL "" FORCE)
set(ENABLE_COMPRESSION OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(GENERATE_PKGCONFIG OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(prometheus_cpp)

# ---- p4-cache daemon ----

add_executable(p4-cache
    src/main.cpp
    src/depot_cache.cpp
    src/cache_config.cpp
    src/depot_watcher.cpp
    src/storage_backend.cpp
    src/http_client.cpp
    src/metrics.cpp
)

target_include_directories(p4-cache PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${LMDB_INCLUDE_DIR}
)

target_link_libraries(p4-cache PRIVATE
    OpenSSL::Crypto
    OpenSSL::SSL
    Threads::Threads
    ZLIB::ZLIB
    CURL::libcurl
    nlohmann_json::nlohmann_json
    ${LMDB_LIBRARY}
    prometheus-cpp::core
)

# ---- LD_PRELOAD shim library ----

add_library(p4shim SHARED src/shim.cpp)
target_link_libraries(p4shim PRIVATE ${CMAKE_DL_LIBS} Threads::Threads)
set_target_properties(p4shim PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    OUTPUT_NAME "p4shim"
)

# ---- Tests ----

add_executable(test-p4-cache
    tests/test_cache.cpp
    src/depot_cache.cpp
    src/cache_config.cpp
    src/depot_watcher.cpp
    src/storage_backend.cpp
    src/http_client.cpp
    src/metrics.cpp
)

target_include_directories(test-p4-cache PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${LMDB_INCLUDE_DIR}
)

target_link_libraries(test-p4-cache PRIVATE
    OpenSSL::Crypto
    OpenSSL::SSL
    Threads::Threads
    ZLIB::ZLIB
    CURL::libcurl
    nlohmann_json::nlohmann_json
    ${LMDB_LIBRARY}
    prometheus-cpp::core
)

# ---- Install ----

install(TARGETS p4-cache RUNTIME DESTINATION bin)
install(TARGETS p4shim LIBRARY DESTINATION lib)
