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

# SQLite3
find_library(SQLITE3_LIBRARY NAMES sqlite3)
find_path(SQLITE3_INCLUDE_DIR sqlite3.h)
if(NOT SQLITE3_LIBRARY OR NOT SQLITE3_INCLUDE_DIR)
    message(FATAL_ERROR "SQLite3 not found")
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

# ---- Vendored meridian (StorageBackend + ThreadPool) ----

set(VENDORED_MERIDIAN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/vendor/meridian")

# ---- p4-cache daemon ----

add_executable(p4-cache
    src/main.cpp
    src/depot_cache.cpp
    src/cache_config.cpp
    src/depot_watcher.cpp
    ${VENDORED_MERIDIAN_DIR}/src/storage/backend.cpp
    ${VENDORED_MERIDIAN_DIR}/src/storage/write_cache.cpp
    ${VENDORED_MERIDIAN_DIR}/src/net/http.cpp
)

target_include_directories(p4-cache PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${VENDORED_MERIDIAN_DIR}/include
    ${SQLITE3_INCLUDE_DIR}
)

target_link_libraries(p4-cache PRIVATE
    OpenSSL::Crypto
    OpenSSL::SSL
    Threads::Threads
    ZLIB::ZLIB
    CURL::libcurl
    nlohmann_json::nlohmann_json
    ${SQLITE3_LIBRARY}
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
    ${VENDORED_MERIDIAN_DIR}/src/storage/backend.cpp
    ${VENDORED_MERIDIAN_DIR}/src/storage/write_cache.cpp
    ${VENDORED_MERIDIAN_DIR}/src/net/http.cpp
)

target_include_directories(test-p4-cache PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${VENDORED_MERIDIAN_DIR}/include
    ${SQLITE3_INCLUDE_DIR}
)

target_link_libraries(test-p4-cache PRIVATE
    OpenSSL::Crypto
    OpenSSL::SSL
    Threads::Threads
    ZLIB::ZLIB
    CURL::libcurl
    nlohmann_json::nlohmann_json
    ${SQLITE3_LIBRARY}
)

# ---- Install ----

install(TARGETS p4-cache RUNTIME DESTINATION bin)
install(TARGETS p4shim LIBRARY DESTINATION lib)
