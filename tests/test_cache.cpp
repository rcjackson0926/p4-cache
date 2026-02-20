// Comprehensive test suite for p4-cache.
//
// Tests:
//   1. BackendConfig validation (all backend types)
//   2. CacheConfig CLI argument parsing
//   3. CacheConfig JSON loading
//   4. CacheConfig defaults and validation
//   5. DepotCache integration: full lifecycle with local storage backend
//      - Start/stop
//      - File write detection → background upload
//      - Shim fetch for cold files
//      - Eviction under pressure
//      - Restore after eviction
//      - Primary/secondary fallback
//      - Stats tracking

#include "p4cache/cache_config.hpp"
#include "p4cache/depot_cache.hpp"
#include "p4cache/metrics.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <lmdb.h>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace p4cache;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                                                    \
    do {                                                              \
        std::cout << "  " << #name << "... " << std::flush;          \
    } while (0)

#define PASS()                                                        \
    do {                                                              \
        std::cout << "OK" << std::endl;                               \
        ++tests_passed;                                               \
    } while (0)

#define FAIL(msg)                                                     \
    do {                                                              \
        std::cout << "FAIL: " << msg << std::endl;                    \
        ++tests_failed;                                               \
    } while (0)

#define ASSERT_TRUE(cond, msg)                                        \
    do {                                                              \
        if (!(cond)) { FAIL(msg); return; }                           \
    } while (0)

#define ASSERT_EQ(a, b, msg)                                          \
    do {                                                              \
        if ((a) != (b)) {                                             \
            std::cout << "FAIL: " << msg << " (got \"" << (a)        \
                      << "\", expected \"" << (b) << "\")"            \
                      << std::endl;                                   \
            ++tests_failed;                                           \
            return;                                                   \
        }                                                             \
    } while (0)

#define ASSERT_EMPTY(s, msg)                                          \
    ASSERT_TRUE((s).empty(), msg ": " + (s))

#define ASSERT_NOT_EMPTY(s, msg)                                      \
    ASSERT_TRUE(!(s).empty(), msg)

/// Create a unique temp directory under /tmp.
static fs::path make_temp_dir(const std::string& prefix) {
    auto path = fs::temp_directory_path() / (prefix + "-XXXXXX");
    std::string tpl = path.string();
    char* result = mkdtemp(tpl.data());
    if (!result) throw std::runtime_error("mkdtemp failed");
    return fs::path(result);
}

/// Write binary content to a file.
static void write_file(const fs::path& path, const std::string& content) {
    fs::create_directories(path.parent_path());
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    ofs.write(content.data(), content.size());
}

/// Read entire file into a string.
static std::string read_file(const fs::path& path) {
    std::ifstream ifs(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(ifs)),
                       std::istreambuf_iterator<char>());
}

/// Wait for a condition with timeout (milliseconds). Returns true if met.
static bool wait_for(std::function<bool()> cond, int timeout_ms = 5000) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (cond()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return cond();
}

/// Send a FETCH command over the shim Unix socket. Returns the response.
static std::string shim_fetch(const fs::path& sock_path, const std::string& rel_path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return "ERROR socket()";

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path.c_str(), sizeof(addr.sun_path) - 1);

    if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return "ERROR connect()";
    }

    struct timeval tv;
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string request = "FETCH " + rel_path + "\n";
    write(fd, request.c_str(), request.size());

    char buf[256];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);

    if (n <= 0) return "ERROR read()";
    buf[n] = '\0';
    // Trim trailing newline
    std::string resp(buf);
    while (!resp.empty() && (resp.back() == '\n' || resp.back() == '\r'))
        resp.pop_back();
    return resp;
}

// ---------------------------------------------------------------------------
// 1. BackendConfig validation tests
// ---------------------------------------------------------------------------

static void test_backend_config_validation() {
    std::cout << "\n=== BackendConfig validation ===" << std::endl;

    {
        TEST(empty_type_fails);
        BackendConfig bc;
        auto err = bc.validate();
        ASSERT_NOT_EMPTY(err, "empty type should fail");
        PASS();
    }
    {
        TEST(unknown_type_fails);
        BackendConfig bc;
        bc.type = "ftp";
        auto err = bc.validate();
        ASSERT_TRUE(err.find("unknown") != std::string::npos, "should say unknown");
        PASS();
    }
    {
        TEST(s3_requires_bucket);
        BackendConfig bc;
        bc.type = "s3";
        auto err = bc.validate();
        ASSERT_TRUE(err.find("bucket") != std::string::npos, "s3 needs bucket");
        bc.params["bucket"] = "my-bucket";
        err = bc.validate();
        ASSERT_EMPTY(err, "s3 with bucket should pass");
        PASS();
    }
    {
        TEST(azure_requires_container);
        BackendConfig bc;
        bc.type = "azure";
        auto err = bc.validate();
        ASSERT_TRUE(err.find("container") != std::string::npos, "azure needs container");
        bc.params["container"] = "my-container";
        err = bc.validate();
        ASSERT_EMPTY(err, "azure with container should pass");
        PASS();
    }
    {
        TEST(gcs_requires_bucket);
        BackendConfig bc;
        bc.type = "gcs";
        auto err = bc.validate();
        ASSERT_TRUE(err.find("bucket") != std::string::npos, "gcs needs bucket");
        bc.params["bucket"] = "my-gcs-bucket";
        err = bc.validate();
        ASSERT_EMPTY(err, "gcs with bucket should pass");
        PASS();
    }
    {
        TEST(nfs_requires_existing_path);
        BackendConfig bc;
        bc.type = "nfs";
        auto err = bc.validate();
        ASSERT_TRUE(err.find("path") != std::string::npos, "nfs needs path");
        bc.params["path"] = "/nonexistent/path/abc123";
        err = bc.validate();
        ASSERT_TRUE(err.find("does not exist") != std::string::npos,
                     "nfs path must exist");
        bc.params["path"] = "/tmp";
        err = bc.validate();
        ASSERT_EMPTY(err, "nfs with existing path should pass");
        PASS();
    }
}

// ---------------------------------------------------------------------------
// 2. CacheConfig CLI argument parsing
// ---------------------------------------------------------------------------

static void test_config_cli_parsing() {
    std::cout << "\n=== CacheConfig CLI parsing ===" << std::endl;

    {
        TEST(basic_s3_args);
        const char* args[] = {
            "p4-cache",
            "--depot-path", "/tmp",
            "--primary-type", "s3",
            "--primary-bucket", "test-bucket",
            "--primary-endpoint", "https://s3.example.com",
            "--primary-region", "eu-west-1",
            "--max-cache-gb", "50",
        };
        auto cfg = CacheConfig::from_args(13, const_cast<char**>(args));
        ASSERT_TRUE(cfg.has_value(), "should parse");
        ASSERT_EQ(cfg->depot_path.string(), "/tmp", "depot_path");
        ASSERT_EQ(cfg->primary.type, "s3", "primary type");
        ASSERT_EQ(cfg->primary.params["bucket"], "test-bucket", "bucket");
        ASSERT_EQ(cfg->primary.params["endpoint"], "https://s3.example.com", "endpoint");
        ASSERT_EQ(cfg->primary.params["region"], "eu-west-1", "region");
        ASSERT_EQ(cfg->max_cache_bytes, 50ULL * 1024 * 1024 * 1024, "max_cache");
        PASS();
    }
    {
        TEST(azure_with_secondary);
        const char* args[] = {
            "p4-cache",
            "--depot-path", "/tmp",
            "--primary-type", "azure",
            "--primary-container", "depot",
            "--primary-account-name", "myaccount",
            "--secondary-type", "nfs",
            "--secondary-path", "/tmp",
        };
        auto cfg = CacheConfig::from_args(13, const_cast<char**>(args));
        ASSERT_TRUE(cfg.has_value(), "should parse");
        ASSERT_EQ(cfg->primary.type, "azure", "primary type");
        ASSERT_EQ(cfg->primary.params["container"], "depot", "container");
        ASSERT_EQ(cfg->primary.params["account_name"], "myaccount", "account_name");
        ASSERT_EQ(cfg->secondary.type, "nfs", "secondary type");
        ASSERT_EQ(cfg->secondary.params["path"], "/tmp", "secondary path");
        PASS();
    }
    {
        TEST(bool_flags);
        const char* args[] = {
            "p4-cache",
            "--depot-path", "/tmp",
            "--primary-type", "s3",
            "--primary-bucket", "b",
            "--primary-no-verify-ssl",
            "--primary-sse",
            "--read-only",
            "--verbose",
        };
        auto cfg = CacheConfig::from_args(11, const_cast<char**>(args));
        ASSERT_TRUE(cfg.has_value(), "should parse");
        ASSERT_EQ(cfg->primary.params["verify_ssl"], "false", "no-verify-ssl");
        ASSERT_EQ(cfg->primary.params["server_side_encryption"], "true", "sse");
        ASSERT_TRUE(cfg->read_only, "read_only");
        ASSERT_TRUE(cfg->verbose, "verbose");
        PASS();
    }
    {
        TEST(daemon_flags);
        const char* args[] = {
            "p4-cache",
            "--depot-path", "/tmp",
            "--primary-type", "s3",
            "--primary-bucket", "b",
            "--daemon",
            "--upload-threads", "4",
            "--restore-threads", "8",
            "--stats-interval", "30",
            "--low-watermark-gb", "60",
        };
        auto cfg = CacheConfig::from_args(16, const_cast<char**>(args));
        ASSERT_TRUE(cfg.has_value(), "should parse");
        ASSERT_TRUE(cfg->daemonize, "daemonize");
        ASSERT_EQ(cfg->upload_threads, (size_t)4, "upload_threads");
        ASSERT_EQ(cfg->restore_threads, (size_t)8, "restore_threads");
        ASSERT_EQ(cfg->stats_interval_secs, (size_t)30, "stats_interval");
        ASSERT_EQ(cfg->eviction_low_watermark, 60ULL * 1024 * 1024 * 1024, "low_watermark");
        PASS();
    }
    {
        TEST(unknown_option_fails);
        const char* args[] = {
            "p4-cache", "--depot-path", "/tmp", "--bogus-flag",
        };
        auto cfg = CacheConfig::from_args(4, const_cast<char**>(args));
        ASSERT_TRUE(!cfg.has_value(), "should fail on unknown flag");
        PASS();
    }
    {
        TEST(help_returns_nullopt);
        const char* args[] = { "p4-cache", "--help" };
        auto cfg = CacheConfig::from_args(2, const_cast<char**>(args));
        ASSERT_TRUE(!cfg.has_value(), "help should return nullopt");
        PASS();
    }
}

// ---------------------------------------------------------------------------
// 3. CacheConfig JSON loading
// ---------------------------------------------------------------------------

static void test_config_json() {
    std::cout << "\n=== CacheConfig JSON loading ===" << std::endl;

    auto tmpdir = make_temp_dir("p4cache-json");
    auto json_path = tmpdir / "config.json";

    {
        TEST(basic_json_loading);
        write_file(json_path,
            R"({
                "depot_path": "/tmp",
                "read_only": true,
                "max_cache_gb": 200,
                "low_watermark_gb": 150,
                "upload_threads": 4,
                "upload_concurrency": 8,
                "restore_threads": 12,
                "verbose": true,
                "stats_interval": 120,
                "primary": {
                    "type": "s3",
                    "bucket": "json-bucket",
                    "region": "ap-southeast-1",
                    "endpoint": "https://s3.ap.example.com"
                },
                "secondary": {
                    "type": "nfs",
                    "path": "/tmp"
                }
            })");

        CacheConfig cfg;
        ASSERT_TRUE(cfg.load_json(json_path), "load_json should succeed");
        ASSERT_EQ(cfg.depot_path.string(), "/tmp", "depot_path");
        ASSERT_TRUE(cfg.read_only, "read_only");
        ASSERT_EQ(cfg.max_cache_bytes, 200ULL * 1024 * 1024 * 1024, "max_cache");
        ASSERT_EQ(cfg.eviction_low_watermark, 150ULL * 1024 * 1024 * 1024, "low_watermark");
        ASSERT_EQ(cfg.upload_threads, (size_t)4, "upload_threads");
        ASSERT_EQ(cfg.upload_concurrency, (size_t)8, "upload_concurrency");
        ASSERT_EQ(cfg.restore_threads, (size_t)12, "restore_threads");
        ASSERT_TRUE(cfg.verbose, "verbose");
        ASSERT_EQ(cfg.stats_interval_secs, (size_t)120, "stats_interval");
        ASSERT_EQ(cfg.primary.type, "s3", "primary type");
        ASSERT_EQ(cfg.primary.params["bucket"], "json-bucket", "bucket");
        ASSERT_EQ(cfg.primary.params["region"], "ap-southeast-1", "region");
        ASSERT_EQ(cfg.primary.params["endpoint"], "https://s3.ap.example.com", "endpoint");
        ASSERT_EQ(cfg.secondary.type, "nfs", "secondary type");
        ASSERT_EQ(cfg.secondary.params["path"], "/tmp", "secondary path");
        PASS();
    }
    {
        TEST(cli_config_flag_loads_json);
        write_file(json_path,
            R"({
                "depot_path": "/tmp",
                "primary": { "type": "gcs", "bucket": "gcs-bucket" }
            })");
        std::string path_str = json_path.string();
        const char* args[] = {
            "p4-cache", "--config", path_str.c_str(),
        };
        auto cfg = CacheConfig::from_args(3, const_cast<char**>(args));
        ASSERT_TRUE(cfg.has_value(), "should parse");
        ASSERT_EQ(cfg->primary.type, "gcs", "type from json");
        ASSERT_EQ(cfg->primary.params["bucket"], "gcs-bucket", "bucket from json");
        PASS();
    }
    {
        TEST(nonexistent_json_fails);
        CacheConfig cfg;
        ASSERT_TRUE(!cfg.load_json("/nonexistent/config.json"), "should fail");
        PASS();
    }

    fs::remove_all(tmpdir);
}

// ---------------------------------------------------------------------------
// 4. CacheConfig defaults and validation
// ---------------------------------------------------------------------------

static void test_config_defaults_and_validation() {
    std::cout << "\n=== CacheConfig defaults and validation ===" << std::endl;

    {
        TEST(apply_defaults_state_dir);
        CacheConfig cfg;
        cfg.depot_path = "/mnt/nvme/depot";
        cfg.primary.type = "s3";
        cfg.primary.params["bucket"] = "b";
        cfg.apply_defaults();
        ASSERT_EQ(cfg.state_dir.string(), "/mnt/nvme/depot/.p4cache", "state_dir");
        PASS();
    }
    {
        TEST(apply_defaults_path_prefix_cloud);
        CacheConfig cfg;
        cfg.depot_path = "/mnt/nvme/depot";
        cfg.primary.type = "s3";
        cfg.primary.params["bucket"] = "b";
        cfg.apply_defaults();
        ASSERT_EQ(cfg.primary.params["path_prefix"], "depot", "prefix should be dirname");
        PASS();
    }
    {
        TEST(apply_defaults_no_prefix_for_nfs);
        CacheConfig cfg;
        cfg.depot_path = "/mnt/nvme/depot";
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = "/tmp";
        cfg.apply_defaults();
        ASSERT_TRUE(cfg.primary.params.count("path_prefix") == 0 ||
                     cfg.primary.params["path_prefix"].empty(),
                     "nfs should not get a prefix");
        PASS();
    }
    {
        TEST(apply_defaults_eviction_target);
        CacheConfig cfg;
        cfg.eviction_low_watermark = 100;
        cfg.apply_defaults();
        ASSERT_EQ(cfg.eviction_target, (uint64_t)90, "target = 90% of watermark");
        PASS();
    }
    {
        TEST(validate_requires_depot_path);
        CacheConfig cfg;
        cfg.primary.type = "s3";
        cfg.primary.params["bucket"] = "b";
        auto err = cfg.validate();
        ASSERT_TRUE(err.find("depot_path") != std::string::npos, "needs depot_path");
        PASS();
    }
    {
        TEST(validate_requires_primary_type);
        CacheConfig cfg;
        cfg.depot_path = "/tmp";
        auto err = cfg.validate();
        ASSERT_TRUE(err.find("primary") != std::string::npos, "needs primary type");
        PASS();
    }
    {
        TEST(validate_checks_primary_backend);
        CacheConfig cfg;
        cfg.depot_path = "/tmp";
        cfg.primary.type = "s3";
        // missing bucket
        auto err = cfg.validate();
        ASSERT_TRUE(err.find("bucket") != std::string::npos, "needs bucket");
        PASS();
    }
    {
        TEST(validate_checks_secondary_if_present);
        CacheConfig cfg;
        cfg.depot_path = "/tmp";
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = "/tmp";
        cfg.secondary.type = "azure";
        // missing container
        auto err = cfg.validate();
        ASSERT_TRUE(err.find("secondary") != std::string::npos &&
                     err.find("container") != std::string::npos,
                     "should validate secondary");
        PASS();
    }
    {
        TEST(validate_good_config_passes);
        CacheConfig cfg;
        cfg.depot_path = "/tmp";
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = "/tmp";
        auto err = cfg.validate();
        ASSERT_EMPTY(err, "valid config should pass");
        PASS();
    }
}

// ---------------------------------------------------------------------------
// 5. DepotCache integration tests (with local filesystem backend)
// ---------------------------------------------------------------------------

static void test_depot_cache_integration() {
    std::cout << "\n=== DepotCache integration ===" << std::endl;

    auto root = make_temp_dir("p4cache-integ");
    auto depot = root / "depot";
    auto storage = root / "storage";
    fs::create_directories(depot);
    fs::create_directories(storage);

    // ---- start / stop ----
    {
        TEST(start_and_stop);
        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.upload_threads = 2;
        cfg.upload_concurrency = 4;
        cfg.restore_threads = 4;
        cfg.stats_interval_secs = 0;  // disable stats thread
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start should succeed");

        // Verify state directory was created
        ASSERT_TRUE(fs::is_directory(cfg.state_dir / "manifest"), "manifest directory should exist");
        // Shim socket is created by a background thread, wait for it
        bool sock_ready = wait_for([&]{ return fs::exists(cfg.state_dir / "shim.sock"); }, 3000);
        ASSERT_TRUE(sock_ready, "shim.sock should exist");

        cache.stop();
        cache.wait();
        PASS();
    }

    // Clean up for fresh start
    fs::remove_all(root / "state");
    // Clear any files from previous test
    for (auto& entry : fs::directory_iterator(depot)) fs::remove_all(entry);
    for (auto& entry : fs::directory_iterator(storage)) fs::remove_all(entry);

    // ---- write → upload lifecycle ----
    {
        TEST(write_triggers_upload);
        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.upload_threads = 2;
        cfg.upload_concurrency = 4;
        cfg.restore_threads = 4;
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        // Write a file to the depot
        auto file1 = depot / "dir1" / "file1.txt";
        write_file(file1, "hello world from test");

        // Notify the cache
        cache.on_file_written(file1);

        // Wait for the upload to complete (file should appear in storage)
        auto uploaded = storage / "dir1" / "file1.txt";
        bool appeared = wait_for([&]{ return fs::exists(uploaded); }, 10000);
        ASSERT_TRUE(appeared, "file should be uploaded to storage backend");

        // Verify content matches
        auto content = read_file(uploaded);
        ASSERT_EQ(content, "hello world from test", "uploaded content should match");

        // Check stats
        auto stats = cache.get_stats();
        ASSERT_TRUE(stats.uploads_completed >= 1, "should have at least 1 upload");

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- multiple file uploads ----
    {
        TEST(multiple_file_uploads);

        // Clean storage
        for (auto& entry : fs::directory_iterator(storage)) fs::remove_all(entry);
        fs::remove_all(root / "state");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.upload_threads = 2;
        cfg.upload_concurrency = 4;
        cfg.restore_threads = 4;
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        // Write 10 files
        for (int i = 0; i < 10; ++i) {
            auto path = depot / ("batch" + std::to_string(i) + ".dat");
            write_file(path, "content-" + std::to_string(i));
            cache.on_file_written(path);
        }

        // Wait for all uploads
        bool all_uploaded = wait_for([&] {
            for (int i = 0; i < 10; ++i) {
                if (!fs::exists(storage / ("batch" + std::to_string(i) + ".dat")))
                    return false;
            }
            return true;
        }, 15000);
        ASSERT_TRUE(all_uploaded, "all 10 files should upload");

        auto stats = cache.get_stats();
        ASSERT_TRUE(stats.uploads_completed >= 10, "should have >= 10 uploads");

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- shim fetch for cold files ----
    {
        TEST(shim_fetch_cold_file);

        // Clean depot and storage, start fresh
        for (auto& entry : fs::directory_iterator(depot)) fs::remove_all(entry);
        for (auto& entry : fs::directory_iterator(storage)) fs::remove_all(entry);
        fs::remove_all(root / "state");

        // Pre-stage a file in storage that doesn't exist in depot
        auto cold_storage_path = storage / "cold" / "data.bin";
        write_file(cold_storage_path, "cold-file-content-12345");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.upload_threads = 2;
        cfg.upload_concurrency = 4;
        cfg.restore_threads = 4;
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        // Use fetch_for_shim API directly to fetch a file not on NVMe
        auto result = cache.fetch_for_shim("cold/data.bin");
        ASSERT_TRUE(result.find("OK ") == 0, "fetch should return OK: " + result);

        // Verify the file now exists in the depot
        auto depot_file = depot / "cold" / "data.bin";
        ASSERT_TRUE(fs::exists(depot_file), "file should now exist in depot");
        ASSERT_EQ(read_file(depot_file), "cold-file-content-12345", "content should match");

        // Check stats
        auto stats = cache.get_stats();
        ASSERT_TRUE(stats.shim_fetches >= 1, "shim_fetches should be >= 1");

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- shim NOTFOUND ----
    {
        TEST(shim_fetch_not_found);

        fs::remove_all(root / "state");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        auto result = cache.fetch_for_shim("does/not/exist.txt");
        ASSERT_EQ(result, "NOTFOUND", "nonexistent file should return NOTFOUND");

        auto stats = cache.get_stats();
        ASSERT_TRUE(stats.shim_not_found >= 1, "shim_not_found should be >= 1");

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- shim Unix socket protocol ----
    {
        TEST(shim_socket_protocol);

        for (auto& entry : fs::directory_iterator(depot)) fs::remove_all(entry);
        fs::remove_all(root / "state");

        // Pre-stage a file in storage
        write_file(storage / "socket-test.txt", "socket-test-data");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        auto sock_path = cfg.state_dir / "shim.sock";

        // Wait for socket to be ready
        bool sock_ready = wait_for([&]{ return fs::exists(sock_path); }, 3000);
        ASSERT_TRUE(sock_ready, "shim socket should exist");

        // Test FETCH via socket
        auto resp = shim_fetch(sock_path, "socket-test.txt");
        ASSERT_TRUE(resp.find("OK ") == 0, "should get OK response: " + resp);

        // Test FETCH for nonexistent file via socket
        resp = shim_fetch(sock_path, "nonexistent-socket.dat");
        ASSERT_EQ(resp, "NOTFOUND", "should get NOTFOUND via socket");

        // Test invalid command via socket
        {
            int fd = socket(AF_UNIX, SOCK_STREAM, 0);
            struct sockaddr_un addr;
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, sock_path.c_str(), sizeof(addr.sun_path) - 1);
            connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
            struct timeval tv;
            tv.tv_sec = 5; tv.tv_usec = 0;
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            const char* cmd = "INVALID command\n";
            write(fd, cmd, strlen(cmd));
            char buf[256];
            ssize_t n = read(fd, buf, sizeof(buf) - 1);
            close(fd);
            if (n > 0) {
                buf[n] = '\0';
                std::string r(buf);
                ASSERT_TRUE(r.find("ERROR") != std::string::npos,
                             "invalid command should return ERROR: " + r);
            }
        }

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- eviction and restore ----
    {
        TEST(eviction_and_restore);

        for (auto& entry : fs::directory_iterator(depot)) fs::remove_all(entry);
        for (auto& entry : fs::directory_iterator(storage)) fs::remove_all(entry);
        fs::remove_all(root / "state");

        // Set very small cache limits to trigger eviction
        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.upload_threads = 2;
        cfg.upload_concurrency = 4;
        cfg.restore_threads = 4;
        cfg.stats_interval_secs = 0;
        // 4 KB cache, triggers eviction very quickly
        cfg.max_cache_bytes = 4096;
        cfg.eviction_low_watermark = 2048;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        // Write a 1 KB file — should upload then become eviction candidate
        std::string data1(1024, 'A');
        auto f1 = depot / "evict1.dat";
        write_file(f1, data1);
        cache.on_file_written(f1);

        // Write another 1 KB file
        std::string data2(1024, 'B');
        auto f2 = depot / "evict2.dat";
        write_file(f2, data2);
        cache.on_file_written(f2);

        // Wait for uploads to complete
        bool uploaded = wait_for([&] {
            return fs::exists(storage / "evict1.dat") &&
                   fs::exists(storage / "evict2.dat");
        }, 10000);
        ASSERT_TRUE(uploaded, "files should upload");

        // Now write a big file to trigger eviction
        std::string data3(3072, 'C');
        auto f3 = depot / "evict3.dat";
        write_file(f3, data3);
        cache.on_file_written(f3);

        // Wait for the big file to upload
        bool big_uploaded = wait_for([&] {
            return fs::exists(storage / "evict3.dat");
        }, 10000);
        ASSERT_TRUE(big_uploaded, "big file should upload");

        // Wait for eviction to happen (evict1 or evict2 should be deleted)
        bool evicted = wait_for([&] {
            return !fs::exists(f1) || !fs::exists(f2);
        }, 15000);
        ASSERT_TRUE(evicted, "at least one file should be evicted (deleted)");

        auto stats = cache.get_stats();
        ASSERT_TRUE(stats.evictions_performed >= 1, "should have >= 1 eviction");

        // Now restore an evicted file via fetch_for_shim
        // Find which file was evicted (deleted)
        std::string evicted_rel;
        std::string expected_content;
        if (!fs::exists(f1)) {
            evicted_rel = "evict1.dat";
            expected_content = data1;
        } else {
            evicted_rel = "evict2.dat";
            expected_content = data2;
        }

        auto fetch_result = cache.fetch_for_shim(evicted_rel);
        ASSERT_TRUE(fetch_result.find("OK ") == 0,
                     "fetch should restore evicted file: " + fetch_result);

        // Verify restored content
        auto restored_path = depot / evicted_rel;
        ASSERT_TRUE(fs::file_size(restored_path) > 0, "restored file should have content");
        ASSERT_EQ(read_file(restored_path), expected_content, "restored content should match");

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- secondary backend fallback ----
    {
        TEST(secondary_backend_fallback);

        for (auto& entry : fs::directory_iterator(depot)) fs::remove_all(entry);
        for (auto& entry : fs::directory_iterator(storage)) fs::remove_all(entry);
        fs::remove_all(root / "state");

        auto secondary_storage = root / "secondary";
        fs::create_directories(secondary_storage);

        // Put a file only in secondary storage (not primary)
        write_file(secondary_storage / "fallback.txt", "secondary-only-content");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.secondary.type = "nfs";
        cfg.secondary.params["path"] = secondary_storage.string();
        cfg.state_dir = root / "state";
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        // Fetch should fail from primary, succeed from secondary
        auto result = cache.fetch_for_shim("fallback.txt");
        ASSERT_TRUE(result.find("OK ") == 0,
                     "should fetch from secondary: " + result);

        auto depot_file = depot / "fallback.txt";
        ASSERT_TRUE(fs::exists(depot_file), "file should be in depot");
        ASSERT_EQ(read_file(depot_file), "secondary-only-content", "content from secondary");

        cache.stop();
        cache.wait();

        // Clean up secondary
        fs::remove_all(secondary_storage);
        PASS();
    }

    // ---- scan_untracked_files (files written while daemon was down) ----
    {
        TEST(scan_untracked_files);

        for (auto& entry : fs::directory_iterator(depot)) fs::remove_all(entry);
        for (auto& entry : fs::directory_iterator(storage)) fs::remove_all(entry);
        fs::remove_all(root / "state");

        // Write files to depot BEFORE starting the cache
        write_file(depot / "pre1.txt", "pre-existing-1");
        write_file(depot / "subdir" / "pre2.txt", "pre-existing-2");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.upload_threads = 2;
        cfg.upload_concurrency = 4;
        cfg.restore_threads = 4;
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        // Wait for the untracked files to be uploaded
        bool uploaded = wait_for([&] {
            return fs::exists(storage / "pre1.txt") &&
                   fs::exists(storage / "subdir" / "pre2.txt");
        }, 15000);
        ASSERT_TRUE(uploaded, "pre-existing files should be uploaded");
        ASSERT_EQ(read_file(storage / "pre1.txt"), "pre-existing-1", "pre1 content");
        ASSERT_EQ(read_file(storage / "subdir" / "pre2.txt"), "pre-existing-2", "pre2 content");

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- read-only mode ----
    {
        TEST(read_only_mode);

        for (auto& entry : fs::directory_iterator(depot)) fs::remove_all(entry);
        for (auto& entry : fs::directory_iterator(storage)) fs::remove_all(entry);
        fs::remove_all(root / "state");

        // Pre-stage a file in storage
        write_file(storage / "readonly.txt", "readonly-content");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.read_only = true;
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        ASSERT_TRUE(cache.is_read_only(), "should be read_only");

        auto err = cache.start();
        ASSERT_EMPTY(err, "start in read-only");

        // Fetching should still work
        auto result = cache.fetch_for_shim("readonly.txt");
        ASSERT_TRUE(result.find("OK ") == 0, "fetch in read-only: " + result);
        ASSERT_EQ(read_file(depot / "readonly.txt"), "readonly-content", "content");

        cache.stop();
        cache.wait();
        PASS();
    }

    // Clean up
    fs::remove_all(root);
}

// ---------------------------------------------------------------------------
// 6. Access log tests
// ---------------------------------------------------------------------------

/// Helper: read an access LMDB entry. Returns 0 on found, MDB_NOTFOUND, or error.
static int access_db_get(const fs::path& access_dir, const std::string& key, uint64_t& ts_out) {
    MDB_env* env = nullptr;
    int rc = mdb_env_create(&env);
    if (rc) return rc;
    mdb_env_set_mapsize(env, 1ULL * 1024 * 1024 * 1024);
    rc = mdb_env_open(env, access_dir.c_str(), MDB_RDONLY, 0664);
    if (rc) { mdb_env_close(env); return rc; }

    MDB_txn* txn = nullptr;
    rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
    if (rc) { mdb_env_close(env); return rc; }

    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, nullptr, 0, &dbi);
    if (rc) { mdb_txn_abort(txn); mdb_env_close(env); return rc; }

    MDB_val k = {key.size(), const_cast<char*>(key.data())};
    MDB_val v;
    rc = mdb_get(txn, dbi, &k, &v);
    if (rc == 0 && v.mv_size >= 8) {
        memcpy(&ts_out, v.mv_data, sizeof(ts_out));
    }
    mdb_txn_abort(txn);
    mdb_env_close(env);
    return rc;
}

/// Helper: count entries in an access LMDB.
static size_t access_db_count(const fs::path& access_dir) {
    MDB_env* env = nullptr;
    int rc = mdb_env_create(&env);
    if (rc) return 0;
    mdb_env_set_mapsize(env, 1ULL * 1024 * 1024 * 1024);
    rc = mdb_env_open(env, access_dir.c_str(), MDB_RDONLY, 0664);
    if (rc) { mdb_env_close(env); return 0; }

    MDB_txn* txn = nullptr;
    rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
    if (rc) { mdb_env_close(env); return 0; }

    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, nullptr, 0, &dbi);
    if (rc) { mdb_txn_abort(txn); mdb_env_close(env); return 0; }

    MDB_stat stat;
    rc = mdb_stat(txn, dbi, &stat);
    mdb_txn_abort(txn);
    mdb_env_close(env);
    return (rc == 0) ? stat.ms_entries : 0;
}

/// Helper: send access events to the daemon via datagram socket.
static void send_access_events(const fs::path& sock_path, const std::string& data) {
    int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (fd < 0) return;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path.c_str(), sizeof(addr.sun_path) - 1);

    sendto(fd, data.data(), data.size(), 0,
           reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    close(fd);
}

static void test_access_log() {
    std::cout << "\n=== Access log ===" << std::endl;

    auto root = make_temp_dir("p4cache-access");
    auto depot = root / "depot";
    auto storage = root / "storage";
    fs::create_directories(depot);
    fs::create_directories(storage);

    // ---- basic access log ----
    {
        TEST(access_log_basic);

        fs::remove_all(root / "state");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.access_log_enabled = true;
        cfg.access_batch_size = 10;   // Small batch for testing
        cfg.access_flush_interval_secs = 1;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        // Wait for access socket to be ready
        auto access_sock = cfg.state_dir / "access.sock";
        bool sock_ready = wait_for([&]{ return fs::exists(access_sock); }, 3000);
        ASSERT_TRUE(sock_ready, "access.sock should exist");

        // Send some access events
        std::string events = "dir1/file1.txt\ndir1/file2.txt\ndir2/file3.txt\n";
        send_access_events(access_sock, events);

        // Wait for the batch to flush (flush interval is 1s)
        // Sleep to allow receiver to get events and writer to flush
        auto access_dir = cfg.state_dir / "access";
        std::this_thread::sleep_for(std::chrono::seconds(3));

        // Verify via daemon's own access_db_entries() FIRST
        // (must be called before any external env opens the same LMDB dir,
        //  because opening a second env with different flags can invalidate
        //  the shared lock file's reader mutex for the first env)
        uint64_t db_entries = cache.access_db_entries();
        ASSERT_TRUE(db_entries >= 3,
                    "access_db_entries should be >= 3 (got " + std::to_string(db_entries) + ")");

        // Verify stats
        auto stats = cache.get_stats();
        ASSERT_TRUE(stats.access_events_received >= 3,
                    "should have >= 3 access events (got " + std::to_string(stats.access_events_received) + ")");
        ASSERT_TRUE(stats.access_batches_written >= 1,
                    "should have >= 1 batch written (got " + std::to_string(stats.access_batches_written) + ")");

        // Stop the daemon before opening external env handles for detailed verification
        cache.stop();

        size_t count = access_db_count(access_dir);
        ASSERT_TRUE(count >= 3, "access entries should be written (got " +
                    std::to_string(count) + ")");

        // Verify specific entries
        uint64_t ts = 0;
        int rc = access_db_get(access_dir, "dir1/file1.txt", ts);
        ASSERT_TRUE(rc == 0, "file1.txt should be in access DB");
        ASSERT_TRUE(ts > 0, "timestamp should be non-zero");

        rc = access_db_get(access_dir, "dir1/file2.txt", ts);
        ASSERT_TRUE(rc == 0, "file2.txt should be in access DB");

        rc = access_db_get(access_dir, "dir2/file3.txt", ts);
        ASSERT_TRUE(rc == 0, "file3.txt should be in access DB");

        PASS();
    }

    // ---- batch flush threshold ----
    {
        TEST(access_log_batch_flush);

        fs::remove_all(root / "state");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.access_log_enabled = true;
        cfg.access_batch_size = 50;     // Batch threshold
        cfg.access_flush_interval_secs = 60;  // Long interval — force flush via batch size
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        auto access_sock = cfg.state_dir / "access.sock";
        bool sock_ready = wait_for([&]{ return fs::exists(access_sock); }, 3000);
        ASSERT_TRUE(sock_ready, "access.sock should exist");

        // Send 100 events in batches to exceed threshold
        for (int batch = 0; batch < 10; ++batch) {
            std::string events;
            for (int i = 0; i < 10; ++i) {
                events += "batch" + std::to_string(batch) + "/file" + std::to_string(i) + ".dat\n";
            }
            send_access_events(access_sock, events);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        // Wait for entries to appear (batch threshold should trigger flush)
        // Use daemon's own access_db_entries() to avoid opening a second env
        bool flushed = wait_for([&]{
            return cache.access_db_entries() >= 50;
        }, 10000);
        ASSERT_TRUE(flushed, "batch threshold should trigger flush: got " +
                    std::to_string(cache.access_db_entries()));

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- access log disabled ----
    {
        TEST(access_log_disabled);

        fs::remove_all(root / "state");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.access_log_enabled = false;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        // Access socket should NOT exist
        auto access_sock = cfg.state_dir / "access.sock";
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ASSERT_TRUE(!fs::exists(access_sock), "access.sock should not exist when disabled");

        // Access directory should NOT exist
        auto access_dir = cfg.state_dir / "access";
        ASSERT_TRUE(!fs::exists(access_dir), "access dir should not exist when disabled");

        cache.stop();
        cache.wait();
        PASS();
    }

    // ---- access log deduplication ----
    {
        TEST(access_log_dedup);

        fs::remove_all(root / "state");

        CacheConfig cfg;
        cfg.depot_path = depot;
        cfg.primary.type = "nfs";
        cfg.primary.params["path"] = storage.string();
        cfg.state_dir = root / "state";
        cfg.stats_interval_secs = 0;
        cfg.max_cache_bytes = 1ULL * 1024 * 1024 * 1024;
        cfg.eviction_low_watermark = 800ULL * 1024 * 1024;
        cfg.access_log_enabled = true;
        cfg.access_batch_size = 10;
        cfg.access_flush_interval_secs = 1;
        cfg.apply_defaults();

        DepotCache cache(cfg);
        auto err = cache.start();
        ASSERT_EMPTY(err, "start");

        auto access_sock = cfg.state_dir / "access.sock";
        bool sock_ready = wait_for([&]{ return fs::exists(access_sock); }, 3000);
        ASSERT_TRUE(sock_ready, "access.sock should exist");

        // Send the same file multiple times
        std::string events = "same/file.txt\nsame/file.txt\nsame/file.txt\n";
        send_access_events(access_sock, events);

        // Use daemon's own access_db_entries() to avoid opening a second env
        // on the same directory (which corrupts the shared LMDB lock file)
        bool flushed = wait_for([&]{
            return cache.access_db_entries() >= 1;
        }, 5000);
        ASSERT_TRUE(flushed, "at least one entry should be written");

        // Should only have 1 entry (deduplicated)
        ASSERT_EQ(cache.access_db_entries(), (uint64_t)1, "duplicate paths should be deduplicated");

        cache.stop();
        cache.wait();
        PASS();
    }

    // Clean up
    fs::remove_all(root);
}

// ---------------------------------------------------------------------------
// 7. Access tool tests
// ---------------------------------------------------------------------------

/// Helper: write entries directly to an access LMDB for tool testing.
static void populate_access_db(const fs::path& access_dir,
                                const std::vector<std::pair<std::string, uint64_t>>& entries) {
    fs::create_directories(access_dir);

    MDB_env* env = nullptr;
    mdb_env_create(&env);
    mdb_env_set_mapsize(env, 1ULL * 1024 * 1024 * 1024);
    mdb_env_open(env, access_dir.c_str(), 0, 0664);

    MDB_txn* txn = nullptr;
    mdb_txn_begin(env, nullptr, 0, &txn);

    MDB_dbi dbi;
    mdb_dbi_open(txn, nullptr, MDB_CREATE, &dbi);

    for (auto& [path, ts] : entries) {
        uint64_t le_ts = ts;
        MDB_val k = {path.size(), const_cast<char*>(path.data())};
        MDB_val v = {sizeof(le_ts), &le_ts};
        mdb_put(txn, dbi, &k, &v, 0);
    }

    mdb_txn_commit(txn);
    mdb_env_close(env);
}

static void test_access_tool() {
    std::cout << "\n=== Access tool ===" << std::endl;

    auto root = make_temp_dir("p4cache-atool");
    auto access_dir = root / "access";

    // Populate a test database
    auto now = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    populate_access_db(access_dir, {
        {"dir1/file1.txt", now},
        {"dir1/file2.txt", now - 86400},     // 1 day ago
        {"dir2/file3.txt", now - 604800},     // 7 days ago
        {"dir2/sub/file4.txt", now - 2592000}, // 30 days ago
        {"other/file5.txt", now},
    });

    // ---- stat ----
    {
        TEST(access_tool_stat);
        auto count = access_db_count(access_dir);
        ASSERT_EQ(count, (size_t)5, "should have 5 entries");
        PASS();
    }

    // ---- get ----
    {
        TEST(access_tool_get);
        uint64_t ts = 0;
        int rc = access_db_get(access_dir, "dir1/file1.txt", ts);
        ASSERT_TRUE(rc == 0, "should find dir1/file1.txt");
        ASSERT_EQ(ts, now, "timestamp should match");

        rc = access_db_get(access_dir, "nonexistent.txt", ts);
        ASSERT_TRUE(rc == MDB_NOTFOUND, "nonexistent should return NOTFOUND");
        PASS();
    }

    // ---- prefix ----
    {
        TEST(access_tool_prefix);
        // Open DB and do a prefix scan like the tool would
        MDB_env* env = nullptr;
        mdb_env_create(&env);
        mdb_env_set_mapsize(env, 1ULL * 1024 * 1024 * 1024);
        mdb_env_open(env, access_dir.c_str(), MDB_RDONLY, 0664);

        MDB_txn* txn = nullptr;
        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);

        MDB_dbi dbi;
        mdb_dbi_open(txn, nullptr, 0, &dbi);

        MDB_cursor* cursor = nullptr;
        mdb_cursor_open(txn, dbi, &cursor);

        std::string prefix = "dir2/";
        MDB_val k = {prefix.size(), const_cast<char*>(prefix.data())};
        MDB_val v;
        std::vector<std::string> results;

        int rc = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
        while (rc == 0) {
            std::string path(static_cast<const char*>(k.mv_data), k.mv_size);
            if (path.compare(0, prefix.size(), prefix) != 0) break;
            results.push_back(path);
            rc = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        mdb_env_close(env);

        ASSERT_EQ(results.size(), (size_t)2, "dir2/ prefix should match 2 entries");
        PASS();
    }

    // ---- stale ----
    {
        TEST(access_tool_stale);
        // Scan for entries older than 2 days
        uint64_t threshold = now - 2 * 86400;

        MDB_env* env = nullptr;
        mdb_env_create(&env);
        mdb_env_set_mapsize(env, 1ULL * 1024 * 1024 * 1024);
        mdb_env_open(env, access_dir.c_str(), MDB_RDONLY, 0664);

        MDB_txn* txn = nullptr;
        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);

        MDB_dbi dbi;
        mdb_dbi_open(txn, nullptr, 0, &dbi);

        MDB_cursor* cursor = nullptr;
        mdb_cursor_open(txn, dbi, &cursor);

        MDB_val k, v;
        std::vector<std::string> stale;

        int rc2 = mdb_cursor_get(cursor, &k, &v, MDB_FIRST);
        while (rc2 == 0) {
            if (v.mv_size >= 8) {
                uint64_t ts;
                memcpy(&ts, v.mv_data, sizeof(ts));
                if (ts < threshold) {
                    stale.emplace_back(static_cast<const char*>(k.mv_data), k.mv_size);
                }
            }
            rc2 = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        mdb_env_close(env);

        // dir2/file3.txt (7 days ago) and dir2/sub/file4.txt (30 days ago) should be stale
        ASSERT_EQ(stale.size(), (size_t)2, "should have 2 stale entries");
        PASS();
    }

    fs::remove_all(root);
}

// ---------------------------------------------------------------------------
// 8. Azure key sanitization tests
// ---------------------------------------------------------------------------

static size_t count_segments(const std::string& s) {
    if (s.empty()) return 0;
    size_t count = 1;
    for (char c : s) {
        if (c == '/') ++count;
    }
    return count;
}

static void test_azure_key_sanitization() {
    std::cout << "\n=== Azure key sanitization ===" << std::endl;

    {
        TEST(normal_path_unchanged);
        auto result = sanitize_azure_key("foo/bar/file.txt");
        ASSERT_EQ(result, std::string("foo/bar/file.txt"), "normal path");
        PASS();
    }
    {
        TEST(backslash_normalized);
        auto result = sanitize_azure_key("foo\\bar\\file.txt");
        ASSERT_EQ(result, std::string("foo/bar/file.txt"), "backslash normalization");
        PASS();
    }
    {
        TEST(trailing_dot_encoded);
        auto result = sanitize_azure_key("foo/bar./file.");
        ASSERT_EQ(result, std::string("foo/bar%2E/file%2E"), "trailing dots");
        PASS();
    }
    {
        TEST(multiple_trailing_dots);
        auto result = sanitize_azure_key("foo/bar.../file");
        ASSERT_EQ(result, std::string("foo/bar..%2E/file"), "multiple trailing dots");
        PASS();
    }
    {
        TEST(control_chars_encoded);
        auto result = sanitize_azure_key(std::string("foo/\x01" "bar/\x7f" "baz"));
        ASSERT_EQ(result, std::string("foo/%01bar/%7Fbaz"), "control chars");
        PASS();
    }
    {
        TEST(empty_segments_removed);
        auto result = sanitize_azure_key("foo//bar///file.txt");
        ASSERT_EQ(result, std::string("foo/bar/file.txt"), "empty segments");
        PASS();
    }
    {
        TEST(trailing_slash_removed);
        auto result = sanitize_azure_key("foo/bar/");
        ASSERT_EQ(result, std::string("foo/bar"), "trailing slash");
        PASS();
    }
    {
        TEST(length_limit_enforced);
        // Build a path of 1100 characters: segments of "aaaa...a" separated by "/"
        std::string long_path;
        for (int i = 0; i < 110; ++i) {
            if (i > 0) long_path += '/';
            long_path += std::string(9, 'a');  // 110 segments * 10 chars each ≈ 1100
        }
        auto result = sanitize_azure_key(long_path);
        ASSERT_TRUE(result.size() <= 1024, "result exceeds 1024 chars: " + std::to_string(result.size()));
        ASSERT_TRUE(result.size() >= 1024, "result should be exactly 1024 chars: " + std::to_string(result.size()));
        PASS();
    }
    {
        TEST(segment_limit_enforced);
        // Build a path with 300 segments
        std::string many_segments;
        for (int i = 0; i < 300; ++i) {
            if (i > 0) many_segments += '/';
            many_segments += "s" + std::to_string(i);
        }
        auto result = sanitize_azure_key(many_segments);
        size_t seg_count = count_segments(result);
        ASSERT_TRUE(seg_count <= 254,
                     "too many segments: " + std::to_string(seg_count));
        PASS();
    }
    {
        TEST(mixed_issues);
        // Input: backslash, control char, trailing dot, trailing slash
        std::string input = std::string("foo\\.bar/\x01/baz./");
        auto result = sanitize_azure_key(input);
        // foo\.bar → foo/.bar (backslash becomes separator)
        // \x01 → %01 (control char encoded)
        // baz. → baz%2E (trailing dot encoded)
        // trailing slash → removed (empty segment)
        ASSERT_EQ(result, std::string("foo/.bar/%01/baz%2E"), "mixed issues");
        PASS();
    }
}

// ---------------------------------------------------------------------------
// 9. Metrics tests
// ---------------------------------------------------------------------------

static void test_metrics() {
    std::cout << "\n=== Metrics ===" << std::endl;

    auto tmpdir = make_temp_dir("p4cache-metrics");
    auto prom_path = tmpdir / "test.prom";

    {
        TEST(creates_prom_file);
        std::map<std::string, std::string> labels = {
            {"depot", "/test/depot"},
            {"mode", "readwrite"},
        };
        MetricsExporter exporter(prom_path, std::chrono::seconds(1), labels);
        exporter.start();

        // Wait for at least one write cycle
        bool created = wait_for([&]{ return fs::exists(prom_path); }, 5000);
        exporter.stop();

        ASSERT_TRUE(created, ".prom file should be created");
        auto content = read_file(prom_path);
        ASSERT_TRUE(!content.empty(), ".prom file should not be empty");
        // Verify it contains expected metric names
        ASSERT_TRUE(content.find("p4cache_uploads_total") != std::string::npos,
                     "should contain p4cache_uploads_total");
        ASSERT_TRUE(content.find("p4cache_files_dirty") != std::string::npos,
                     "should contain p4cache_files_dirty");
        ASSERT_TRUE(content.find("p4cache_upload_duration_seconds") != std::string::npos,
                     "should contain p4cache_upload_duration_seconds");
        PASS();
    }

    // Clean up for next test
    fs::remove(prom_path);

    {
        TEST(counter_increments_appear);
        std::map<std::string, std::string> labels = {
            {"depot", "/test"},
            {"mode", "readwrite"},
        };
        MetricsExporter exporter(prom_path, std::chrono::seconds(60), labels);

        // Increment some counters
        exporter.uploads_success().Increment();
        exporter.uploads_success().Increment();
        exporter.uploads_failure().Increment();
        exporter.upload_bytes_total().Increment(12345);
        exporter.shim_fetched().Increment();
        exporter.evictions_total().Increment(5);

        // Force a write (stop writes final snapshot)
        exporter.stop();

        auto content = read_file(prom_path);
        // Check that counter values appear
        ASSERT_TRUE(content.find("p4cache_uploads_total{") != std::string::npos,
                     "should contain uploads counter");
        // The success counter should show 2
        ASSERT_TRUE(content.find("result=\"success\"") != std::string::npos,
                     "should contain success label");
        PASS();
    }

    fs::remove(prom_path);

    {
        TEST(scoped_timer_records_duration);
        std::map<std::string, std::string> labels;
        MetricsExporter exporter(prom_path, std::chrono::seconds(60), labels);

        // Use ScopedTimer to record a duration
        {
            ScopedTimer timer(exporter.upload_duration());
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        exporter.stop();

        auto content = read_file(prom_path);
        // Histogram should have at least one observation
        ASSERT_TRUE(content.find("p4cache_upload_duration_seconds_count") != std::string::npos,
                     "should contain histogram count");
        // The count should be 1
        ASSERT_TRUE(content.find("p4cache_upload_duration_seconds_count 1") != std::string::npos,
                     "histogram count should be 1");
        PASS();
    }

    fs::remove(prom_path);

    {
        TEST(atomic_rename_no_partial_reads);
        std::map<std::string, std::string> labels;
        MetricsExporter exporter(prom_path, std::chrono::seconds(1), labels);

        exporter.uploads_success().Increment();
        exporter.start();

        // Wait for file to appear
        bool created = wait_for([&]{ return fs::exists(prom_path); }, 5000);
        ASSERT_TRUE(created, "file should exist");

        // Read the file multiple times quickly — should never see a partial/empty file
        // (the .tmp file is renamed atomically)
        bool saw_partial = false;
        for (int i = 0; i < 50; ++i) {
            auto content = read_file(prom_path);
            if (content.empty()) {
                saw_partial = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        exporter.stop();
        ASSERT_TRUE(!saw_partial, "should never see empty/partial .prom file");

        // Verify .tmp file does not persist
        auto tmp_path = prom_path;
        tmp_path += ".tmp";
        ASSERT_TRUE(!fs::exists(tmp_path), ".tmp file should not persist");
        PASS();
    }

    {
        TEST(constant_labels_present);
        fs::remove(prom_path);
        std::map<std::string, std::string> labels = {
            {"depot", "/mnt/depot"},
            {"mode", "readonly"},
        };
        MetricsExporter exporter(prom_path, std::chrono::seconds(60), labels);
        exporter.stop();

        auto content = read_file(prom_path);
        ASSERT_TRUE(content.find("depot=\"/mnt/depot\"") != std::string::npos,
                     "should contain depot label");
        ASSERT_TRUE(content.find("mode=\"readonly\"") != std::string::npos,
                     "should contain mode label");
        PASS();
    }

    fs::remove_all(tmpdir);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
    std::cout << "p4-cache test suite" << std::endl;
    std::cout << "===================" << std::endl;

    test_backend_config_validation();
    test_config_cli_parsing();
    test_config_json();
    test_config_defaults_and_validation();
    test_depot_cache_integration();
    test_access_log();
    test_access_tool();
    test_azure_key_sanitization();
    test_metrics();

    std::cout << "\n===================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, "
              << tests_failed << " failed" << std::endl;

    return tests_failed > 0 ? 1 : 0;
}
