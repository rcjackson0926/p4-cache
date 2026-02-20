#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace p4cache {

/// Configuration for a single storage backend (S3, Azure, GCS, or NFS).
struct BackendConfig {
    std::string type;  // "s3", "azure", "gcs", "nfs"
    std::map<std::string, std::string> params;  // Passed to StorageBackendFactory

    bool empty() const { return type.empty(); }

    /// Validate required fields for this backend type.
    /// Returns error message or empty string on success.
    std::string validate() const;
};

/// Configuration for the P4 cache daemon.
/// Supports read-write mode (primary servers) and read-only mode (replica servers).
struct CacheConfig {
    // Depot path to watch (NVMe mount point where P4d reads/writes)
    std::filesystem::path depot_path;

    // Read-only mode for replica servers.
    // When true: no upload workers, no FAN_CLOSE_WRITE monitoring.
    // Only caches reads with LRU eviction.
    bool read_only = false;

    // Cache limits
    uint64_t max_cache_bytes = 100ULL * 1024 * 1024 * 1024;       // 100 GB
    uint64_t eviction_low_watermark = 80ULL * 1024 * 1024 * 1024;  // 80 GB
    uint64_t eviction_target = 72ULL * 1024 * 1024 * 1024;         // 72 GB (90% of watermark)

    // Storage backends
    BackendConfig primary;     // Required: uploads go here, checked first on reads
    BackendConfig secondary;   // Optional: read-only fallback

    // Upload workers (ignored in read-only mode)
    size_t upload_threads = 8;
    size_t upload_concurrency = 16;  // Concurrent PUTs per thread

    // Restore workers (used in both read-write and read-only modes)
    size_t restore_threads = 16;

    // Manifest
    std::filesystem::path state_dir;  // Default: <depot_path>/.p4cache/

    // Startup
    bool skip_startup_scan = false;  // Skip scan_untracked_files() on startup

    // Tuning
    size_t upload_batch_size = 64;
    std::chrono::milliseconds upload_poll_interval{100};
    size_t stats_interval_secs = 60;

    // Daemon
    bool daemonize = false;
    bool verbose = false;
    std::filesystem::path pid_file;
    std::filesystem::path log_file;

    // Prometheus metrics (textfile collector)
    std::filesystem::path metrics_file;    // e.g. /var/lib/node_exporter/textfile/p4cache.prom
    size_t metrics_interval_secs = 15;

    // Access log (permanent LMDB database tracking last read time per file)
    bool access_log_enabled = true;
    size_t access_batch_size = 10000;
    size_t access_flush_interval_secs = 5;
    size_t access_sync_interval_secs = 60;
    uint64_t access_mapsize_gb = 512;
    std::filesystem::path access_db_path;  // Override: shared LMDB path (default: <state_dir>/access/)

    /// Parse configuration from command line arguments.
    /// Returns empty optional on error (prints usage to stderr).
    static std::optional<CacheConfig> from_args(int argc, char* argv[]);

    /// Load configuration from a JSON file, overlaying onto current values.
    bool load_json(const std::filesystem::path& path);

    /// Fill in defaults (state_dir, path_prefix) based on depot_path.
    void apply_defaults();

    /// Validate required fields. Returns error message or empty string.
    std::string validate() const;
};

}  // namespace p4cache
