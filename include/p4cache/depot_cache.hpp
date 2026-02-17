#pragma once

#include "p4cache/depot_watcher.hpp"
#include "p4cache/cache_config.hpp"

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

// Forward declarations
struct sqlite3;
struct sqlite3_stmt;

namespace meridian {
class StorageBackend;
class ThreadPool;
}  // namespace meridian

namespace p4cache {

/// Core depot cache engine.
///
/// Manages the SQLite manifest, upload workers (read-write mode only),
/// restore workers, LRU eviction, and the Unix socket shim server.
///
/// Supports a primary backend (read-write) and an optional secondary
/// backend (read-only fallback for restores).
///
/// In read-only mode (replica servers):
///   - No upload workers are started
///   - FAN_CLOSE_WRITE events are not monitored
///   - Files fetched from storage are cached locally with LRU eviction
///   - The shim server still handles cold-read requests
class DepotCache {
public:
    explicit DepotCache(const CacheConfig& config);
    ~DepotCache();

    DepotCache(const DepotCache&) = delete;
    DepotCache& operator=(const DepotCache&) = delete;

    /// Initialize the cache: open SQLite, create storage backends, start workers.
    /// Returns error message on failure, empty string on success.
    std::string start();

    /// Graceful shutdown: drain uploads, stop workers.
    void stop();

    /// Block until stop() is called (for daemon mode).
    void wait();

    // --- Callbacks for DepotWatcher ---

    /// Called on FAN_CLOSE_WRITE: record file as dirty, queue upload.
    /// Only used in read-write mode.
    void on_file_written(const std::filesystem::path& path);

    /// Called on FAN_OPEN_PERM: check if file is a 0-byte evicted stub.
    /// If so, restore from storage before allowing the open.
    /// @return true to allow the open, false to deny.
    bool on_file_open(const std::filesystem::path& path);

    // --- Shim server (Unix socket) ---

    /// Handle a FETCH request from the LD_PRELOAD shim.
    /// Fetches the file from storage if it exists, creates it on NVMe.
    /// @param relative_path Path relative to depot_path.
    /// @return "OK <size>", "NOTFOUND", or "ERROR <msg>".
    std::string fetch_for_shim(const std::string& relative_path);

    // --- Statistics ---

    struct Stats {
        uint64_t files_tracked = 0;
        uint64_t dirty_files = 0;
        uint64_t uploading_files = 0;
        uint64_t clean_files = 0;
        uint64_t evicted_files = 0;
        uint64_t cache_bytes = 0;
        uint64_t uploads_completed = 0;
        uint64_t uploads_failed = 0;
        uint64_t restores_completed = 0;
        uint64_t restores_failed = 0;
        uint64_t secondary_restores = 0;
        uint64_t evictions_performed = 0;
        uint64_t shim_fetches = 0;
        uint64_t shim_not_found = 0;
    };
    Stats get_stats() const;

    bool is_read_only() const { return config_.read_only; }

private:
    // SQLite manifest operations
    void init_manifest();
    void crash_recovery();
    void scan_untracked_files();
    uint64_t calculate_cache_bytes();

    // File state
    std::string relative_path(const std::filesystem::path& abs_path) const;
    std::string make_storage_key(const std::string& rel_path) const;

    // Upload workers (read-write mode only)
    void upload_worker_loop();
    bool upload_file(const std::string& rel_path, const std::string& storage_key);

    // Eviction
    void eviction_worker_loop();
    void maybe_trigger_eviction();

    // Restore
    bool restore_file(const std::string& rel_path, const std::string& storage_key);

    // In-flight fetch deduplication
    struct PendingFetch {
        std::shared_future<bool> result;
    };
    std::mutex pending_mutex_;
    std::unordered_map<std::string, PendingFetch> pending_fetches_;

    // Shim server
    void shim_server_loop();

    // Stats reporting
    void stats_reporter_loop();

    // Build a storage backend from config
    static std::unique_ptr<meridian::StorageBackend> build_backend(const BackendConfig& cfg);

    // Config
    CacheConfig config_;

    // SQLite manifest
    // Write operations use BEGIN IMMEDIATE for proper serialization.
    // WAL mode allows concurrent readers.
    std::mutex db_mutex_;  // Protects prepared statement usage
    sqlite3* db_ = nullptr;
    sqlite3_stmt* stmt_insert_ = nullptr;
    sqlite3_stmt* stmt_update_state_ = nullptr;
    sqlite3_stmt* stmt_get_entry_ = nullptr;
    sqlite3_stmt* stmt_get_dirty_batch_ = nullptr;
    sqlite3_stmt* stmt_get_evict_candidates_ = nullptr;
    sqlite3_stmt* stmt_update_access_ = nullptr;
    sqlite3_stmt* stmt_get_stats_ = nullptr;

    // Storage backends
    std::unique_ptr<meridian::StorageBackend> primary_;     // read-write
    std::unique_ptr<meridian::StorageBackend> secondary_;   // read-only fallback, may be null

    // Workers
    std::unique_ptr<meridian::ThreadPool> upload_pool_;   // null in read-only mode
    std::unique_ptr<meridian::ThreadPool> restore_pool_;

    // Threads
    std::vector<std::thread> upload_threads_;
    std::thread eviction_thread_;
    std::thread shim_thread_;
    std::thread stats_thread_;

    // Synchronization
    std::atomic<bool> running_{false};
    std::mutex upload_cv_mutex_;
    std::condition_variable upload_cv_;
    std::mutex eviction_cv_mutex_;
    std::condition_variable eviction_cv_;
    std::condition_variable wait_cv_;
    std::mutex wait_mutex_;

    // Cache size tracking
    std::atomic<uint64_t> cache_bytes_{0};

    // Shim server socket
    int shim_sock_fd_ = -1;

    // Stats
    mutable std::mutex stats_mutex_;
    Stats stats_;
};

}  // namespace p4cache
