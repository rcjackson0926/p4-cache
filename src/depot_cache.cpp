#include "meridian/proxy/depot_cache.hpp"
#include "meridian/storage/backend.hpp"
#include "meridian/core/thread_pool.hpp"

#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sqlite3.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace meridian::proxy {

namespace {

constexpr const char* MANIFEST_SCHEMA = R"(
CREATE TABLE IF NOT EXISTS cache_entries (
    path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    storage_key TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'dirty',
    last_access INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    etag TEXT
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_evict
    ON cache_entries(state, last_access) WHERE state = 'clean';

CREATE INDEX IF NOT EXISTS idx_drain
    ON cache_entries(state, created_at) WHERE state = 'dirty';
)";

int64_t now_epoch() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void log_info(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
void log_info(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    va_end(args);
    fputc('\n', stdout);
    fflush(stdout);
}

void log_error(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
void log_error(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    fprintf(stderr, "ERROR: ");
    vfprintf(stderr, fmt, args);
    va_end(args);
    fputc('\n', stderr);
}

// Execute a SQL statement with retry on SQLITE_BUSY
bool sql_exec(sqlite3* db, const char* sql) {
    for (int attempt = 0; attempt < 10; ++attempt) {
        char* err = nullptr;
        int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
        if (rc == SQLITE_OK) return true;
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            if (err) sqlite3_free(err);
            std::this_thread::sleep_for(std::chrono::milliseconds(10 * (attempt + 1)));
            continue;
        }
        if (err) {
            log_error("SQL error: %s (rc=%d)", err, rc);
            sqlite3_free(err);
        }
        return false;
    }
    log_error("SQL timed out after retries");
    return false;
}

// Step a prepared statement with SQLITE_BUSY retry
int sql_step_retry(sqlite3_stmt* stmt) {
    for (int attempt = 0; attempt < 10; ++attempt) {
        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) return rc;
        sqlite3_reset(stmt);
        std::this_thread::sleep_for(std::chrono::milliseconds(10 * (attempt + 1)));
    }
    return SQLITE_BUSY;
}

}  // namespace

// --- Build backend from config ---

std::unique_ptr<StorageBackend> DepotCache::build_backend(const BackendConfig& cfg) {
    // Map "nfs" to the "local" backend type that StorageBackendFactory expects
    std::string factory_type = (cfg.type == "nfs") ? "local" : cfg.type;
    return StorageBackendFactory::create(factory_type, cfg.params);
}

DepotCache::DepotCache(const CacheConfig& config) : config_(config) {}

DepotCache::~DepotCache() {
    stop();

    // Finalize prepared statements
    if (stmt_insert_) sqlite3_finalize(stmt_insert_);
    if (stmt_update_state_) sqlite3_finalize(stmt_update_state_);
    if (stmt_get_entry_) sqlite3_finalize(stmt_get_entry_);
    if (stmt_get_dirty_batch_) sqlite3_finalize(stmt_get_dirty_batch_);
    if (stmt_get_evict_candidates_) sqlite3_finalize(stmt_get_evict_candidates_);
    if (stmt_update_access_) sqlite3_finalize(stmt_update_access_);
    if (stmt_get_stats_) sqlite3_finalize(stmt_get_stats_);

    if (db_) {
        sqlite3_exec(db_, "PRAGMA wal_checkpoint(TRUNCATE)", nullptr, nullptr, nullptr);
        sqlite3_close(db_);
    }

    if (shim_sock_fd_ >= 0) {
        close(shim_sock_fd_);
        auto sock_path = config_.state_dir / "shim.sock";
        unlink(sock_path.c_str());
    }
}

std::string DepotCache::start() {
    // Validate config
    auto err = config_.validate();
    if (!err.empty()) return err;

    // Create state directory
    std::error_code ec;
    std::filesystem::create_directories(config_.state_dir, ec);
    if (ec) return "Failed to create state_dir: " + ec.message();

    // Initialize SQLite manifest
    try {
        init_manifest();
    } catch (const std::exception& e) {
        return std::string("Failed to init manifest: ") + e.what();
    }

    // Create storage backends via unified factory
    primary_ = build_backend(config_.primary);
    if (!config_.secondary.empty()) {
        secondary_ = build_backend(config_.secondary);
    }

    // Crash recovery
    crash_recovery();
    cache_bytes_ = calculate_cache_bytes();

    log_info("Cache initialized: %lu bytes tracked, read-only=%s",
             cache_bytes_.load(), config_.read_only ? "true" : "false");

    // Scan for untracked files (files written while daemon was down)
    if (!config_.read_only) {
        scan_untracked_files();
    }

    running_ = true;

    // Start restore pool (used in both read-write and read-only modes)
    restore_pool_ = std::make_unique<ThreadPool>(config_.restore_threads);

    // Start upload coordinator (read-write mode only).
    // Single coordinator thread fetches dirty batches and dispatches to upload pool.
    if (!config_.read_only) {
        upload_pool_ = std::make_unique<ThreadPool>(config_.upload_concurrency);
        upload_threads_.emplace_back(&DepotCache::upload_worker_loop, this);
    }

    // Start eviction thread
    eviction_thread_ = std::thread(&DepotCache::eviction_worker_loop, this);

    // Start shim Unix socket server
    shim_thread_ = std::thread(&DepotCache::shim_server_loop, this);

    // Start stats reporter
    if (config_.stats_interval_secs > 0) {
        stats_thread_ = std::thread(&DepotCache::stats_reporter_loop, this);
    }

    return {};
}

void DepotCache::stop() {
    if (!running_.exchange(false)) return;

    log_info("Shutting down cache...");

    // Wake up all waiting threads
    upload_cv_.notify_all();
    eviction_cv_.notify_all();

    // Close shim socket to unblock accept()
    if (shim_sock_fd_ >= 0) {
        shutdown(shim_sock_fd_, SHUT_RDWR);
        close(shim_sock_fd_);
        shim_sock_fd_ = -1;
        auto sock_path = config_.state_dir / "shim.sock";
        unlink(sock_path.c_str());
    }

    // Join upload threads
    for (auto& t : upload_threads_) {
        if (t.joinable()) t.join();
    }
    upload_threads_.clear();

    if (eviction_thread_.joinable()) eviction_thread_.join();
    if (shim_thread_.joinable()) shim_thread_.join();
    if (stats_thread_.joinable()) stats_thread_.join();

    // Shutdown thread pools
    if (upload_pool_) upload_pool_->shutdown(true);
    if (restore_pool_) restore_pool_->shutdown(true);

    // Signal waiters
    {
        std::lock_guard lock(wait_mutex_);
    }
    wait_cv_.notify_all();

    log_info("Cache stopped");
}

void DepotCache::wait() {
    std::unique_lock lock(wait_mutex_);
    wait_cv_.wait(lock, [this] { return !running_.load(); });
}

// --- Manifest ---

void DepotCache::init_manifest() {
    auto db_path = config_.state_dir / "manifest.db";
    int rc = sqlite3_open(db_path.c_str(), &db_);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Cannot open manifest: " + std::string(sqlite3_errmsg(db_)));
    }

    // WAL mode for concurrent readers
    sql_exec(db_, "PRAGMA journal_mode=WAL");
    sql_exec(db_, "PRAGMA synchronous=NORMAL");
    sql_exec(db_, "PRAGMA cache_size=-65536");  // 64 MB
    sql_exec(db_, "PRAGMA busy_timeout=5000");
    sql_exec(db_, "PRAGMA wal_autocheckpoint=10000");
    sql_exec(db_, MANIFEST_SCHEMA);

    // Prepare statements
    sqlite3_prepare_v2(db_,
        "INSERT OR REPLACE INTO cache_entries (path, size, storage_key, state, last_access, created_at, etag) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        -1, &stmt_insert_, nullptr);

    sqlite3_prepare_v2(db_,
        "UPDATE cache_entries SET state = ?2, size = ?3 WHERE path = ?1",
        -1, &stmt_update_state_, nullptr);

    sqlite3_prepare_v2(db_,
        "SELECT path, size, storage_key, state, last_access, etag FROM cache_entries WHERE path = ?1",
        -1, &stmt_get_entry_, nullptr);

    sqlite3_prepare_v2(db_,
        "SELECT path, storage_key FROM cache_entries WHERE state = 'dirty' ORDER BY created_at ASC LIMIT ?1",
        -1, &stmt_get_dirty_batch_, nullptr);

    sqlite3_prepare_v2(db_,
        "SELECT path, size FROM cache_entries WHERE state = 'clean' ORDER BY last_access ASC LIMIT ?1",
        -1, &stmt_get_evict_candidates_, nullptr);

    sqlite3_prepare_v2(db_,
        "UPDATE cache_entries SET last_access = ?2 WHERE path = ?1",
        -1, &stmt_update_access_, nullptr);

    sqlite3_prepare_v2(db_,
        "SELECT state, COUNT(*), COALESCE(SUM(size), 0) FROM cache_entries GROUP BY state",
        -1, &stmt_get_stats_, nullptr);
}

void DepotCache::crash_recovery() {
    // Reset any 'uploading' entries back to 'dirty' (may not have completed)
    sql_exec(db_, "UPDATE cache_entries SET state = 'dirty' WHERE state = 'uploading'");

    log_info("Crash recovery: reset uploading entries to dirty");
}

void DepotCache::scan_untracked_files() {
    // Walk depot directory for files not in the manifest
    size_t found = 0;
    auto now = now_epoch();

    sql_exec(db_, "BEGIN TRANSACTION");
    for (auto& entry : std::filesystem::recursive_directory_iterator(
             config_.depot_path,
             std::filesystem::directory_options::skip_permission_denied)) {
        if (!entry.is_regular_file()) continue;

        auto path = entry.path();
        auto rel = relative_path(path);

        // Skip .p4cache directory
        if (rel.compare(0, 9, ".p4cache/") == 0 || rel == ".p4cache") continue;

        // Check if already tracked
        sqlite3_reset(stmt_get_entry_);
        sqlite3_bind_text(stmt_get_entry_, 1, rel.c_str(), -1, SQLITE_TRANSIENT);
        int rc = sql_step_retry(stmt_get_entry_);
        if (rc == SQLITE_ROW) continue;  // Already tracked

        // New untracked file — register as dirty
        auto file_size = entry.file_size();
        auto storage_key = make_storage_key(rel);

        sqlite3_reset(stmt_insert_);
        sqlite3_bind_text(stmt_insert_, 1, rel.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt_insert_, 2, static_cast<int64_t>(file_size));
        sqlite3_bind_text(stmt_insert_, 3, storage_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt_insert_, 4, "dirty", -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt_insert_, 5, now);
        sqlite3_bind_int64(stmt_insert_, 6, now);
        sqlite3_bind_null(stmt_insert_, 7);
        sql_step_retry(stmt_insert_);

        cache_bytes_ += file_size;
        ++found;
    }
    sql_exec(db_, "COMMIT");

    if (found > 0) {
        log_info("Found %zu untracked files, registered as dirty", found);
    }
}

uint64_t DepotCache::calculate_cache_bytes() {
    uint64_t total = 0;

    sqlite3_stmt* stmt = nullptr;
    sqlite3_prepare_v2(db_,
        "SELECT COALESCE(SUM(size), 0) FROM cache_entries WHERE state != 'evicted'",
        -1, &stmt, nullptr);
    if (sql_step_retry(stmt) == SQLITE_ROW) {
        total = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return total;
}

// --- Path helpers ---

std::string DepotCache::relative_path(const std::filesystem::path& abs_path) const {
    auto rel = std::filesystem::relative(abs_path, config_.depot_path);
    return rel.string();
}

std::string DepotCache::make_storage_key(const std::string& rel_path) const {
    // StorageBackend implementations prepend the configured path_prefix via make_key(),
    // so we just return the relative path as-is.
    return rel_path;
}

// --- Callbacks ---

void DepotCache::on_file_written(const std::filesystem::path& path) {
    if (config_.read_only) return;  // Should not happen, but guard

    auto rel = relative_path(path);
    auto storage_key = make_storage_key(rel);
    auto now = now_epoch();

    std::error_code ec;
    auto file_size = std::filesystem::file_size(path, ec);
    if (ec) return;  // File may have been deleted already

    // Insert or update in manifest
    int64_t old_size = 0;
    {
        std::lock_guard<std::mutex> db_lock(db_mutex_);
        sqlite3_reset(stmt_get_entry_);
        sqlite3_bind_text(stmt_get_entry_, 1, rel.c_str(), -1, SQLITE_TRANSIENT);
        if (sql_step_retry(stmt_get_entry_) == SQLITE_ROW) {
            old_size = sqlite3_column_int64(stmt_get_entry_, 1);
        }

        sqlite3_reset(stmt_insert_);
        sqlite3_bind_text(stmt_insert_, 1, rel.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt_insert_, 2, static_cast<int64_t>(file_size));
        sqlite3_bind_text(stmt_insert_, 3, storage_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt_insert_, 4, "dirty", -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt_insert_, 5, now);
        sqlite3_bind_int64(stmt_insert_, 6, now);
        sqlite3_bind_null(stmt_insert_, 7);
        sql_step_retry(stmt_insert_);
    }

    cache_bytes_ -= old_size;
    cache_bytes_ += file_size;

    // Wake up the upload coordinator
    upload_cv_.notify_one();

    // Check if we need to evict
    maybe_trigger_eviction();
}

bool DepotCache::on_file_open(const std::filesystem::path& path) {
    auto rel = relative_path(path);

    // Check file size first (no DB access needed for warm reads)
    std::error_code ec;
    auto file_size = std::filesystem::file_size(path, ec);

    // Update LRU timestamp
    {
        std::lock_guard<std::mutex> db_lock(db_mutex_);
        auto now = now_epoch();
        sqlite3_reset(stmt_update_access_);
        sqlite3_bind_text(stmt_update_access_, 1, rel.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt_update_access_, 2, now);
        sql_step_retry(stmt_update_access_);
    }

    if (ec || file_size > 0) {
        return true;  // Normal file, allow immediately
    }

    // 0-byte file — check manifest for evicted state
    std::string key_str;
    std::string rel_str(rel);
    {
        std::lock_guard<std::mutex> db_lock(db_mutex_);
        sqlite3_reset(stmt_get_entry_);
        sqlite3_bind_text(stmt_get_entry_, 1, rel.c_str(), -1, SQLITE_TRANSIENT);
        int rc = sql_step_retry(stmt_get_entry_);
        if (rc != SQLITE_ROW) {
            return true;  // Not tracked — might be legitimately empty
        }

        const char* state = reinterpret_cast<const char*>(sqlite3_column_text(stmt_get_entry_, 3));
        if (!state || strcmp(state, "evicted") != 0) {
            return true;  // Not evicted — allow
        }

        const char* stored_key = reinterpret_cast<const char*>(sqlite3_column_text(stmt_get_entry_, 2));
        if (!stored_key) return true;
        key_str = stored_key;
    }

    if (config_.verbose) {
        log_info("Restoring evicted file: %s", rel_str.c_str());
    }

    // Restore from storage synchronously (blocks the open until ready)
    if (restore_file(rel_str, key_str)) {
        return true;  // Restored successfully, allow the open
    }

    // Restore failed — allow the open anyway
    log_error("Failed to restore evicted file: %s", rel_str.c_str());
    return true;
}

// --- Upload workers ---

void DepotCache::upload_worker_loop() {
    while (running_.load(std::memory_order_relaxed)) {
        // Wait for work
        {
            std::unique_lock lock(upload_cv_mutex_);
            upload_cv_.wait_for(lock, config_.upload_poll_interval);
        }
        if (!running_.load(std::memory_order_relaxed)) break;

        // Fetch a batch of dirty entries and mark them as uploading
        struct DirtyEntry {
            std::string path;
            std::string storage_key;
        };
        std::vector<DirtyEntry> batch;

        {
            std::lock_guard<std::mutex> db_lock(db_mutex_);

            sqlite3_reset(stmt_get_dirty_batch_);
            sqlite3_bind_int(stmt_get_dirty_batch_, 1, static_cast<int>(config_.upload_batch_size));
            while (sql_step_retry(stmt_get_dirty_batch_) == SQLITE_ROW) {
                auto col0 = sqlite3_column_text(stmt_get_dirty_batch_, 0);
                auto col1 = sqlite3_column_text(stmt_get_dirty_batch_, 1);
                if (!col0 || !col1) continue;
                DirtyEntry e;
                e.path = reinterpret_cast<const char*>(col0);
                e.storage_key = reinterpret_cast<const char*>(col1);
                batch.push_back(std::move(e));
            }

            if (!batch.empty()) {
                // Mark batch as 'uploading'
                sqlite3_stmt* mark_stmt = nullptr;
                sqlite3_prepare_v2(db_,
                    "UPDATE cache_entries SET state = 'uploading' WHERE path = ?1 AND state = 'dirty'",
                    -1, &mark_stmt, nullptr);

                sql_exec(db_, "BEGIN IMMEDIATE");
                for (auto& e : batch) {
                    sqlite3_reset(mark_stmt);
                    sqlite3_bind_text(mark_stmt, 1, e.path.c_str(), -1, SQLITE_TRANSIENT);
                    sql_step_retry(mark_stmt);
                }
                sql_exec(db_, "COMMIT");
                sqlite3_finalize(mark_stmt);
            }
        }

        if (batch.empty()) continue;

        // Upload concurrently via thread pool
        std::vector<std::future<bool>> futures;
        for (auto& e : batch) {
            auto path = e.path;
            auto key = e.storage_key;
            futures.push_back(upload_pool_->submit([this, path, key]() {
                return upload_file(path, key);
            }));
        }

        // Wait for all uploads to complete
        std::vector<bool> results(futures.size());
        for (size_t i = 0; i < futures.size(); ++i) {
            try {
                results[i] = futures[i].get();
            } catch (...) {
                results[i] = false;
            }
        }

        // Update states in DB
        {
            std::lock_guard<std::mutex> db_lock(db_mutex_);
            sqlite3_stmt* result_stmt = nullptr;
            sqlite3_prepare_v2(db_,
                "UPDATE cache_entries SET state = ?2 WHERE path = ?1",
                -1, &result_stmt, nullptr);

            sql_exec(db_, "BEGIN IMMEDIATE");
            for (size_t i = 0; i < results.size(); ++i) {
                sqlite3_reset(result_stmt);
                sqlite3_bind_text(result_stmt, 1, batch[i].path.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(result_stmt, 2, results[i] ? "clean" : "dirty", -1, SQLITE_STATIC);
                sql_step_retry(result_stmt);

                std::lock_guard lock(stats_mutex_);
                if (results[i]) {
                    stats_.uploads_completed++;
                } else {
                    stats_.uploads_failed++;
                }
            }
            sql_exec(db_, "COMMIT");
            sqlite3_finalize(result_stmt);
        }

        // WAL checkpoint periodically
        sqlite3_wal_checkpoint_v2(db_, nullptr, SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
    }
}

bool DepotCache::upload_file(const std::string& rel_path, const std::string& storage_key) {
    auto full_path = config_.depot_path / rel_path;

    // Read file content
    std::ifstream ifs(full_path, std::ios::binary);
    if (!ifs) {
        log_error("Upload: cannot read %s", rel_path.c_str());
        return false;
    }

    std::vector<uint8_t> data((std::istreambuf_iterator<char>(ifs)),
                              std::istreambuf_iterator<char>());
    ifs.close();

    // Upload to primary backend
    auto put_result = primary_->put(storage_key, std::span<const uint8_t>(data), {});
    if (put_result.success) {
        if (config_.verbose) {
            log_info("Uploaded: %s (%zu bytes)", rel_path.c_str(), data.size());
        }
    } else {
        log_error("Upload failed: %s (key=%s, %zu bytes): %s",
                  rel_path.c_str(), storage_key.c_str(), data.size(),
                  put_result.error_message.c_str());
    }
    return put_result.success;
}

// --- Eviction ---

void DepotCache::maybe_trigger_eviction() {
    if (cache_bytes_.load() > config_.eviction_low_watermark) {
        eviction_cv_.notify_one();
    }
}

void DepotCache::eviction_worker_loop() {
    while (running_.load(std::memory_order_relaxed)) {
        {
            std::unique_lock lock(eviction_cv_mutex_);
            eviction_cv_.wait_for(lock, std::chrono::seconds(10), [this] {
                return !running_.load() || cache_bytes_.load() > config_.eviction_low_watermark;
            });
        }
        if (!running_.load(std::memory_order_relaxed)) break;

        if (cache_bytes_.load() <= config_.eviction_low_watermark) continue;

        log_info("Eviction started: cache=%lu bytes, target=%lu bytes",
                 cache_bytes_.load(), config_.eviction_target);

        while (running_.load() && cache_bytes_.load() > config_.eviction_target) {
            // Fetch candidates: oldest clean files
            struct EvictCandidate {
                std::string path;
                int64_t size;
            };
            std::vector<EvictCandidate> candidates;

            {
                std::lock_guard<std::mutex> db_lock(db_mutex_);
                sqlite3_reset(stmt_get_evict_candidates_);
                sqlite3_bind_int(stmt_get_evict_candidates_, 1, 100);
                while (sql_step_retry(stmt_get_evict_candidates_) == SQLITE_ROW) {
                    EvictCandidate c;
                    c.path = reinterpret_cast<const char*>(
                        sqlite3_column_text(stmt_get_evict_candidates_, 0));
                    c.size = sqlite3_column_int64(stmt_get_evict_candidates_, 1);
                    candidates.push_back(std::move(c));
                }
            }

            if (candidates.empty()) {
                log_info("No clean files to evict, waiting for uploads");
                break;
            }

            // Evict: truncate files to 0 bytes, then update DB
            std::vector<EvictCandidate> evicted;
            for (auto& c : candidates) {
                auto full_path = config_.depot_path / c.path;
                if (truncate(full_path.c_str(), 0) == 0) {
                    evicted.push_back(c);
                    cache_bytes_ -= c.size;
                    {
                        std::lock_guard lock(stats_mutex_);
                        stats_.evictions_performed++;
                    }
                    if (config_.verbose) {
                        log_info("Evicted: %s (%ld bytes)", c.path.c_str(), c.size);
                    }
                }
            }

            if (!evicted.empty()) {
                std::lock_guard<std::mutex> db_lock(db_mutex_);
                sqlite3_stmt* evict_stmt = nullptr;
                sqlite3_prepare_v2(db_,
                    "UPDATE cache_entries SET state = 'evicted', size = 0 WHERE path = ?1 AND state = 'clean'",
                    -1, &evict_stmt, nullptr);

                sql_exec(db_, "BEGIN IMMEDIATE");
                for (auto& c : evicted) {
                    sqlite3_reset(evict_stmt);
                    sqlite3_bind_text(evict_stmt, 1, c.path.c_str(), -1, SQLITE_TRANSIENT);
                    sql_step_retry(evict_stmt);
                }
                sql_exec(db_, "COMMIT");
                sqlite3_finalize(evict_stmt);
            }
        }

        log_info("Eviction complete: cache=%lu bytes", cache_bytes_.load());
    }
}

// --- Restore ---

bool DepotCache::restore_file(const std::string& rel_path, const std::string& storage_key) {
    auto full_path = config_.depot_path / rel_path;

    // Deduplicate concurrent fetches of the same file
    std::shared_future<bool> future;
    {
        std::lock_guard lock(pending_mutex_);
        auto it = pending_fetches_.find(rel_path);
        if (it != pending_fetches_.end()) {
            future = it->second.result;
        } else {
            auto promise = std::make_shared<std::promise<bool>>();
            future = promise->get_future().share();
            pending_fetches_[rel_path] = {future};

            // Launch restore in restore pool
            std::string path_copy = rel_path;
            std::string key_copy = storage_key;
            restore_pool_->execute([this, path_copy, key_copy, promise]() {
                bool ok = false;
                bool from_secondary = false;
                try {
                    auto get_result = primary_->get(key_copy, {});
                    if (!get_result.success && secondary_) {
                        get_result = secondary_->get(key_copy, {});
                        if (get_result.success) from_secondary = true;
                    }

                    if (get_result.success) {
                        auto full = config_.depot_path / path_copy;

                        // Create parent directories if needed
                        std::filesystem::create_directories(full.parent_path());

                        // Write content
                        std::ofstream ofs(full, std::ios::binary | std::ios::trunc);
                        if (ofs) {
                            ofs.write(reinterpret_cast<const char*>(get_result.data.data()), get_result.data.size());
                            ofs.close();
                            ok = ofs.good();
                        }

                        if (ok) {
                            // Update manifest: state=clean, size=actual
                            std::lock_guard<std::mutex> db_lock(db_mutex_);
                            sqlite3_stmt* restore_stmt = nullptr;
                            sqlite3_prepare_v2(db_,
                                "UPDATE cache_entries SET state = 'clean', size = ?2, last_access = ?3 "
                                "WHERE path = ?1",
                                -1, &restore_stmt, nullptr);
                            sqlite3_bind_text(restore_stmt, 1, path_copy.c_str(), -1, SQLITE_TRANSIENT);
                            sqlite3_bind_int64(restore_stmt, 2, static_cast<int64_t>(get_result.data.size()));
                            sqlite3_bind_int64(restore_stmt, 3, now_epoch());
                            sql_step_retry(restore_stmt);
                            sqlite3_finalize(restore_stmt);

                            cache_bytes_ += get_result.data.size();
                        }
                    }
                } catch (const std::exception& e) {
                    log_error("Restore failed for %s: %s", path_copy.c_str(), e.what());
                }

                {
                    std::lock_guard lock(stats_mutex_);
                    if (ok) {
                        stats_.restores_completed++;
                        if (from_secondary) stats_.secondary_restores++;
                    } else {
                        stats_.restores_failed++;
                    }
                }

                promise->set_value(ok);

                // Remove from pending map
                {
                    std::lock_guard lock2(pending_mutex_);
                    pending_fetches_.erase(path_copy);
                }
            });
        }
    }

    // Wait for the restore to complete (blocks the fanotify FAN_OPEN_PERM response)
    return future.get();
}

// --- Shim server ---

std::string DepotCache::fetch_for_shim(const std::string& relative_path) {
    // Check if file already exists on NVMe (race: another thread may have created it)
    auto full_path = config_.depot_path / relative_path;
    {
        std::error_code ec;
        if (std::filesystem::exists(full_path, ec) && !ec) {
            auto sz = std::filesystem::file_size(full_path, ec);
            if (!ec && sz > 0) {
                return "OK " + std::to_string(sz);
            }
        }
    }

    // Check manifest first for known storage key
    std::string storage_key;
    {
        std::lock_guard<std::mutex> db_lock(db_mutex_);
        sqlite3_reset(stmt_get_entry_);
        sqlite3_bind_text(stmt_get_entry_, 1, relative_path.c_str(), -1, SQLITE_TRANSIENT);
        int rc = sql_step_retry(stmt_get_entry_);
        if (rc == SQLITE_ROW) {
            storage_key = reinterpret_cast<const char*>(sqlite3_column_text(stmt_get_entry_, 2));
        } else {
            storage_key = make_storage_key(relative_path);
        }
    }

    // Try to fetch from primary, then secondary (no DB lock held during network I/O)
    auto get_result = primary_->get(storage_key, {});
    if (!get_result.success && secondary_) {
        get_result = secondary_->get(storage_key, {});
    }
    if (!get_result.success) {
        std::lock_guard lock(stats_mutex_);
        stats_.shim_not_found++;
        return "NOTFOUND";
    }

    // Create parent directories
    std::filesystem::create_directories(full_path.parent_path());

    // Write the file
    std::ofstream ofs(full_path, std::ios::binary | std::ios::trunc);
    if (!ofs) {
        return "ERROR cannot create file";
    }
    ofs.write(reinterpret_cast<const char*>(get_result.data.data()), get_result.data.size());
    ofs.close();
    if (!ofs.good()) {
        return "ERROR write failed";
    }

    // Register in manifest as clean
    auto now = now_epoch();
    {
        std::lock_guard<std::mutex> db_lock(db_mutex_);
        sqlite3_reset(stmt_insert_);
        sqlite3_bind_text(stmt_insert_, 1, relative_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt_insert_, 2, static_cast<int64_t>(get_result.data.size()));
        sqlite3_bind_text(stmt_insert_, 3, storage_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt_insert_, 4, "clean", -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt_insert_, 5, now);
        sqlite3_bind_int64(stmt_insert_, 6, now);
        sqlite3_bind_null(stmt_insert_, 7);
        sql_step_retry(stmt_insert_);
    }

    cache_bytes_ += get_result.data.size();
    {
        std::lock_guard lock(stats_mutex_);
        stats_.shim_fetches++;
    }

    maybe_trigger_eviction();
    return "OK " + std::to_string(get_result.data.size());
}

void DepotCache::shim_server_loop() {
    auto sock_path = config_.state_dir / "shim.sock";

    // Remove stale socket
    unlink(sock_path.c_str());

    shim_sock_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (shim_sock_fd_ < 0) {
        log_error("Failed to create shim socket: %s", strerror(errno));
        return;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(shim_sock_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        log_error("Failed to bind shim socket: %s", strerror(errno));
        return;
    }

    if (listen(shim_sock_fd_, 64) < 0) {
        log_error("Failed to listen on shim socket: %s", strerror(errno));
        return;
    }

    // Make socket world-accessible (P4d runs as different user)
    chmod(sock_path.c_str(), 0666);

    log_info("Shim server listening on %s", sock_path.c_str());

    while (running_.load(std::memory_order_relaxed)) {
        struct sockaddr_un client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(shim_sock_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) continue;
            if (!running_.load()) break;
            log_error("Shim accept failed: %s", strerror(errno));
            continue;
        }

        // Set read timeout
        struct timeval tv;
        tv.tv_sec = 30;
        tv.tv_usec = 0;
        setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        // Handle request in restore pool
        restore_pool_->execute([this, client_fd]() {
            char buf[4096];
            ssize_t n = read(client_fd, buf, sizeof(buf) - 1);
            if (n <= 0) {
                close(client_fd);
                return;
            }
            buf[n] = '\0';

            // Parse "FETCH <path>\n"
            std::string request(buf);
            std::string response;

            if (request.size() > 6 && request.compare(0, 6, "FETCH ") == 0) {
                auto path = request.substr(6);
                // Trim trailing newline
                while (!path.empty() && (path.back() == '\n' || path.back() == '\r')) {
                    path.pop_back();
                }
                response = fetch_for_shim(path);
            } else {
                response = "ERROR unknown command";
            }

            response += "\n";
            (void)write(client_fd, response.c_str(), response.size());
            close(client_fd);
        });
    }
}

// --- Stats ---

DepotCache::Stats DepotCache::get_stats() const {
    std::lock_guard lock(stats_mutex_);
    return stats_;
}

void DepotCache::stats_reporter_loop() {
    while (running_.load(std::memory_order_relaxed)) {
        for (size_t i = 0; i < config_.stats_interval_secs && running_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (!running_.load()) break;

        // Query manifest stats
        uint64_t dirty = 0, uploading = 0, clean = 0, evicted = 0, total = 0;
        {
            std::lock_guard<std::mutex> db_lock(db_mutex_);
            sqlite3_reset(stmt_get_stats_);
            while (sql_step_retry(stmt_get_stats_) == SQLITE_ROW) {
                const char* state = reinterpret_cast<const char*>(sqlite3_column_text(stmt_get_stats_, 0));
                uint64_t count = sqlite3_column_int64(stmt_get_stats_, 1);
                total += count;
                if (state) {
                    if (strcmp(state, "dirty") == 0) dirty = count;
                    else if (strcmp(state, "uploading") == 0) uploading = count;
                    else if (strcmp(state, "clean") == 0) clean = count;
                    else if (strcmp(state, "evicted") == 0) evicted = count;
                }
            }
        }

        auto s = get_stats();
        double cache_gb = static_cast<double>(cache_bytes_.load()) / (1024.0 * 1024 * 1024);
        double max_gb = static_cast<double>(config_.max_cache_bytes) / (1024.0 * 1024 * 1024);

        log_info("[stats] cache=%.1f/%.1f GB | files: %lu total, %lu dirty, %lu uploading, "
                 "%lu clean, %lu evicted | uploads: %lu ok, %lu fail | restores: %lu ok, "
                 "%lu fail (%lu secondary) | evictions: %lu | shim: %lu fetch, %lu miss",
                 cache_gb, max_gb, total, dirty, uploading, clean, evicted,
                 s.uploads_completed, s.uploads_failed,
                 s.restores_completed, s.restores_failed, s.secondary_restores,
                 s.evictions_performed,
                 s.shim_fetches, s.shim_not_found);
    }
}

}  // namespace meridian::proxy
