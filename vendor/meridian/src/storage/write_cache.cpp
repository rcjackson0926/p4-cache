#include "meridian/storage/write_cache.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include <sqlite3.h>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace meridian {

// ============================================================================
// NVMe Write Cache Implementation
// ============================================================================
//
// Data layout on NVMe:
//   <cache_dir>/
//     manifest.db        — SQLite manifest (WAL mode) tracking all entries
//     data/
//       <shard>/         — 256 shard directories (00-ff) for lock partitioning
//         <key>.dat      — raw object data
//
// Manifest schema:
//   cache_entries(key TEXT PK, data_path TEXT, size INT, status TEXT, created_at INT)
//   status: 'pending' → 'draining' → 'drained'
//
// Lifecycle:
//   put() → write data file → INSERT manifest row (status=pending) → return
//   drain_worker() → SELECT batch WHERE status=pending → upload to backend →
//                    UPDATE status=drained → DELETE data file → DELETE row
//
// Crash recovery:
//   On startup, entries with status='draining' are reset to 'pending'
//   (the upload may not have completed). Data files are still on NVMe.

struct NVMeWriteCache::Impl {
    std::unique_ptr<StorageBackend> backend_;
    WriteCacheConfig config_;

    // SQLite manifest
    sqlite3* db_ = nullptr;
    sqlite3_stmt* stmt_insert_ = nullptr;
    mutable sqlite3_stmt* stmt_select_ = nullptr;
    mutable sqlite3_stmt* stmt_exists_ = nullptr;
    mutable sqlite3_stmt* stmt_drain_batch_ = nullptr;
    sqlite3_stmt* stmt_mark_draining_ = nullptr;
    sqlite3_stmt* stmt_delete_ = nullptr;
    mutable sqlite3_stmt* stmt_count_ = nullptr;
    mutable sqlite3_stmt* stmt_total_size_ = nullptr;
    sqlite3_stmt* stmt_remove_ = nullptr;
    sqlite3_stmt* stmt_reset_pending_ = nullptr;

    // Entry fetched from manifest for drain processing
    struct DrainEntry {
        std::string key;
        std::filesystem::path data_path;
        int64_t size = 0;
    };

    // Drain workers
    std::vector<std::thread> drain_workers_;
    std::atomic<bool> shutdown_{false};
    mutable std::mutex drain_mutex_;
    std::condition_variable drain_cv_;

    // Backpressure
    std::atomic<uint64_t> cached_bytes_{0};
    std::mutex backpressure_mutex_;
    std::condition_variable backpressure_cv_;

    // Stats
    std::atomic<uint64_t> drained_count_{0};
    std::atomic<uint64_t> drain_errors_{0};

    // WAL checkpoint tracking — checkpoint after every N drain batches
    static constexpr uint64_t DRAIN_BATCHES_PER_CHECKPOINT = 16;
    std::atomic<uint64_t> drain_batch_counter_{0};

    // Flush support
    std::mutex flush_mutex_;
    std::condition_variable flush_cv_;

    Impl(std::unique_ptr<StorageBackend> backend, const WriteCacheConfig& config)
        : backend_(std::move(backend)), config_(config) {
        init_directories();
        init_manifest();
        recover_from_crash();
        start_drain_workers();
    }

    ~Impl() {
        shutdown_.store(true);
        drain_cv_.notify_all();
        backpressure_cv_.notify_all();

        for (auto& w : drain_workers_) {
            if (w.joinable()) w.join();
        }

        // Final WAL checkpoint before closing — ensures the WAL file
        // is folded back into the main database and won't persist on disk.
        checkpoint_wal();

        finalize_statements();
        if (db_) sqlite3_close(db_);
    }

    void init_directories() {
        std::filesystem::create_directories(config_.cache_dir / "data");
        // Create 256 shard directories for write parallelism
        for (int i = 0; i < 256; ++i) {
            char shard[3];
            snprintf(shard, sizeof(shard), "%02x", i);
            std::filesystem::create_directories(config_.cache_dir / "data" / shard);
        }
    }

    void init_manifest() {
        auto db_path = config_.cache_dir / "manifest.db";
        int rc = sqlite3_open(db_path.c_str(), &db_);
        if (rc != SQLITE_OK) {
            throw std::runtime_error("Failed to open write cache manifest: " +
                                     db_path.string());
        }

        // Optimized for NVMe write performance
        auto run_pragma = [this](const char* pragma) {
            char* err = nullptr;
            int prc = sqlite3_exec(db_, pragma, nullptr, nullptr, &err);
            if (prc != SQLITE_OK) {
                std::string msg = err ? err : "unknown error";
                sqlite3_free(err);
                std::cerr << "Warning: PRAGMA failed (" << pragma << "): " << msg << "\n";
            }
        };
        run_pragma("PRAGMA journal_mode=WAL");
        run_pragma("PRAGMA synchronous=NORMAL");
        run_pragma("PRAGMA cache_size=-32768");  // 32MB
        run_pragma("PRAGMA temp_store=MEMORY");
        run_pragma("PRAGMA mmap_size=268435456");  // 256MB mmap
        run_pragma("PRAGMA wal_autocheckpoint=10000");  // ~40MB

        const char* schema = R"(
            CREATE TABLE IF NOT EXISTS cache_entries (
                key TEXT PRIMARY KEY NOT NULL,
                data_path TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_status ON cache_entries(status);
            CREATE INDEX IF NOT EXISTS idx_created ON cache_entries(created_at);
        )";
        char* errmsg = nullptr;
        rc = sqlite3_exec(db_, schema, nullptr, nullptr, &errmsg);
        if (rc != SQLITE_OK) {
            std::string err = errmsg ? errmsg : "Unknown";
            sqlite3_free(errmsg);
            throw std::runtime_error("Failed to create cache schema: " + err);
        }

        // Prepare statements
        sqlite3_prepare_v2(db_,
            "INSERT OR REPLACE INTO cache_entries (key, data_path, size, status, created_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            -1, &stmt_insert_, nullptr);

        sqlite3_prepare_v2(db_,
            "SELECT data_path, size FROM cache_entries WHERE key = ?",
            -1, &stmt_select_, nullptr);

        sqlite3_prepare_v2(db_,
            "SELECT 1 FROM cache_entries WHERE key = ?",
            -1, &stmt_exists_, nullptr);

        sqlite3_prepare_v2(db_,
            "SELECT key, data_path, size FROM cache_entries "
            "WHERE status = 'pending' ORDER BY created_at LIMIT ?",
            -1, &stmt_drain_batch_, nullptr);

        sqlite3_prepare_v2(db_,
            "UPDATE cache_entries SET status = 'draining' WHERE key = ?",
            -1, &stmt_mark_draining_, nullptr);

        sqlite3_prepare_v2(db_,
            "DELETE FROM cache_entries WHERE key = ? AND status = 'draining'",
            -1, &stmt_delete_, nullptr);

        sqlite3_prepare_v2(db_,
            "SELECT COUNT(*) FROM cache_entries WHERE status != 'drained'",
            -1, &stmt_count_, nullptr);

        sqlite3_prepare_v2(db_,
            "SELECT COALESCE(SUM(size), 0) FROM cache_entries WHERE status != 'drained'",
            -1, &stmt_total_size_, nullptr);

        sqlite3_prepare_v2(db_,
            "DELETE FROM cache_entries WHERE key = ?",
            -1, &stmt_remove_, nullptr);

        sqlite3_prepare_v2(db_,
            "UPDATE cache_entries SET status = 'pending' WHERE key = ?",
            -1, &stmt_reset_pending_, nullptr);
    }

    void finalize_statements() {
        auto finalize = [](sqlite3_stmt*& s) {
            if (s) { sqlite3_finalize(s); s = nullptr; }
        };
        finalize(stmt_insert_);
        finalize(stmt_select_);
        finalize(stmt_exists_);
        finalize(stmt_drain_batch_);
        finalize(stmt_mark_draining_);
        finalize(stmt_delete_);
        finalize(stmt_count_);
        finalize(stmt_total_size_);
        finalize(stmt_remove_);
        finalize(stmt_reset_pending_);
    }

    void recover_from_crash() {
        // Reset any 'draining' entries back to 'pending' — they may not have
        // completed uploading before the crash
        sqlite3_exec(db_,
            "UPDATE cache_entries SET status = 'pending' WHERE status = 'draining'",
            nullptr, nullptr, nullptr);

        // Clean up any 'drained' entries (data files should already be deleted)
        sqlite3_exec(db_,
            "DELETE FROM cache_entries WHERE status = 'drained'",
            nullptr, nullptr, nullptr);

        // Recalculate cached_bytes_ from manifest
        std::lock_guard<std::mutex> lock(drain_mutex_);
        sqlite3_reset(stmt_total_size_);
        if (sqlite3_step(stmt_total_size_) == SQLITE_ROW) {
            cached_bytes_.store(sqlite3_column_int64(stmt_total_size_, 0));
        }

        // Checkpoint WAL after recovery — the previous session may have left
        // a large WAL file from incomplete drain operations.
        checkpoint_wal();

        uint64_t bytes = cached_bytes_.load();
        if (bytes > 0) {
            std::cerr << "[write-cache] Recovered " << bytes / (1024*1024)
                      << " MB of pending data from previous session\n";
        }
    }

    void start_drain_workers() {
        for (size_t i = 0; i < config_.drain_threads; ++i) {
            drain_workers_.emplace_back([this, i]() { drain_worker(i); });
        }
    }

    // Key → shard directory + filename
    // Keys may contain '/' (e.g., "objects/ab/cdef1234...") so we:
    //   1. Extract the hex hash portion for consistent shard placement
    //   2. Flatten '/' to '_' in the filename to avoid nested directories
    std::filesystem::path key_to_data_path(const std::string& key) const {
        // Extract hex portion from key for shard placement.
        // Keys from BackendContentStore: "objects/ab/HASH" → use "ab" as shard.
        // Keys that are raw hex hashes: "abcdef..." → use first 2 chars.
        std::string shard = "00";
        if (key.size() > 10 && key.substr(0, 8) == "objects/") {
            shard = key.substr(8, 2);  // "objects/XX/..." → "XX"
        } else if (key.size() >= 2) {
            shard = key.substr(0, 2);
        }

        // Sanitize shard to hex range
        for (auto& c : shard) {
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                c = '0';
            }
        }

        // Flatten key for use as filename (replace '/' with '_')
        std::string safe_key = key;
        for (auto& c : safe_key) {
            if (c == '/' || c == '\\') c = '_';
        }
        return config_.cache_dir / "data" / shard / (safe_key + ".dat");
    }

    // Write data to NVMe with optional fsync
    bool write_data_file(const std::filesystem::path& path, std::span<const uint8_t> data) {
#ifndef _WIN32
        if (config_.fsync_on_write) {
            // Direct write with fsync for crash safety
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (fd < 0) return false;

            ssize_t written = 0;
            while (written < static_cast<ssize_t>(data.size())) {
                ssize_t n = ::write(fd, data.data() + written, data.size() - written);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    ::close(fd);
                    return false;
                }
                if (n == 0) {
                    ::close(fd);
                    return false;
                }
                written += n;
            }
            ::fdatasync(fd);
            ::close(fd);
            return true;
        }
#endif
        // Standard write path
        std::ofstream ofs(path, std::ios::binary);
        if (!ofs) return false;
        ofs.write(reinterpret_cast<const char*>(data.data()), data.size());
        return ofs.good();
    }

    // Put an object into the cache (fast path — NVMe write + manifest update)
    PutResult cache_put(const std::string& key, std::span<const uint8_t> data) {
        PutResult result;

        // Backpressure: if cache is full, wait for drain to free space
        {
            std::unique_lock<std::mutex> lock(backpressure_mutex_);
            backpressure_cv_.wait(lock, [this]() {
                return cached_bytes_.load() < config_.max_cache_bytes ||
                       shutdown_.load();
            });
            if (shutdown_.load()) {
                result.error_message = "Write cache shutting down";
                return result;
            }
        }

        auto data_path = key_to_data_path(key);

        // Write data to NVMe
        if (!write_data_file(data_path, data)) {
            result.error_message = "Failed to write cache data file";
            return result;
        }

        // Update manifest (atomic — inside SQLite transaction)
        auto now = std::chrono::system_clock::now().time_since_epoch().count();
        {
            std::lock_guard<std::mutex> lock(drain_mutex_);
            sqlite3_reset(stmt_insert_);
            sqlite3_bind_text(stmt_insert_, 1, key.c_str(), key.size(), SQLITE_STATIC);
            std::string data_path_str = data_path.string();
            sqlite3_bind_text(stmt_insert_, 2, data_path_str.c_str(),
                              static_cast<int>(data_path_str.size()), SQLITE_TRANSIENT);
            sqlite3_bind_int64(stmt_insert_, 3, data.size());
            sqlite3_bind_int64(stmt_insert_, 4, now);
            int rc = sqlite3_step(stmt_insert_);
            if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
                // Manifest insert failed — remove orphaned data file
                std::error_code ec;
                std::filesystem::remove(data_path, ec);
                result.error_message = "Failed to insert into manifest: ";
                result.error_message += sqlite3_errmsg(db_);
                return result;
            }
        }

        cached_bytes_.fetch_add(data.size());

        // Wake a drain worker
        drain_cv_.notify_one();

        result.success = true;
        return result;
    }

    // Check if key exists in cache
    bool cache_exists(const std::string& key) const {
        std::lock_guard<std::mutex> lock(drain_mutex_);
        sqlite3_reset(stmt_exists_);
        sqlite3_bind_text(stmt_exists_, 1, key.c_str(), key.size(), SQLITE_STATIC);
        return sqlite3_step(stmt_exists_) == SQLITE_ROW;
    }

    // Read data from cache
    GetResult cache_get(const std::string& key) const {
        GetResult result;

        std::filesystem::path data_path;
        {
            std::lock_guard<std::mutex> lock(drain_mutex_);
            sqlite3_reset(stmt_select_);
            sqlite3_bind_text(stmt_select_, 1, key.c_str(), key.size(), SQLITE_STATIC);
            if (sqlite3_step(stmt_select_) != SQLITE_ROW) {
                return result;  // Not in cache
            }
            const char* path_str = reinterpret_cast<const char*>(
                sqlite3_column_text(stmt_select_, 0));
            if (!path_str) return result;
            data_path = path_str;
        }

        // Read data from NVMe
        std::ifstream ifs(data_path, std::ios::binary | std::ios::ate);
        if (!ifs) return result;

        auto size = ifs.tellg();
        if (size < 0) return result;
        ifs.seekg(0, std::ios::beg);

        result.data.resize(static_cast<size_t>(size));
        ifs.read(reinterpret_cast<char*>(result.data.data()), size);
        result.data.resize(ifs.gcount());
        result.success = true;
        result.metadata.size = result.data.size();
        return result;
    }

    // Remove a key from cache
    bool cache_remove(const std::string& key) {
        std::filesystem::path data_path;
        {
            std::lock_guard<std::mutex> lock(drain_mutex_);
            sqlite3_reset(stmt_select_);
            sqlite3_bind_text(stmt_select_, 1, key.c_str(), key.size(), SQLITE_STATIC);
            if (sqlite3_step(stmt_select_) == SQLITE_ROW) {
                const char* path_str = reinterpret_cast<const char*>(
                    sqlite3_column_text(stmt_select_, 0));
                if (path_str) data_path = path_str;
                int64_t size = sqlite3_column_int64(stmt_select_, 1);

                sqlite3_reset(stmt_remove_);
                sqlite3_bind_text(stmt_remove_, 1, key.c_str(), key.size(), SQLITE_STATIC);
                sqlite3_step(stmt_remove_);

                // CAS loop to prevent underflow on concurrent decrements
                uint64_t bytes_to_sub = static_cast<uint64_t>(size);
                uint64_t old_val = cached_bytes_.load();
                while (old_val > 0) {
                    uint64_t sub = std::min(bytes_to_sub, old_val);
                    if (cached_bytes_.compare_exchange_weak(old_val, old_val - sub)) {
                        break;
                    }
                }
            }
        }

        if (!data_path.empty()) {
            std::error_code ec;
            std::filesystem::remove(data_path, ec);
        }
        backpressure_cv_.notify_all();
        return true;
    }

    // Upload a single entry to the backend. Returns true on success.
    // Thread-safe: backend_->put() is safe to call from multiple threads.
    bool upload_entry(const std::string& key,
                      const std::filesystem::path& data_path) {
        std::ifstream ifs(data_path, std::ios::binary | std::ios::ate);
        if (!ifs) return false;

        auto size = ifs.tellg();
        if (size < 0) return false;
        ifs.seekg(0, std::ios::beg);
        std::vector<uint8_t> data(static_cast<size_t>(size));
        ifs.read(reinterpret_cast<char*>(data.data()), size);
        data.resize(ifs.gcount());
        ifs.close();

        auto result = backend_->put(key, data);
        if (!result.success) {
            std::cerr << "[write-cache] S3 upload failed: key=" << key
                      << " error=" << result.error_message << "\n";
        }
        return result.success;
    }

    // Process upload results: batch-delete successful entries from manifest,
    // reset failed entries to pending. Runs under drain_mutex_ once per batch.
    void finalize_drain_batch(const std::vector<DrainEntry>& batch,
                              const std::vector<bool>& success) {
        std::vector<size_t> ok_indices;
        std::vector<size_t> fail_indices;
        for (size_t i = 0; i < batch.size(); ++i) {
            if (success[i]) {
                ok_indices.push_back(i);
            } else {
                fail_indices.push_back(i);
            }
        }

        uint64_t bytes_freed = 0;

        // Single lock acquisition for the entire batch
        {
            std::lock_guard<std::mutex> lock(drain_mutex_);

            // Batch-delete successful entries in one transaction
            if (!ok_indices.empty()) {
                sqlite3_exec(db_, "BEGIN", nullptr, nullptr, nullptr);
                for (size_t idx : ok_indices) {
                    sqlite3_reset(stmt_delete_);
                    sqlite3_bind_text(stmt_delete_, 1,
                                      batch[idx].key.c_str(),
                                      batch[idx].key.size(), SQLITE_STATIC);
                    sqlite3_step(stmt_delete_);
                    int deleted = sqlite3_changes(db_);

                    // Only delete the data file if we actually removed the
                    // manifest entry. A concurrent put() may have replaced it.
                    if (deleted > 0) {
                        std::error_code ec;
                        std::filesystem::remove(batch[idx].data_path, ec);
                        bytes_freed += batch[idx].size;
                    }
                }
                sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
            }

            // Reset failed entries to pending for retry
            if (!fail_indices.empty()) {
                sqlite3_exec(db_, "BEGIN", nullptr, nullptr, nullptr);
                for (size_t idx : fail_indices) {
                    sqlite3_reset(stmt_reset_pending_);
                    sqlite3_bind_text(stmt_reset_pending_, 1,
                                      batch[idx].key.c_str(),
                                      batch[idx].key.size(), SQLITE_STATIC);
                    sqlite3_step(stmt_reset_pending_);
                }
                sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
            }
        }

        // Update stats outside the lock — CAS loop to prevent underflow
        if (bytes_freed > 0) {
            uint64_t old_val = cached_bytes_.load();
            while (old_val > 0) {
                uint64_t sub = std::min(bytes_freed, old_val);
                if (cached_bytes_.compare_exchange_weak(old_val, old_val - sub)) {
                    break;
                }
            }
        }
        drained_count_.fetch_add(ok_indices.size());
        drain_errors_.fetch_add(fail_indices.size());
    }

    // Drain worker: moves objects from NVMe cache to permanent backend.
    //
    // Each worker fetches a batch of pending entries, then uploads them
    // concurrently (drain_upload_concurrency in-flight PUTs at a time).
    // This hides S3 network latency behind parallelism: with 8 workers
    // × 16 concurrent uploads = 128 in-flight PUTs, the drain can sustain
    // ~10K obj/sec even at 13ms per PUT.
    void drain_worker(size_t worker_id) {
        (void)worker_id;

        const size_t concurrency = std::max(config_.drain_upload_concurrency,
                                            static_cast<size_t>(1));

        while (!shutdown_.load()) {
            // Fetch a batch of pending entries
            std::vector<DrainEntry> batch;

            {
                std::unique_lock<std::mutex> lock(drain_mutex_);

                // Wait for work or shutdown
                drain_cv_.wait_for(lock, config_.drain_poll_interval, [this]() {
                    return shutdown_.load() || has_pending_locked();
                });

                if (shutdown_.load() && !has_pending_locked()) return;

                // Fetch batch
                sqlite3_reset(stmt_drain_batch_);
                sqlite3_bind_int(stmt_drain_batch_, 1,
                                 static_cast<int>(config_.drain_batch_size));

                while (sqlite3_step(stmt_drain_batch_) == SQLITE_ROW) {
                    DrainEntry entry;
                    const char* key = reinterpret_cast<const char*>(
                        sqlite3_column_text(stmt_drain_batch_, 0));
                    const char* path = reinterpret_cast<const char*>(
                        sqlite3_column_text(stmt_drain_batch_, 1));
                    entry.size = sqlite3_column_int64(stmt_drain_batch_, 2);

                    if (key) entry.key = key;
                    if (path) entry.data_path = path;
                    batch.push_back(std::move(entry));
                }

                // Mark all as 'draining' in one transaction
                if (!batch.empty()) {
                    sqlite3_exec(db_, "BEGIN", nullptr, nullptr, nullptr);
                    for (const auto& entry : batch) {
                        sqlite3_reset(stmt_mark_draining_);
                        sqlite3_bind_text(stmt_mark_draining_, 1,
                                          entry.key.c_str(), entry.key.size(),
                                          SQLITE_STATIC);
                        sqlite3_step(stmt_mark_draining_);
                    }
                    sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
                }
            }

            if (batch.empty()) continue;

            // Upload entries concurrently using async tasks.
            // Process the batch in waves of `concurrency` uploads at a time.
            std::vector<bool> success(batch.size(), false);

            for (size_t wave_start = 0; wave_start < batch.size();
                 wave_start += concurrency) {
                size_t wave_end = std::min(wave_start + concurrency, batch.size());

                // Launch concurrent uploads for this wave
                std::vector<std::future<bool>> futures;
                futures.reserve(wave_end - wave_start);

                for (size_t i = wave_start; i < wave_end; ++i) {
                    futures.push_back(std::async(std::launch::async,
                        &Impl::upload_entry, this,
                        std::cref(batch[i].key),
                        std::cref(batch[i].data_path)));
                }

                // Collect results
                for (size_t i = 0; i < futures.size(); ++i) {
                    success[wave_start + i] = futures[i].get();
                }
            }

            // Batch-finalize all manifest updates in one lock acquisition
            finalize_drain_batch(batch, success);

            // Release backpressure if we've drained enough
            if (cached_bytes_.load() < config_.low_watermark_bytes) {
                backpressure_cv_.notify_all();
            }

            // Periodically checkpoint WAL to prevent unbounded growth
            maybe_checkpoint_after_drain();

            // Notify flush waiters
            flush_cv_.notify_all();
        }

        // Shutdown drain: process remaining entries with same concurrent model
        while (true) {
            std::vector<DrainEntry> remaining;
            {
                std::lock_guard<std::mutex> lock(drain_mutex_);
                sqlite3_reset(stmt_drain_batch_);
                sqlite3_bind_int(stmt_drain_batch_, 1, 256);
                while (sqlite3_step(stmt_drain_batch_) == SQLITE_ROW) {
                    DrainEntry entry;
                    const char* key = reinterpret_cast<const char*>(
                        sqlite3_column_text(stmt_drain_batch_, 0));
                    const char* path = reinterpret_cast<const char*>(
                        sqlite3_column_text(stmt_drain_batch_, 1));
                    entry.size = sqlite3_column_int64(stmt_drain_batch_, 2);
                    if (key) entry.key = key;
                    if (path) entry.data_path = path;
                    if (key && path) remaining.push_back(std::move(entry));
                }
                if (remaining.empty()) break;

                // Mark draining
                sqlite3_exec(db_, "BEGIN", nullptr, nullptr, nullptr);
                for (const auto& e : remaining) {
                    sqlite3_reset(stmt_mark_draining_);
                    sqlite3_bind_text(stmt_mark_draining_, 1,
                                      e.key.c_str(), e.key.size(), SQLITE_STATIC);
                    sqlite3_step(stmt_mark_draining_);
                }
                sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
            }

            std::vector<bool> success(remaining.size(), false);
            for (size_t wave_start = 0; wave_start < remaining.size();
                 wave_start += concurrency) {
                size_t wave_end = std::min(wave_start + concurrency,
                                           remaining.size());
                std::vector<std::future<bool>> futures;
                futures.reserve(wave_end - wave_start);
                for (size_t i = wave_start; i < wave_end; ++i) {
                    futures.push_back(std::async(std::launch::async,
                        &Impl::upload_entry, this,
                        std::cref(remaining[i].key),
                        std::cref(remaining[i].data_path)));
                }
                for (size_t i = 0; i < futures.size(); ++i) {
                    success[wave_start + i] = futures[i].get();
                }
            }
            finalize_drain_batch(remaining, success);
        }

        // Final WAL checkpoint on shutdown to reclaim disk space
        checkpoint_wal();

        flush_cv_.notify_all();
    }

    bool has_pending_locked() const {
        sqlite3_reset(stmt_count_);
        if (sqlite3_step(stmt_count_) == SQLITE_ROW) {
            return sqlite3_column_int64(stmt_count_, 0) > 0;
        }
        return false;
    }

    // Checkpoint the WAL to prevent unbounded growth.
    // PASSIVE mode won't block writers — it checkpoints only pages that
    // aren't currently needed by active readers/writers.
    void checkpoint_wal() {
        if (!db_) return;
        sqlite3_wal_checkpoint_v2(db_, nullptr, SQLITE_CHECKPOINT_PASSIVE,
                                  nullptr, nullptr);
    }

    // Called by drain workers after processing a batch. Triggers a WAL
    // checkpoint every DRAIN_BATCHES_PER_CHECKPOINT batches to bound
    // WAL file size during sustained high-throughput drain operations.
    void maybe_checkpoint_after_drain() {
        uint64_t count = drain_batch_counter_.fetch_add(1) + 1;
        if (count % DRAIN_BATCHES_PER_CHECKPOINT == 0) {
            checkpoint_wal();
        }
    }

    void flush() {
        // Wake all drain workers
        drain_cv_.notify_all();

        // Poll until no more pending entries.
        // Avoids holding flush_mutex_ while acquiring drain_mutex_ (which
        // would invert the lock order vs. drain workers that hold
        // drain_mutex_ and then notify flush_cv_).
        while (!shutdown_.load()) {
            {
                std::lock_guard<std::mutex> dlock(drain_mutex_);
                if (!has_pending_locked()) return;
            }
            // Wait for drain workers to make progress, then re-check
            std::unique_lock<std::mutex> lock(flush_mutex_);
            flush_cv_.wait_for(lock, std::chrono::milliseconds(50));
            drain_cv_.notify_all();
        }
    }

    uint64_t count_cached() const {
        std::lock_guard<std::mutex> lock(drain_mutex_);
        sqlite3_reset(stmt_count_);
        if (sqlite3_step(stmt_count_) == SQLITE_ROW) {
            return sqlite3_column_int64(stmt_count_, 0);
        }
        return 0;
    }

};

// ============================================================================
// NVMeWriteCache public interface
// ============================================================================

NVMeWriteCache::NVMeWriteCache(
    std::unique_ptr<StorageBackend> permanent_backend,
    const WriteCacheConfig& config)
    : impl_(std::make_unique<Impl>(std::move(permanent_backend), config)) {
}

NVMeWriteCache::~NVMeWriteCache() = default;

std::string NVMeWriteCache::type_name() const {
    return "nvme-cache:" + impl_->backend_->type_name();
}

bool NVMeWriteCache::exists(const std::string& key) const {
    // Check cache first (fast NVMe lookup)
    if (impl_->cache_exists(key)) return true;
    // Fall back to permanent backend
    return impl_->backend_->exists(key);
}

std::optional<ObjectMetadata> NVMeWriteCache::head(const std::string& key) const {
    // Check cache metadata
    {
        std::lock_guard<std::mutex> lock(impl_->drain_mutex_);
        sqlite3_reset(impl_->stmt_select_);
        sqlite3_bind_text(impl_->stmt_select_, 1, key.c_str(), key.size(), SQLITE_STATIC);
        if (sqlite3_step(impl_->stmt_select_) == SQLITE_ROW) {
            ObjectMetadata meta;
            meta.size = sqlite3_column_int64(impl_->stmt_select_, 1);
            return meta;
        }
    }
    return impl_->backend_->head(key);
}

GetResult NVMeWriteCache::get(const std::string& key, const GetOptions& options) const {
    // Try cache first (NVMe speed)
    auto cached = impl_->cache_get(key);
    if (cached.success) return cached;
    // Fall back to permanent backend
    return impl_->backend_->get(key, options);
}

std::vector<std::pair<std::string, GetResult>> NVMeWriteCache::get_batch(
    const std::vector<std::string>& keys,
    const GetOptions& options,
    size_t concurrency) const {
    std::vector<std::pair<std::string, GetResult>> results;
    results.reserve(keys.size());
    std::vector<std::string> miss_keys;
    std::vector<size_t> miss_indices;

    for (size_t i = 0; i < keys.size(); ++i) {
        auto cached = impl_->cache_get(keys[i]);
        if (cached.success) {
            results.emplace_back(keys[i], std::move(cached));
        } else {
            results.emplace_back(keys[i], GetResult{});
            miss_keys.push_back(keys[i]);
            miss_indices.push_back(i);
        }
    }

    if (!miss_keys.empty()) {
        auto backend_results = impl_->backend_->get_batch(miss_keys, options, concurrency);
        for (size_t i = 0; i < backend_results.size(); ++i) {
            results[miss_indices[i]] = std::move(backend_results[i]);
        }
    }

    return results;
}

PutResult NVMeWriteCache::put(const std::string& key, std::span<const uint8_t> data,
                               const PutOptions& options) {
    (void)options;
    return impl_->cache_put(key, data);
}

PutResult NVMeWriteCache::put_file(const std::string& key, const std::filesystem::path& path,
                                    const PutOptions& options) {
    // Read file into memory and cache it
    std::ifstream ifs(path, std::ios::binary | std::ios::ate);
    if (!ifs) {
        return PutResult{false, "", "Failed to read file: " + path.string()};
    }
    auto size = ifs.tellg();
    if (size < 0) {
        return PutResult{false, "", "Failed to determine file size: " + path.string()};
    }
    ifs.seekg(0);
    std::vector<uint8_t> data(static_cast<size_t>(size));
    ifs.read(reinterpret_cast<char*>(data.data()), size);
    data.resize(ifs.gcount());

    return put(key, data, options);
}

bool NVMeWriteCache::remove(const std::string& key) {
    impl_->cache_remove(key);
    return impl_->backend_->remove(key);
}

std::vector<std::string> NVMeWriteCache::remove_batch(const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
        impl_->cache_remove(key);
    }
    return impl_->backend_->remove_batch(keys);
}

ListResult NVMeWriteCache::list(const ListOptions& options) const {
    return impl_->backend_->list(options);
}

bool NVMeWriteCache::copy(const std::string& source, const std::string& destination) {
    // Flush source from cache first
    // (simple approach — could be optimized to copy within cache)
    flush();
    return impl_->backend_->copy(source, destination);
}

bool NVMeWriteCache::move(const std::string& source, const std::string& destination) {
    flush();
    return impl_->backend_->move(source, destination);
}

std::optional<StorageTier> NVMeWriteCache::get_tier(const std::string& key) const {
    return impl_->backend_->get_tier(key);
}

bool NVMeWriteCache::set_tier(const std::string& key, StorageTier tier) {
    return impl_->backend_->set_tier(key, tier);
}

bool NVMeWriteCache::restore(const std::string& key, std::chrono::hours duration) {
    return impl_->backend_->restore(key, duration);
}

bool NVMeWriteCache::is_restoring(const std::string& key) const {
    return impl_->backend_->is_restoring(key);
}

uint64_t NVMeWriteCache::total_objects() const {
    return impl_->backend_->total_objects();
}

uint64_t NVMeWriteCache::total_bytes() const {
    return impl_->backend_->total_bytes();
}

bool NVMeWriteCache::is_healthy() const {
    return impl_->backend_->is_healthy();
}

void NVMeWriteCache::flush() {
    impl_->flush();
}

uint64_t NVMeWriteCache::cached_objects() const {
    return impl_->count_cached();
}

uint64_t NVMeWriteCache::cached_bytes() const {
    return impl_->cached_bytes_.load();
}

uint64_t NVMeWriteCache::drained_objects() const {
    return impl_->drained_count_.load();
}

uint64_t NVMeWriteCache::drain_errors() const {
    return impl_->drain_errors_.load();
}

// Factory function
std::unique_ptr<StorageBackend> create_nvme_write_cache(
    std::unique_ptr<StorageBackend> backend,
    const WriteCacheConfig& config) {
    return std::make_unique<NVMeWriteCache>(std::move(backend), config);
}

} // namespace meridian
