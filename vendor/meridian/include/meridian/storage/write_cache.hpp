#pragma once

#include "meridian/storage/backend.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace meridian {

// Configuration for the NVMe write cache
struct WriteCacheConfig {
    // Path for the cache data and manifest (should be on NVMe for best perf)
    std::filesystem::path cache_dir;

    // Number of drain worker threads (move data from cache to permanent storage)
    size_t drain_threads = 8;

    // Maximum cache size in bytes. When exceeded, writes block until
    // drain threads free space (backpressure).
    uint64_t max_cache_bytes = 50ULL * 1024 * 1024 * 1024;  // 50 GB default

    // Low-water mark: when draining reduces cache below this, unblock writers
    uint64_t low_watermark_bytes = 40ULL * 1024 * 1024 * 1024;  // 40 GB

    // Batch size for drain operations (number of objects per drain cycle)
    size_t drain_batch_size = 64;

    // Interval for drain thread polling when queue is empty
    std::chrono::milliseconds drain_poll_interval{100};

    // Enable fsync on writes for crash safety (slower but crash-proof)
    bool fsync_on_write = true;

    // Compression for cache entries (reduces NVMe space, adds CPU)
    // false = store raw (maximum throughput), true = store compressed
    bool compress_cache = false;

    // Number of concurrent backend uploads per drain worker.
    // Each drain worker fires this many S3 PUTs in parallel, hiding network
    // latency behind concurrency. Total in-flight uploads =
    // drain_threads * drain_upload_concurrency.
    // At 13ms per S3 PUT: 8 workers × 16 concurrent = 128 in-flight → ~9800 obj/s.
    size_t drain_upload_concurrency = 16;
};

// Persistent NVMe write cache that wraps a remote storage backend.
//
// Architecture:
//   Writer threads → NVMe data files + SQLite manifest → ACK
//   Drain threads  → read from NVMe → upload to permanent backend → mark drained
//
// Crash safety:
//   - SQLite manifest uses WAL mode; all state changes are atomic
//   - On restart, pending/uploading entries are re-queued for drain
//   - Data files written with optional fsync before manifest update
//
// Backpressure:
//   - When cache exceeds max_cache_bytes, put() blocks until drain frees space
//   - Prevents unbounded NVMe usage during bursts (e.g., large P4 import)
//
// Benefits:
//   - Import/write operations ack at NVMe speed (3-7 GB/s) not S3 speed
//   - Survives server crashes — drain resumes on restart
//   - Works for both P4 import AND normal server operations
//
class NVMeWriteCache : public StorageBackend {
public:
    explicit NVMeWriteCache(
        std::unique_ptr<StorageBackend> permanent_backend,
        const WriteCacheConfig& config);

    ~NVMeWriteCache() override;

    // Non-copyable, non-movable
    NVMeWriteCache(const NVMeWriteCache&) = delete;
    NVMeWriteCache& operator=(const NVMeWriteCache&) = delete;

    std::string type_name() const override;
    bool exists(const std::string& key) const override;
    std::optional<ObjectMetadata> head(const std::string& key) const override;
    GetResult get(const std::string& key, const GetOptions& options = {}) const override;
    std::vector<std::pair<std::string, GetResult>> get_batch(
        const std::vector<std::string>& keys,
        const GetOptions& options = {},
        size_t concurrency = 8) const override;
    PutResult put(const std::string& key, std::span<const uint8_t> data,
                  const PutOptions& options = {}) override;
    PutResult put_file(const std::string& key, const std::filesystem::path& path,
                       const PutOptions& options = {}) override;
    bool remove(const std::string& key) override;
    std::vector<std::string> remove_batch(const std::vector<std::string>& keys) override;
    ListResult list(const ListOptions& options = {}) const override;
    bool copy(const std::string& source, const std::string& destination) override;
    bool move(const std::string& source, const std::string& destination) override;
    std::optional<StorageTier> get_tier(const std::string& key) const override;
    bool set_tier(const std::string& key, StorageTier tier) override;
    bool restore(const std::string& key, std::chrono::hours duration) override;
    bool is_restoring(const std::string& key) const override;
    uint64_t total_objects() const override;
    uint64_t total_bytes() const override;
    bool is_healthy() const override;

    // Flush: block until all cached entries are drained to permanent storage
    void flush();

    // Statistics
    uint64_t cached_objects() const;
    uint64_t cached_bytes() const;
    uint64_t drained_objects() const;
    uint64_t drain_errors() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Factory function
std::unique_ptr<StorageBackend> create_nvme_write_cache(
    std::unique_ptr<StorageBackend> backend,
    const WriteCacheConfig& config);

} // namespace meridian
