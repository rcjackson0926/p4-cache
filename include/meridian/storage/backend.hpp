#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace meridian {

// Metadata about a stored object
struct ObjectMetadata {
    uint64_t size = 0;
    std::chrono::system_clock::time_point last_modified;
    std::string content_type;
    std::string etag;
    std::string storage_class;  // S3 storage class (STANDARD, STANDARD_IA, GLACIER, etc.)
    std::map<std::string, std::string> user_metadata;
};

// Result of a put operation
struct PutResult {
    bool success = false;
    std::string etag;
    std::string error_message;
};

// Result of a get operation
struct GetResult {
    bool success = false;
    std::vector<uint8_t> data;
    ObjectMetadata metadata;
    std::string error_message;
};

// Entry in a listing operation
struct ListEntry {
    std::string key;
    uint64_t size = 0;
    std::chrono::system_clock::time_point last_modified;
    std::string etag;
    std::string storage_class;  // S3 storage class
    bool is_directory = false;
};

// Result of a list operation
struct ListResult {
    bool success = false;
    std::vector<ListEntry> entries;
    bool truncated = false;
    std::string continuation_token;
    std::string error_message;
};

// Options for put operations
struct PutOptions {
    std::string content_type = "application/octet-stream";
    std::map<std::string, std::string> metadata;
    bool if_not_exists = false;  // Only write if key doesn't exist
};

// Options for get operations
struct GetOptions {
    std::optional<uint64_t> range_start;
    std::optional<uint64_t> range_end;
    std::optional<std::string> if_match;  // ETag condition
};

// Options for list operations
struct ListOptions {
    std::string prefix;
    std::string delimiter = "/";
    uint32_t max_keys = 1000;
    std::string continuation_token;
};

// Storage tier for tiered storage support
enum class StorageTier {
    Hot,      // Fastest access, highest cost
    Warm,     // Moderate access time
    Cold,     // Slow access, requires restore
    Archive   // Cheapest, longest restore time
};

// Abstract interface for storage backends
// Provides low-level byte storage, independent of object format
class StorageBackend {
public:
    virtual ~StorageBackend() = default;

    // Get the backend type name (for logging/debugging)
    virtual std::string type_name() const = 0;

    // Check if a key exists
    virtual bool exists(const std::string& key) const = 0;

    // Get object metadata without downloading content
    virtual std::optional<ObjectMetadata> head(const std::string& key) const = 0;

    // Read object content
    virtual GetResult get(const std::string& key,
                          const GetOptions& options = {}) const = 0;

    // Batch read: fetch multiple objects concurrently
    // Default implementation is serial; remote backends override for parallel I/O
    virtual std::vector<std::pair<std::string, GetResult>> get_batch(
        const std::vector<std::string>& keys,
        const GetOptions& options = {},
        size_t concurrency = 8) const {
        (void)concurrency;
        std::vector<std::pair<std::string, GetResult>> results;
        results.reserve(keys.size());
        for (const auto& key : keys) {
            results.emplace_back(key, get(key, options));
        }
        return results;
    }

    // Write object content
    virtual PutResult put(const std::string& key,
                          std::span<const uint8_t> data,
                          const PutOptions& options = {}) = 0;

    // Write from a file (more efficient for large objects)
    virtual PutResult put_file(const std::string& key,
                               const std::filesystem::path& path,
                               const PutOptions& options = {}) = 0;

    // Delete an object
    virtual bool remove(const std::string& key) = 0;

    // Delete multiple objects (batch delete)
    virtual std::vector<std::string> remove_batch(
        const std::vector<std::string>& keys) = 0;

    // List objects with prefix
    virtual ListResult list(const ListOptions& options = {}) const = 0;

    // Copy object within backend (if supported)
    virtual bool copy(const std::string& source,
                      const std::string& destination) = 0;

    // Move/rename object (if supported)
    virtual bool move(const std::string& source,
                      const std::string& destination) = 0;

    // Get storage tier for an object
    virtual std::optional<StorageTier> get_tier(const std::string& key) const = 0;

    // Set storage tier (for tiered storage backends)
    virtual bool set_tier(const std::string& key, StorageTier tier) = 0;

    // Restore from archive tier (returns true if restore started)
    virtual bool restore(const std::string& key,
                         std::chrono::hours duration = std::chrono::hours(24)) = 0;

    // Check if object is being restored
    virtual bool is_restoring(const std::string& key) const = 0;

    // Statistics
    virtual uint64_t total_objects() const = 0;
    virtual uint64_t total_bytes() const = 0;

    // Health check
    virtual bool is_healthy() const = 0;
};

// Factory for creating storage backends from configuration
class StorageBackendFactory {
public:
    // Create a backend from a configuration map
    static std::unique_ptr<StorageBackend> create(
        const std::string& type,
        const std::map<std::string, std::string>& config);

    // Create a local filesystem backend
    static std::unique_ptr<StorageBackend> create_local(
        const std::filesystem::path& root_path);

    // Create an S3-compatible backend
    static std::unique_ptr<StorageBackend> create_s3(
        const std::string& bucket,
        const std::string& region,
        const std::string& endpoint = "",
        const std::string& access_key = "",
        const std::string& secret_key = "");

    // Create a caching wrapper around another backend
    static std::unique_ptr<StorageBackend> create_cached(
        std::unique_ptr<StorageBackend> backend,
        const std::filesystem::path& cache_path,
        uint64_t max_cache_bytes);

    // Create a write-behind async upload wrapper
    // Writes go to local staging dir, background threads upload to the backend
    static std::unique_ptr<StorageBackend> create_async_upload(
        std::unique_ptr<StorageBackend> backend,
        const std::filesystem::path& staging_path,
        size_t upload_threads = 4);

    // Create an Azure Blob Storage backend
    static std::unique_ptr<StorageBackend> create_azure(
        const std::string& account_name,
        const std::string& account_key,
        const std::string& container,
        const std::string& endpoint = "");

    // Create a Google Cloud Storage backend
    static std::unique_ptr<StorageBackend> create_gcs(
        const std::string& bucket,
        const std::string& project_id,
        const std::string& credentials_json = "",
        const std::string& endpoint = "");

    // Create an access-tracking wrapper that records read operations
    // for tiering decisions (calls on_access callback on get/head/exists)
    static std::unique_ptr<StorageBackend> create_access_tracked(
        std::unique_ptr<StorageBackend> backend,
        std::function<void(const std::string&)> on_access);
};

} // namespace meridian
