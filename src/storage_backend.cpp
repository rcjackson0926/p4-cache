#include "meridian/storage/backend.hpp"
#include "meridian/core/compat.hpp"
#include "meridian/net/http.hpp"
#include "meridian/core/constants.hpp"
#include "meridian/storage/lru_cache.hpp"
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <list>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <vector>

namespace meridian {

// ============================================================================
// Configurable shard count for petabyte-scale distribution
// ============================================================================

// Get the number of storage shards - configurable via MERIDIAN_STORAGE_SHARDS env var
static size_t get_num_shards() {
    static size_t num_shards = []() {
        if (const char* env = std::getenv("MERIDIAN_STORAGE_SHARDS")) {
            try {
                size_t val = std::stoul(env);
                // Sanity bounds: 1 to 4096
                if (val >= 1 && val <= 4096) {
                    return val;
                }
            } catch (...) {}
        }
        // Default: 256 shards for better petabyte-scale distribution
        return size_t{256};
    }();
    return num_shards;
}

// ============================================================================
// LocalStorageBackend - File system implementation
// ============================================================================

class LocalStorageBackend : public StorageBackend {
public:
    explicit LocalStorageBackend(const std::filesystem::path& root)
        : root_(std::filesystem::absolute(root)) {
        std::filesystem::create_directories(root_);

        // Initialize shards with configurable count for petabyte-scale distribution
        shards_.resize(get_num_shards());
        for (size_t i = 0; i < get_num_shards(); ++i) {
            shards_[i] = std::make_unique<Shard>();
        }

        // Log the shard configuration at startup
        std::cerr << "[meridian] LocalStorageBackend initialized with "
                  << get_num_shards() << " shards (configurable via MERIDIAN_STORAGE_SHARDS)\n";

        // Initialize counters by scanning (one-time cost at startup)
        initialize_counters();
    }

    std::string type_name() const override { return "local"; }

    bool exists(const std::string& key) const override {
        auto& shard = get_shard(key);
        std::shared_lock lock(shard.mutex);
        return std::filesystem::exists(key_to_path(key));
    }

    std::optional<ObjectMetadata> head(const std::string& key) const override {
        auto& shard = get_shard(key);
        std::shared_lock lock(shard.mutex);
        auto path = key_to_path(key);

        if (!std::filesystem::exists(path)) {
            return std::nullopt;
        }

        ObjectMetadata meta;
        meta.size = std::filesystem::file_size(path);

        auto ftime = std::filesystem::last_write_time(path);
        // CORE-M6 fix: Use C++20 file_clock::to_sys() instead of clock-skew-prone subtraction
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            std::chrono::file_clock::to_sys(ftime));
        meta.last_modified = sctp;

        meta.content_type = "application/octet-stream";
        return meta;
    }

    GetResult get(const std::string& key,
                  const GetOptions& options) const override {
        GetResult result;
        auto& shard = get_shard(key);
        std::shared_lock lock(shard.mutex);

        auto path = key_to_path(key);
        std::ifstream file(path, std::ios::binary | std::ios::ate);

        if (!file) {
            result.success = false;
            result.error_message = "Object not found: " + key;
            return result;
        }

        auto tellg_val = file.tellg();
        if (tellg_val < 0) {
            result.success = false;
            result.error_message = "Cannot determine file size: " + key;
            return result;
        }
        uint64_t file_size = static_cast<uint64_t>(tellg_val);
        uint64_t start = options.range_start.value_or(0);
        uint64_t end = options.range_end.value_or(file_size);

        if (start >= file_size) {
            result.success = false;
            result.error_message = "Range start beyond file size";
            return result;
        }

        end = std::min(end, file_size);
        uint64_t length = end - start;

        file.seekg(start);
        result.data.resize(length);
        file.read(reinterpret_cast<char*>(result.data.data()), length);

        if (!file) {
            result.success = false;
            result.error_message = "Failed to read file";
            return result;
        }

        result.success = true;
        result.metadata.size = file_size;
        return result;
    }

    PutResult put(const std::string& key,
                  std::span<const uint8_t> data,
                  const PutOptions& options) override {
        PutResult result;

        // Use sharded lock for better concurrency on write-heavy workloads
        auto& shard = get_shard(key);
        std::unique_lock lock(shard.mutex);

        auto path = key_to_path(key);

        // Check if file already exists and get its size for counter update
        bool existed = std::filesystem::exists(path);
        uint64_t old_size = 0;
        if (existed) {
            if (options.if_not_exists) {
                result.success = false;
                result.error_message = "Object already exists";
                return result;
            }
            old_size = std::filesystem::file_size(path);
        }

        // Create parent directories
        std::filesystem::create_directories(path.parent_path());

        // Write to temp file then rename (atomic)
        auto temp_path = path.string() + ".tmp." +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

        {
            std::ofstream file(temp_path, std::ios::binary);
            if (!file) {
                result.success = false;
                result.error_message = "Failed to create file";
                return result;
            }

            file.write(reinterpret_cast<const char*>(data.data()), data.size());
            if (!file) {
                std::filesystem::remove(temp_path);
                result.success = false;
                result.error_message = "Failed to write data";
                return result;
            }
        }

        std::error_code ec;
        std::filesystem::rename(temp_path, path, ec);
        if (ec) {
            std::filesystem::remove(temp_path);
            result.success = false;
            result.error_message = "Failed to rename file: " + ec.message();
            return result;
        }

        // Update counters
        if (existed) {
            update_counters(old_size, data.size());
        } else {
            increment_counters(data.size());
        }

        result.success = true;
        return result;
    }

    PutResult put_file(const std::string& key,
                       const std::filesystem::path& source_path,
                       const PutOptions& options) override {
        PutResult result;
        auto& shard = get_shard(key);
        std::unique_lock lock(shard.mutex);

        auto dest_path = key_to_path(key);

        // Check if file already exists and get its size for counter update
        bool existed = std::filesystem::exists(dest_path);
        uint64_t old_size = 0;
        if (existed) {
            if (options.if_not_exists) {
                result.success = false;
                result.error_message = "Object already exists";
                return result;
            }
            old_size = std::filesystem::file_size(dest_path);
        }

        std::filesystem::create_directories(dest_path.parent_path());

        std::error_code ec;
        std::filesystem::copy_file(source_path, dest_path,
            std::filesystem::copy_options::overwrite_existing, ec);

        if (ec) {
            result.success = false;
            result.error_message = "Failed to copy file: " + ec.message();
            return result;
        }

        // Update counters
        uint64_t new_size = std::filesystem::file_size(source_path);
        if (existed) {
            update_counters(old_size, new_size);
        } else {
            increment_counters(new_size);
        }

        result.success = true;
        return result;
    }

    bool remove(const std::string& key) override {
        auto& shard = get_shard(key);
        std::unique_lock lock(shard.mutex);
        auto path = key_to_path(key);

        // Get file size before removal for counter update
        uint64_t size = 0;
        bool existed = false;
        if (std::filesystem::exists(path)) {
            existed = true;
            try {
                size = std::filesystem::file_size(path);
            } catch (const std::exception&) {
                // Ignore size errors
            }
        }

        std::error_code ec;
        bool removed = std::filesystem::remove(path, ec);

        // Update counters if file was removed
        if (removed && existed) {
            decrement_counters(size);
        }

        return removed;
    }

    std::vector<std::string> remove_batch(
        const std::vector<std::string>& keys) override {
        std::vector<std::string> failed;
        for (const auto& key : keys) {
            if (!remove(key)) {
                failed.push_back(key);
            }
        }
        return failed;
    }

    ListResult list(const ListOptions& options) const override {
        ListResult result;
        // List requires global lock since it scans across all shards
        std::shared_lock lock(global_mutex_);

        auto search_path = root_;
        if (!options.prefix.empty()) {
            search_path = root_ / options.prefix;
        }

        if (!std::filesystem::exists(search_path)) {
            result.success = true;
            return result;
        }

        result.entries.reserve(options.max_keys);
        try {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(search_path)) {
                if (result.entries.size() >= options.max_keys) {
                    result.truncated = true;
                    break;
                }

                auto rel_path = std::filesystem::relative(entry.path(), root_);
                std::string key = rel_path.string();

                // Normalize path separators
                std::replace(key.begin(), key.end(), '\\', '/');

                ListEntry le;
                le.key = key;
                le.is_directory = entry.is_directory();

                if (entry.is_regular_file()) {
                    le.size = entry.file_size();
                    auto ftime = entry.last_write_time();
                    auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                        std::chrono::file_clock::to_sys(ftime));
                    le.last_modified = sctp;
                }

                result.entries.push_back(le);
            }
        } catch (const std::exception& e) {
            result.success = false;
            result.error_message = e.what();
            return result;
        }

        result.success = true;
        return result;
    }

    bool copy(const std::string& source, const std::string& destination) override {
        // Lock both source and destination shards (in consistent order to avoid deadlock)
        auto& src_shard = get_shard(source);
        auto& dst_shard = get_shard(destination);

        // Use addresses to determine lock order and avoid deadlock
        std::shared_lock src_lock(src_shard.mutex, std::defer_lock);
        std::unique_lock dst_lock(dst_shard.mutex, std::defer_lock);

        if (&src_shard < &dst_shard) {
            src_lock.lock();
            dst_lock.lock();
        } else if (&src_shard > &dst_shard) {
            dst_lock.lock();
            src_lock.lock();
        } else {
            // Same shard, only need one lock
            dst_lock.lock();
        }

        auto src_path = key_to_path(source);
        auto dst_path = key_to_path(destination);

        std::filesystem::create_directories(dst_path.parent_path());

        std::error_code ec;
        std::filesystem::copy_file(src_path, dst_path,
            std::filesystem::copy_options::overwrite_existing, ec);

        return !ec;
    }

    bool move(const std::string& source, const std::string& destination) override {
        // Lock both source and destination shards (in consistent order to avoid deadlock)
        auto& src_shard = get_shard(source);
        auto& dst_shard = get_shard(destination);

        // Use addresses to determine lock order and avoid deadlock
        std::unique_lock src_lock(src_shard.mutex, std::defer_lock);
        std::unique_lock dst_lock(dst_shard.mutex, std::defer_lock);

        if (&src_shard < &dst_shard) {
            src_lock.lock();
            dst_lock.lock();
        } else if (&src_shard > &dst_shard) {
            dst_lock.lock();
            src_lock.lock();
        } else {
            // Same shard, only need one lock
            src_lock.lock();
        }

        auto src_path = key_to_path(source);
        auto dst_path = key_to_path(destination);

        std::filesystem::create_directories(dst_path.parent_path());

        std::error_code ec;
        std::filesystem::rename(src_path, dst_path, ec);

        return !ec;
    }

    std::optional<StorageTier> get_tier(const std::string& key) const override {
        if (exists(key)) {
            return StorageTier::Hot;  // Local storage is always "hot"
        }
        return std::nullopt;
    }

    bool set_tier(const std::string& /*key*/, StorageTier /*tier*/) override {
        // Local storage doesn't support tiering
        return false;
    }

    bool restore(const std::string& /*key*/,
                 std::chrono::hours /*duration*/) override {
        // Local storage doesn't need restoration
        return false;
    }

    bool is_restoring(const std::string& /*key*/) const override {
        return false;
    }

    uint64_t total_objects() const override {
        // object_count_ is std::atomic - no lock needed
        return object_count_.load(std::memory_order_relaxed);
    }

    uint64_t total_bytes() const override {
        // total_bytes_ is std::atomic - no lock needed
        return total_bytes_.load(std::memory_order_relaxed);
    }

    bool is_healthy() const override {
        return std::filesystem::exists(root_) &&
               std::filesystem::is_directory(root_);
    }

private:
    std::filesystem::path root_;

    // Sharded mutexes for better concurrency on read-heavy workloads
    // Shard count is configurable via MERIDIAN_STORAGE_SHARDS env var (default: 256)
    // for better petabyte-scale distribution

    struct Shard {
        mutable std::shared_mutex mutex;
    };
    mutable std::vector<std::unique_ptr<Shard>> shards_;

    // Legacy single mutex for operations that need global locking (e.g., list)
    mutable std::shared_mutex global_mutex_;

    // Incremental counters - updated on put/remove operations (O(1) queries)
    mutable std::atomic<uint64_t> object_count_{0};
    mutable std::atomic<uint64_t> total_bytes_{0};
    mutable std::atomic<bool> counters_dirty_{false};

    // Get shard for a key using hash
    Shard& get_shard(const std::string& key) const {
        size_t hash = std::hash<std::string>{}(key);
        return *shards_[hash % get_num_shards()];
    }

    std::filesystem::path key_to_path(const std::string& key) const {
        // SECURITY: Validate key to prevent path traversal attacks
        // Reject keys with ".." components or absolute paths
        if (key.empty() || key[0] == '/' || key.find("..") != std::string::npos) {
            throw std::invalid_argument("Invalid storage key: path traversal attempt detected");
        }

        auto result = root_ / key;

        // SECURITY: Verify the result is still under root_
        // Use weakly_canonical to handle symlinks properly
        std::error_code ec;
        auto canonical_root = std::filesystem::weakly_canonical(root_, ec);
        auto canonical_result = std::filesystem::weakly_canonical(result, ec);

        if (!ec) {
            // Check that the result path starts with the root path
            auto root_str = canonical_root.string();
            auto result_str = canonical_result.string();
            if (!result_str.starts_with(root_str)) {
                throw std::invalid_argument("Invalid storage key: path traversal attempt detected");
            }
        }

        return result;
    }

    // Metadata file path for persistent counters
    std::filesystem::path metadata_path() const {
        return root_ / ".storage_metadata";
    }

    void initialize_counters() {
        // Try to load from metadata file first (O(1) startup)
        if (load_counters()) {
            return;
        }

        // Fall back to one-time scan if metadata file is missing or corrupted
        reload_counters();
    }

    // Load counters from metadata file (returns false if file doesn't exist or is corrupted)
    bool load_counters() {
        auto path = metadata_path();
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            return false;
        }

        // Format: [8 bytes magic][8 bytes version][8 bytes object_count][8 bytes total_bytes]
        constexpr uint64_t MAGIC = 0x4D455249444E5354;  // "MERIDNST" in hex
        constexpr uint64_t VERSION = 1;

        uint64_t magic = 0, version = 0, count = 0, bytes = 0;
        file.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        file.read(reinterpret_cast<char*>(&version), sizeof(version));
        file.read(reinterpret_cast<char*>(&count), sizeof(count));
        file.read(reinterpret_cast<char*>(&bytes), sizeof(bytes));

        if (!file || magic != MAGIC || version != VERSION) {
            return false;
        }

        object_count_.store(count, std::memory_order_relaxed);
        total_bytes_.store(bytes, std::memory_order_relaxed);
        counters_dirty_.store(false, std::memory_order_relaxed);
        return true;
    }

    // Save counters to metadata file atomically
    void save_counters() const {
        auto path = metadata_path();
        auto temp_path = path.string() + ".tmp";

        {
            std::ofstream file(temp_path, std::ios::binary);
            if (!file) {
                return;  // Silently fail - not critical
            }

            constexpr uint64_t MAGIC = 0x4D455249444E5354;  // "MERIDNST" in hex
            constexpr uint64_t VERSION = 1;

            uint64_t count = object_count_.load(std::memory_order_relaxed);
            uint64_t bytes = total_bytes_.load(std::memory_order_relaxed);

            file.write(reinterpret_cast<const char*>(&MAGIC), sizeof(MAGIC));
            file.write(reinterpret_cast<const char*>(&VERSION), sizeof(VERSION));
            file.write(reinterpret_cast<const char*>(&count), sizeof(count));
            file.write(reinterpret_cast<const char*>(&bytes), sizeof(bytes));
        }

        std::error_code ec;
        std::filesystem::rename(temp_path, path, ec);

        if (!ec) {
            counters_dirty_.store(false, std::memory_order_relaxed);
        }
    }

    // Full scan to recalculate counters (called when metadata file is missing)
    void reload_counters() {
        uint64_t count = 0;
        uint64_t bytes = 0;
        try {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(root_)) {
                if (entry.is_regular_file()) {
                    // Skip our metadata file
                    if (entry.path().filename() == ".storage_metadata" ||
                        entry.path().filename() == ".storage_metadata.tmp") {
                        continue;
                    }
                    ++count;
                    bytes += entry.file_size();
                }
            }
        } catch (const std::exception&) {
            // Ignore errors during initialization
        }
        object_count_.store(count, std::memory_order_relaxed);
        total_bytes_.store(bytes, std::memory_order_relaxed);

        // Save the counters for next startup
        save_counters();
    }

    // Update counters when adding a file
    void increment_counters(uint64_t size) {
        object_count_.fetch_add(1, std::memory_order_relaxed);
        total_bytes_.fetch_add(size, std::memory_order_relaxed);
        counters_dirty_.store(true, std::memory_order_relaxed);

        // Periodically persist (every 100 operations)
        if ((object_count_.load(std::memory_order_relaxed) % 100) == 0) {
            save_counters();
        }
    }

    // Update counters when removing a file
    void decrement_counters(uint64_t size) {
        object_count_.fetch_sub(1, std::memory_order_relaxed);
        total_bytes_.fetch_sub(size, std::memory_order_relaxed);
        counters_dirty_.store(true, std::memory_order_relaxed);

        // Periodically persist (every 100 operations)
        if ((object_count_.load(std::memory_order_relaxed) % 100) == 0) {
            save_counters();
        }
    }

    // Update counters when replacing a file (old_size -> new_size)
    void update_counters(uint64_t old_size, uint64_t new_size) {
        if (new_size > old_size) {
            total_bytes_.fetch_add(new_size - old_size, std::memory_order_relaxed);
        } else {
            total_bytes_.fetch_sub(old_size - new_size, std::memory_order_relaxed);
        }
        counters_dirty_.store(true, std::memory_order_relaxed);

        // Periodically persist (every 100 operations)
        if ((object_count_.load(std::memory_order_relaxed) % 100) == 0) {
            save_counters();
        }
    }
};

// ============================================================================
// XML parsing helpers for S3 responses (avoids regex for better reliability)
// ============================================================================

namespace xml {

// Find the value between <tag>value</tag>, returns empty string if not found
std::string get_element(const std::string& xml, const std::string& tag, size_t start_pos = 0) {
    std::string open_tag = "<" + tag + ">";
    std::string close_tag = "</" + tag + ">";

    size_t start = xml.find(open_tag, start_pos);
    if (start == std::string::npos) return "";
    start += open_tag.length();

    size_t end = xml.find(close_tag, start);
    if (end == std::string::npos) return "";

    return xml.substr(start, end - start);
}

// Find all occurrences of <tag>...</tag> and return their content positions
struct ElementRange {
    size_t content_start = 0;
    size_t content_end = 0;
    size_t element_end = 0;  // Position after closing tag
};

std::vector<ElementRange> find_elements(const std::string& xml, const std::string& tag) {
    std::vector<ElementRange> results;
    std::string open_tag = "<" + tag + ">";
    std::string close_tag = "</" + tag + ">";

    size_t pos = 0;
    while (pos < xml.size()) {
        size_t start = xml.find(open_tag, pos);
        if (start == std::string::npos) break;

        size_t content_start = start + open_tag.length();
        size_t end = xml.find(close_tag, content_start);
        if (end == std::string::npos) break;

        ElementRange range;
        range.content_start = content_start;
        range.content_end = end;
        range.element_end = end + close_tag.length();
        results.push_back(range);

        pos = range.element_end;
    }

    return results;
}

// Decode XML entities (basic set used by S3)
std::string decode_entities(const std::string& s) {
    std::string result;
    result.reserve(s.size());

    size_t i = 0;
    while (i < s.size()) {
        if (s[i] == '&') {
            if (s.compare(i, 4, "&lt;") == 0) {
                result += '<';
                i += 4;
            } else if (s.compare(i, 4, "&gt;") == 0) {
                result += '>';
                i += 4;
            } else if (s.compare(i, 5, "&amp;") == 0) {
                result += '&';
                i += 5;
            } else if (s.compare(i, 6, "&quot;") == 0) {
                result += '"';
                i += 6;
            } else if (s.compare(i, 6, "&apos;") == 0) {
                result += '\'';
                i += 6;
            } else {
                // Unknown entity, keep as-is
                result += s[i++];
            }
        } else {
            result += s[i++];
        }
    }

    return result;
}

} // namespace xml

// ============================================================================
// SecureString - A string class that zeros memory on destruction
// Prevents credentials from remaining in memory after use
// ============================================================================

class SecureString {
public:
    SecureString() = default;

    explicit SecureString(const std::string& s) : data_(s) {}
    explicit SecureString(std::string&& s) : data_(std::move(s)) {}
    SecureString(const char* s) : data_(s ? s : "") {}

    // Copy constructor - deep copy
    SecureString(const SecureString& other) : data_(other.data_) {}

    // Move constructor
    SecureString(SecureString&& other) noexcept : data_(std::move(other.data_)) {
        // Zero out the moved-from string
        other.secure_clear();
    }

    // Copy assignment
    SecureString& operator=(const SecureString& other) {
        if (this != &other) {
            secure_clear();
            data_ = other.data_;
        }
        return *this;
    }

    // Move assignment
    SecureString& operator=(SecureString&& other) noexcept {
        if (this != &other) {
            secure_clear();
            data_ = std::move(other.data_);
            other.secure_clear();
        }
        return *this;
    }

    // Assignment from std::string
    SecureString& operator=(const std::string& s) {
        secure_clear();
        data_ = s;
        return *this;
    }

    ~SecureString() {
        secure_clear();
    }

    // Access the underlying string (const only to prevent accidental copies)
    const std::string& str() const { return data_; }
    const char* c_str() const { return data_.c_str(); }
    size_t size() const { return data_.size(); }
    size_t length() const { return data_.length(); }
    bool empty() const { return data_.empty(); }

    // Comparison operators
    bool operator==(const SecureString& other) const { return data_ == other.data_; }
    bool operator!=(const SecureString& other) const { return data_ != other.data_; }

private:
    void secure_clear() {
        if (!data_.empty()) {
            // Use volatile to prevent compiler from optimizing away the zeroing
            volatile char* p = const_cast<volatile char*>(data_.data());
            size_t len = data_.size();
            while (len--) {
                *p++ = 0;
            }
            // Also call explicit_bzero if available for extra safety
#if defined(__GLIBC__) && (__GLIBC__ >= 2 && __GLIBC_MINOR__ >= 25)
            explicit_bzero(const_cast<char*>(data_.data()), data_.size());
#endif
            data_.clear();
            data_.shrink_to_fit();  // Release memory back to allocator
        }
    }

    std::string data_;
};

// ============================================================================
// S3StorageBackend - S3-compatible storage implementation
// ============================================================================

class S3StorageBackend : public StorageBackend {
public:
    struct Config {
        std::string bucket;
        std::string region = "us-east-1";
        std::string endpoint;  // Empty for AWS, custom for MinIO/etc
        std::string path_prefix;  // Key prefix for multi-tenant buckets (e.g., "u431/")
        SecureString access_key;  // Uses SecureString to zero on destruction
        SecureString secret_key;  // Uses SecureString to zero on destruction
        bool use_path_style = false;  // For MinIO compatibility
        bool verify_ssl = true;       // Disable for self-signed certs (e.g., MinIO)
        bool unsigned_payload = false; // Skip SHA-256 payload hashing on PUTs
        uint64_t multipart_threshold = constants::DEFAULT_MULTIPART_THRESHOLD;
        uint64_t multipart_chunk_size = 5 * 1024 * 1024;  // 5 MB (S3 minimum part size)
        size_t upload_concurrency = constants::DEFAULT_UPLOAD_CONCURRENCY;
        // Server-side encryption
        bool server_side_encryption = false;
        std::string sse_algorithm = "AES256";  // or "aws:kms"
        std::string kms_key_id;
        // STS/temporary credentials
        std::string session_token;
        // Timeouts
        uint32_t connect_timeout_secs = 10;
        uint32_t request_timeout_secs = 60;
        // Retry
        uint32_t max_retries = 3;
    };

    explicit S3StorageBackend(const Config& config)
        : config_(config)
        , signer_(config.access_key.str(), config.secret_key.str(), config.region, "s3") {
        net::HttpClientConfig http_config;
        http_config.user_agent = "meridian-s3/1.0";
        http_config.verify_ssl_by_default = config_.verify_ssl;
        http_config.default_connect_timeout = std::chrono::milliseconds(config_.connect_timeout_secs * 1000);
        http_config.default_total_timeout = std::chrono::milliseconds(config_.request_timeout_secs * 1000);
        http_client_ = std::make_unique<net::HttpClient>(http_config);
    }

    std::string type_name() const override { return "s3"; }

    bool exists(const std::string& key) const override {
        return head(key).has_value();
    }

    std::optional<ObjectMetadata> head(const std::string& key) const override {
        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::get(url);
        request.method = net::HttpMethod::HEAD;
        sign_request(request);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            return std::nullopt;
        }

        ObjectMetadata meta;
        auto content_length = response.headers.content_length();
        meta.size = content_length.value_or(0);
        meta.etag = response.headers.get("ETag").value_or("");
        meta.content_type = response.headers.content_type().value_or("application/octet-stream");

        // Parse storage class
        auto storage_class = response.headers.get("x-amz-storage-class");
        if (storage_class) {
            meta.storage_class = *storage_class;
        }

        // Check restore status and cache it
        auto restore_header = response.headers.get("x-amz-restore");
        if (restore_header) {
            restore_status_.put(key, *restore_header);
        }

        return meta;
    }

    GetResult get(const std::string& key,
                  const GetOptions& options) const override {
        GetResult result;
        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::get(url);

        if (options.range_start || options.range_end) {
            size_t start = options.range_start.value_or(0);
            size_t end = options.range_end.value_or(0);
            if (end > 0) {
                request.byte_range = {start, end - 1};
            } else {
                // Range from start to end of file
                std::string range_header = "bytes=" + std::to_string(start) + "-";
                request.headers.set("Range", range_header);
            }
        }

        if (options.if_match) {
            request.headers.set("If-Match", *options.if_match);
        }

        sign_request(request);
        auto response = http_client_->execute(request);

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        result.success = true;
        result.data = std::move(response.body);
        result.metadata.size = response.headers.content_length().value_or(result.data.size());
        result.metadata.etag = response.headers.get("ETag").value_or("");
        return result;
    }

    // Parallel batch GET — fires concurrent requests for multiple keys
    std::vector<std::pair<std::string, GetResult>> get_batch(
        const std::vector<std::string>& keys,
        const GetOptions& options = {},
        size_t concurrency = 8) const override {

        std::vector<std::pair<std::string, GetResult>> results;
        results.reserve(keys.size());

        for (size_t batch_start = 0; batch_start < keys.size(); batch_start += concurrency) {
            size_t batch_end = std::min(batch_start + concurrency, keys.size());

            std::vector<std::future<std::pair<std::string, GetResult>>> futures;
            for (size_t i = batch_start; i < batch_end; ++i) {
                futures.push_back(std::async(std::launch::async,
                    [this, &keys, i, &options]() -> std::pair<std::string, GetResult> {
                        return {keys[i], get(keys[i], options)};
                    }));
            }

            for (auto& fut : futures) {
                results.push_back(fut.get());
            }
        }

        return results;
    }

    PutResult put(const std::string& key,
                  std::span<const uint8_t> data,
                  const PutOptions& options) override {
        PutResult result;

        if (data.size() > config_.multipart_threshold) {
            return put_multipart(key, data, options);
        }

        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::put(url,
            std::vector<uint8_t>(data.begin(), data.end()));

        if (!options.content_type.empty()) {
            request.headers.set_content_type(options.content_type);
        } else {
            request.headers.set_content_type("application/octet-stream");
        }

        for (const auto& [k, v] : options.metadata) {
            request.headers.set("x-amz-meta-" + k, v);
        }

        if (options.if_not_exists) {
            request.headers.set("If-None-Match", "*");
        }

        if (config_.unsigned_payload) {
            request.headers.set("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
        }

        // Server-side encryption headers
        if (config_.server_side_encryption) {
            request.headers.set("x-amz-server-side-encryption", config_.sse_algorithm);
            if (config_.sse_algorithm == "aws:kms" && !config_.kms_key_id.empty()) {
                request.headers.set("x-amz-server-side-encryption-aws-kms-key-id", config_.kms_key_id);
            }
        }

        // Sign with session token if present (STS credentials)
        if (!config_.session_token.empty()) {
            signer_.sign_with_token(request, config_.session_token);
        } else {
            sign_request(request);
        }

        // Retry with exponential backoff
        net::HttpResponse response;
        uint32_t attempts = 0;
        uint32_t max_attempts = config_.max_retries + 1;
        while (attempts < max_attempts) {
            response = http_client_->execute(request);
            if (response.ok() || !net::is_retryable_status(response.status_code)) {
                break;
            }
            ++attempts;
            if (attempts < max_attempts) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (1 << attempts)));
            }
        }

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        result.success = true;
        result.etag = response.headers.get("ETag").value_or("");

        // Update cached counters
        increment_counters(data.size());

        return result;
    }

    PutResult put_file(const std::string& key,
                       const std::filesystem::path& path,
                       const PutOptions& options) override {
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) {
            return {false, "", "Failed to open file"};
        }

        auto tellg_val2 = file.tellg();
        if (tellg_val2 < 0) {
            return {false, "", "Failed to determine file size"};
        }
        uint64_t size = static_cast<uint64_t>(tellg_val2);
        file.seekg(0);

        if (size > config_.multipart_threshold) {
            return put_multipart_file(key, path, size, options);
        }

        std::vector<uint8_t> data(size);
        file.read(reinterpret_cast<char*>(data.data()), size);

        return put(key, data, options);
    }

    bool remove(const std::string& key) override {
        // Get size before delete for counter update
        uint64_t size = 0;
        auto meta = head(key);
        if (meta) {
            size = meta->size;
        }

        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::del(url);
        sign_request(request);

        auto response = http_client_->execute(request);
        bool success = response.ok() || response.status_code == 204;

        // Update cached counters on success
        if (success && size > 0) {
            decrement_counters(size);
        }

        return success;
    }

    std::vector<std::string> remove_batch(
        const std::vector<std::string>& keys) override {
        // S3 batch delete - POST to bucket with Delete XML body
        if (keys.empty()) return {};

        auto url = build_url("") + "?delete";

        // Build delete request XML
        std::ostringstream xml;
        xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        xml << "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n";
        xml << "  <Quiet>true</Quiet>\n";
        for (const auto& key : keys) {
            xml << "  <Object><Key>" << escape_xml(key) << "</Key></Object>\n";
        }
        xml << "</Delete>";

        std::string body = xml.str();

        net::HttpRequest request = net::HttpRequest::post(url, body);
        request.headers.set_content_type("application/xml");

        // Calculate content MD5 for integrity
        // (For production, should add Content-MD5 header)

        sign_request(request);
        auto response = http_client_->execute(request);

        std::vector<std::string> failed;
        if (!response.ok()) {
            // If batch fails, return all keys as failed
            return keys;
        }

        // Parse response for individual failures
        std::string response_body = response.body_string();
        // Look for <Error><Key>...</Key></Error> entries
        for (const auto& error_range : xml::find_elements(response_body, "Error")) {
            std::string error_content = response_body.substr(
                error_range.content_start,
                error_range.content_end - error_range.content_start);
            std::string key = xml::decode_entities(xml::get_element(error_content, "Key"));
            if (!key.empty()) {
                failed.push_back(key);
            }
        }

        return failed;
    }

    ListResult list(const ListOptions& options) const override {
        ListResult result;

        std::string url = build_url("");

        // Build query string
        std::vector<std::string> params;
        params.push_back("list-type=2");
        if (!options.prefix.empty()) {
            params.push_back("prefix=" + net::url_encode(options.prefix));
        }
        if (!options.delimiter.empty()) {
            params.push_back("delimiter=" + net::url_encode(options.delimiter));
        }
        params.push_back("max-keys=" + std::to_string(options.max_keys));
        if (!options.continuation_token.empty()) {
            params.push_back("continuation-token=" + net::url_encode(options.continuation_token));
        }

        url += "?";
        for (size_t i = 0; i < params.size(); ++i) {
            if (i > 0) url += "&";
            url += params[i];
        }

        net::HttpRequest request = net::HttpRequest::get(url);
        sign_request(request);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        // Parse XML response
        result = parse_list_response(response.body);
        return result;
    }

    bool copy(const std::string& source, const std::string& destination) override {
        auto url = build_url(destination);

        net::HttpRequest request = net::HttpRequest::put(url, std::vector<uint8_t>{});
        request.headers.set("x-amz-copy-source", "/" + config_.bucket + "/" + net::url_encode(source));

        sign_request(request);
        auto response = http_client_->execute(request);

        return response.ok();
    }

    bool move(const std::string& source, const std::string& destination) override {
        if (!copy(source, destination)) {
            return false;
        }
        return remove(source);
    }

    std::optional<StorageTier> get_tier(const std::string& key) const override {
        auto meta = head(key);
        if (!meta) return std::nullopt;

        // Map S3 storage class to tier
        if (meta->storage_class == "STANDARD" || meta->storage_class.empty()) {
            return StorageTier::Hot;
        } else if (meta->storage_class == "STANDARD_IA" || meta->storage_class == "ONEZONE_IA") {
            return StorageTier::Warm;
        } else if (meta->storage_class == "GLACIER_IR" || meta->storage_class == "GLACIER") {
            return StorageTier::Cold;
        } else if (meta->storage_class == "DEEP_ARCHIVE") {
            return StorageTier::Archive;
        }

        return StorageTier::Hot;
    }

    bool set_tier(const std::string& key, StorageTier tier) override {
        std::string storage_class;
        switch (tier) {
            case StorageTier::Hot: storage_class = "STANDARD"; break;
            case StorageTier::Warm: storage_class = "STANDARD_IA"; break;
            case StorageTier::Cold: storage_class = "GLACIER_IR"; break;
            case StorageTier::Archive: storage_class = "DEEP_ARCHIVE"; break;
        }

        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::put(url, std::vector<uint8_t>{});
        request.headers.set("x-amz-copy-source", "/" + config_.bucket + "/" + net::url_encode(key));
        request.headers.set("x-amz-storage-class", storage_class);
        request.headers.set("x-amz-metadata-directive", "COPY");

        sign_request(request);
        auto response = http_client_->execute(request);

        return response.ok();
    }

    bool restore(const std::string& key, std::chrono::hours duration) override {
        auto url = build_url(key) + "?restore";

        int days = std::max(1, static_cast<int>(duration.count() / 24));
        std::string body =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            "<RestoreRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n"
            "  <Days>" + std::to_string(days) + "</Days>\n"
            "  <GlacierJobParameters>\n"
            "    <Tier>Standard</Tier>\n"
            "  </GlacierJobParameters>\n"
            "</RestoreRequest>";

        net::HttpRequest request = net::HttpRequest::post(url, body);
        request.headers.set_content_type("application/xml");

        sign_request(request);
        auto response = http_client_->execute(request);

        // 200 = restore initiated, 202 = restore already in progress, 409 = already restored
        return response.status_code == 200 || response.status_code == 202 || response.status_code == 409;
    }

    bool is_restoring(const std::string& key) const override {
        // Check cached restore status or fetch fresh
        std::string restore_header;

        if (auto cached = restore_status_.get(key)) {
            restore_header = *cached;
        } else {
            // Fetch fresh status - head() will update restore_status_
            head(key);
            if (auto updated = restore_status_.get(key)) {
                restore_header = *updated;
            }
        }

        // Parse x-amz-restore header
        // Format: ongoing-request="true" or ongoing-request="false", expiry-date="..."
        return restore_header.find("ongoing-request=\"true\"") != std::string::npos;
    }

    uint64_t total_objects() const override {
        // O(1) - return cached counter updated incrementally on put/delete
        return object_count_.load(std::memory_order_relaxed);
    }

    uint64_t total_bytes() const override {
        // O(1) - return cached counter updated incrementally on put/delete
        return total_bytes_.load(std::memory_order_relaxed);
    }

    // Refresh counters from S3 listing (call periodically or on startup)
    void refresh_counters() const {
        uint64_t count = 0;
        uint64_t total = 0;
        ListOptions options;
        options.max_keys = 1000;

        while (true) {
            auto result = list(options);
            if (!result.success) break;

            count += result.entries.size();
            for (const auto& entry : result.entries) {
                total += entry.size;
            }

            if (!result.truncated) break;
            options.continuation_token = result.continuation_token;
        }

        object_count_.store(count, std::memory_order_relaxed);
        total_bytes_.store(total, std::memory_order_relaxed);
        counters_initialized_.store(true, std::memory_order_release);
    }

    // Ensure counters are initialized (lazy initialization on first use)
    void ensure_counters_initialized() const {
        if (!counters_initialized_.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lock(counters_init_mutex_);
            if (!counters_initialized_.load(std::memory_order_relaxed)) {
                refresh_counters();
            }
        }
    }

    bool is_healthy() const override {
        ListOptions options;
        options.max_keys = 1;
        auto result = list(options);
        return result.success;
    }

private:
    // Sign request, using session token if configured
    void sign_request(net::HttpRequest& request) const {
        if (!config_.session_token.empty()) {
            signer_.sign_with_token(request, config_.session_token);
        } else {
            signer_.sign(request);
        }
    }

    Config config_;
    net::AwsSigV4Signer signer_;
    std::unique_ptr<net::HttpClient> http_client_;

    // Restore status cache with TTL (5 minutes) and bounded size (10000 entries max)
    // Restore operations are long-running, so caching for a few minutes is safe
    static constexpr size_t MAX_RESTORE_STATUS_CACHE_SIZE = 10000;
    static constexpr std::chrono::minutes RESTORE_STATUS_TTL{5};
    mutable TTLCache<std::string, std::string> restore_status_{RESTORE_STATUS_TTL, MAX_RESTORE_STATUS_CACHE_SIZE};

    // Cached counters for O(1) total_objects() and total_bytes()
    // Updated incrementally on put/delete operations
    mutable std::atomic<uint64_t> object_count_{0};
    mutable std::atomic<uint64_t> total_bytes_{0};
    mutable std::atomic<bool> counters_initialized_{false};
    mutable std::mutex counters_init_mutex_;

    // Update counters after successful put
    void increment_counters(uint64_t size) const {
        ensure_counters_initialized();
        object_count_.fetch_add(1, std::memory_order_relaxed);
        total_bytes_.fetch_add(size, std::memory_order_relaxed);
    }

    // Update counters after successful delete
    void decrement_counters(uint64_t size) const {
        ensure_counters_initialized();
        object_count_.fetch_sub(1, std::memory_order_relaxed);
        total_bytes_.fetch_sub(size, std::memory_order_relaxed);
    }

    std::string build_url(const std::string& key) const {
        std::string url;
        if (!config_.endpoint.empty()) {
            url = config_.endpoint;
            if (config_.use_path_style && !config_.bucket.empty()) {
                url += "/" + config_.bucket;
            }
        } else {
            if (config_.use_path_style) {
                url = "https://s3." + config_.region + ".amazonaws.com/" + config_.bucket;
            } else {
                url = "https://" + config_.bucket + ".s3." + config_.region + ".amazonaws.com";
            }
        }
        if (!key.empty()) {
            if (!config_.path_prefix.empty()) {
                url += "/" + config_.path_prefix + key;
            } else {
                url += "/" + key;
            }
        }
        return url;
    }

    // Ensure ETag has surrounding quotes (required for S3 CompleteMultipartUpload)
    static std::string ensure_etag_quotes(const std::string& etag) {
        if (etag.empty()) return etag;
        std::string result = etag;
        if (result.front() != '"') result = "\"" + result;
        if (result.back() != '"') result += "\"";
        return result;
    }

    // Helper function for XML entity unescaping
    static std::string unescape_xml_impl(const std::string& s) {
        std::string result;
        result.reserve(s.size());
        size_t i = 0;
        while (i < s.size()) {
            if (s[i] == '&') {
                if (s.compare(i, 4, "&lt;") == 0) { result += '<'; i += 4; }
                else if (s.compare(i, 4, "&gt;") == 0) { result += '>'; i += 4; }
                else if (s.compare(i, 5, "&amp;") == 0) { result += '&'; i += 5; }
                else if (s.compare(i, 6, "&quot;") == 0) { result += '"'; i += 6; }
                else if (s.compare(i, 6, "&apos;") == 0) { result += '\''; i += 6; }
                else { result += s[i++]; }
            } else {
                result += s[i++];
            }
        }
        return result;
    }

    static std::string escape_xml(const std::string& s) {
        std::string result;
        result.reserve(s.size());
        for (char c : s) {
            switch (c) {
                case '&': result += "&amp;"; break;
                case '<': result += "&lt;"; break;
                case '>': result += "&gt;"; break;
                case '"': result += "&quot;"; break;
                case '\'': result += "&apos;"; break;
                default: result += c;
            }
        }
        return result;
    }

    PutResult put_multipart(const std::string& key,
                            std::span<const uint8_t> data,
                            const PutOptions& options) {
        PutResult result;

        // 1. Initiate multipart upload
        std::string upload_id = initiate_multipart_upload(key, options);
        if (upload_id.empty()) {
            result.success = false;
            result.error_message = "Failed to initiate multipart upload";
            return result;
        }

        // 2. Build part list
        struct PartRange { int number = 0; size_t offset = 0; size_t size = 0; };
        std::vector<PartRange> parts;
        size_t off = 0;
        int pn = 1;
        while (off < data.size()) {
            size_t chunk_size = std::min(static_cast<size_t>(config_.multipart_chunk_size), data.size() - off);
            parts.push_back({pn++, off, chunk_size});
            off += chunk_size;
        }

        // 3. Upload parts in parallel
        const size_t concurrency = config_.upload_concurrency;
        std::vector<std::pair<int, std::string>> part_etags;
        std::atomic<bool> any_failed{false};

        for (size_t batch_start = 0; batch_start < parts.size() && !any_failed; batch_start += concurrency) {
            size_t batch_end = std::min(batch_start + concurrency, parts.size());
            std::vector<std::future<std::pair<int, std::string>>> futures;

            for (size_t i = batch_start; i < batch_end; ++i) {
                auto& p = parts[i];
                auto chunk = data.subspan(p.offset, p.size);
                futures.push_back(std::async(std::launch::async,
                    [this, &key, &upload_id, num = p.number, chunk]() -> std::pair<int, std::string> {
                        std::string etag = upload_part(key, upload_id, num, chunk);
                        return {num, etag};
                    }));
            }

            for (auto& fut : futures) {
                auto [num, etag] = fut.get();
                if (etag.empty()) {
                    any_failed = true;
                } else {
                    part_etags.emplace_back(num, etag);
                }
            }
        }

        if (any_failed) {
            abort_multipart_upload(key, upload_id);
            result.success = false;
            result.error_message = "Failed to upload one or more parts";
            return result;
        }

        // 4. Complete multipart upload
        std::string final_etag = complete_multipart_upload(key, upload_id, part_etags);
        if (final_etag.empty()) {
            abort_multipart_upload(key, upload_id);
            result.success = false;
            result.error_message = "Failed to complete multipart upload";
            return result;
        }

        result.success = true;
        result.etag = final_etag;

        // Update cached counters
        increment_counters(data.size());

        return result;
    }

    PutResult put_multipart_file(const std::string& key,
                                  const std::filesystem::path& path,
                                  uint64_t file_size,
                                  const PutOptions& options) {
        PutResult result;

        // 1. Initiate multipart upload
        std::string upload_id = initiate_multipart_upload(key, options);
        if (upload_id.empty()) {
            result.success = false;
            result.error_message = "Failed to initiate multipart upload";
            return result;
        }

        // 2. Read file and upload parts in parallel
        std::vector<std::pair<int, std::string>> part_etags;

        // Read entire file (already know it fits in memory since put() loaded it for small files)
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            abort_multipart_upload(key, upload_id);
            result.success = false;
            result.error_message = "Failed to open file";
            return result;
        }

        std::vector<uint8_t> file_data(file_size);
        file.read(reinterpret_cast<char*>(file_data.data()), file_size);
        if (!file && !file.eof()) {
            abort_multipart_upload(key, upload_id);
            result.success = false;
            result.error_message = "Failed to read file";
            return result;
        }
        file.close();

        // Build part list
        struct PartRange { int number = 0; size_t offset = 0; size_t size = 0; };
        std::vector<PartRange> parts;
        size_t off = 0;
        int pn = 1;
        while (off < file_size) {
            size_t chunk_size = std::min(config_.multipart_chunk_size, file_size - off);
            parts.push_back({pn++, off, chunk_size});
            off += chunk_size;
        }

        // Upload parts in parallel
        const size_t concurrency = config_.upload_concurrency;
        std::atomic<bool> any_failed{false};
        std::span<const uint8_t> data_span(file_data);

        for (size_t batch_start = 0; batch_start < parts.size() && !any_failed; batch_start += concurrency) {
            size_t batch_end = std::min(batch_start + concurrency, parts.size());
            std::vector<std::future<std::pair<int, std::string>>> futures;

            for (size_t i = batch_start; i < batch_end; ++i) {
                auto& p = parts[i];
                auto chunk = data_span.subspan(p.offset, p.size);
                futures.push_back(std::async(std::launch::async,
                    [this, &key, &upload_id, num = p.number, chunk]() -> std::pair<int, std::string> {
                        std::string etag = upload_part(key, upload_id, num, chunk);
                        return {num, etag};
                    }));
            }

            for (auto& fut : futures) {
                auto [num, etag] = fut.get();
                if (etag.empty()) {
                    any_failed = true;
                } else {
                    part_etags.emplace_back(num, etag);
                }
            }
        }

        if (any_failed) {
            abort_multipart_upload(key, upload_id);
            result.success = false;
            result.error_message = "Failed to upload one or more parts";
            return result;
        }

        // 3. Complete multipart upload
        std::string final_etag = complete_multipart_upload(key, upload_id, part_etags);
        if (final_etag.empty()) {
            abort_multipart_upload(key, upload_id);
            result.success = false;
            result.error_message = "Failed to complete multipart upload";
            return result;
        }

        result.success = true;
        result.etag = final_etag;

        // Update cached counters
        increment_counters(file_size);

        return result;
    }

    std::string initiate_multipart_upload(const std::string& key, const PutOptions& options) {
        std::string url = build_url(key) + "?uploads";

        net::HttpRequest request = net::HttpRequest::post(url, std::vector<uint8_t>{});
        if (!options.content_type.empty()) {
            request.headers.set_content_type(options.content_type);
        }
        for (const auto& [k, v] : options.metadata) {
            request.headers.set("x-amz-meta-" + k, v);
        }

        sign_request(request);
        auto response = http_client_->execute(request);

        if (!response.ok()) return "";

        // Parse UploadId from XML response
        std::string body = response.body_string();
        std::string upload_id = xml::get_element(body, "UploadId");
        return upload_id;
    }

    std::string upload_part(const std::string& key, const std::string& upload_id,
                            int part_number, std::span<const uint8_t> data) {
        std::string url = build_url(key) +
            "?partNumber=" + std::to_string(part_number) +
            "&uploadId=" + net::url_encode(upload_id);

        net::HttpRequest request = net::HttpRequest::put(url,
            std::vector<uint8_t>(data.begin(), data.end()));

        if (config_.unsigned_payload) {
            request.headers.set("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
        }

        sign_request(request);
        auto response = http_client_->execute(request);

        if (!response.ok()) return "";

        // ETag from header comes quoted - preserve quotes for CompleteMultipartUpload
        std::string etag = response.headers.get("ETag").value_or("");
        return ensure_etag_quotes(etag);
    }

    std::string complete_multipart_upload(const std::string& key, const std::string& upload_id,
                                          const std::vector<std::pair<int, std::string>>& part_etags) {
        std::string url = build_url(key) + "?uploadId=" + net::url_encode(upload_id);

        // Build completion XML
        std::ostringstream xml;
        xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        xml << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n";
        for (const auto& [part_num, etag] : part_etags) {
            xml << "  <Part>\n";
            xml << "    <PartNumber>" << part_num << "</PartNumber>\n";
            xml << "    <ETag>" << escape_xml(etag) << "</ETag>\n";
            xml << "  </Part>\n";
        }
        xml << "</CompleteMultipartUpload>";

        net::HttpRequest request = net::HttpRequest::post(url, xml.str());
        request.headers.set_content_type("application/xml");

        sign_request(request);
        auto response = http_client_->execute(request);

        if (!response.ok()) return "";

        // Parse ETag from response - may be XML-entity-encoded
        std::string body = response.body_string();
        std::string etag = xml::get_element(body, "ETag");
        if (!etag.empty()) {
            // Decode XML entities (e.g., &quot; -> ")
            etag = xml::decode_entities(etag);
            // Multipart ETags have format "hash-partcount", return as-is with quotes
            return ensure_etag_quotes(etag);
        }
        return "";
    }

    void abort_multipart_upload(const std::string& key, const std::string& upload_id) {
        std::string url = build_url(key) + "?uploadId=" + net::url_encode(upload_id);

        net::HttpRequest request = net::HttpRequest::del(url);
        sign_request(request);
        http_client_->execute(request);  // Ignore result
    }

    ListResult parse_list_response(const std::vector<uint8_t>& body) const {
        ListResult result;
        result.success = true;

        std::string xml_str(body.begin(), body.end());

        // Parse IsTruncated
        std::string truncated = xml::get_element(xml_str, "IsTruncated");
        result.truncated = (truncated == "true");

        // Parse NextContinuationToken
        result.continuation_token = xml::get_element(xml_str, "NextContinuationToken");

        // Parse Contents entries
        for (const auto& range : xml::find_elements(xml_str, "Contents")) {
            std::string content = xml_str.substr(range.content_start,
                                                  range.content_end - range.content_start);

            ListEntry entry;
            entry.is_directory = false;

            // Parse Key
            entry.key = xml::decode_entities(xml::get_element(content, "Key"));

            // Parse Size
            std::string size_str = xml::get_element(content, "Size");
            if (!size_str.empty()) {
                entry.size = std::stoull(size_str);
            }

            // Parse LastModified (ISO 8601 format: 2023-12-15T14:30:00.000Z)
            std::string date_str = xml::get_element(content, "LastModified");
            if (!date_str.empty()) {
                std::tm tm = {};
                int year, month, day, hour, min, sec;
                int millis = 0;
                // Try parsing with milliseconds first
                if (sscanf(date_str.c_str(), "%d-%d-%dT%d:%d:%d.%dZ",
                           &year, &month, &day, &hour, &min, &sec, &millis) >= 6 ||
                    sscanf(date_str.c_str(), "%d-%d-%dT%d:%d:%dZ",
                           &year, &month, &day, &hour, &min, &sec) == 6) {
                    tm.tm_year = year - 1900;
                    tm.tm_mon = month - 1;
                    tm.tm_mday = day;
                    tm.tm_hour = hour;
                    tm.tm_min = min;
                    tm.tm_sec = sec;
                    tm.tm_isdst = 0;
                    // Convert to time_t (UTC)
                    time_t tt = timegm(&tm);
                    if (tt != -1) {
                        entry.last_modified = std::chrono::system_clock::from_time_t(tt);
                        // Add milliseconds if present
                        if (millis > 0) {
                            entry.last_modified += std::chrono::milliseconds(millis);
                        }
                    }
                }
            }

            // Parse ETag
            entry.etag = xml::decode_entities(xml::get_element(content, "ETag"));

            // Parse StorageClass
            entry.storage_class = xml::get_element(content, "StorageClass");

            result.entries.push_back(entry);
        }

        // Parse CommonPrefixes (directories)
        for (const auto& range : xml::find_elements(xml_str, "CommonPrefixes")) {
            std::string content = xml_str.substr(range.content_start,
                                                  range.content_end - range.content_start);
            ListEntry entry;
            entry.key = xml::decode_entities(xml::get_element(content, "Prefix"));
            entry.is_directory = true;
            entry.size = 0;
            result.entries.push_back(entry);
        }

        return result;
    }
};

// ============================================================================
// AzureStorageBackend - Azure Blob Storage implementation
// ============================================================================

class AzureStorageBackend : public StorageBackend {
public:
    struct Config {
        std::string account_name;
        SecureString account_key;       // SharedKey auth
        std::string sas_token;          // Alternative: SAS token auth
        std::string container;
        std::string path_prefix;
        std::string endpoint;           // Empty for Azure, custom for Azurite emulator
        bool verify_ssl = true;
        uint64_t multipart_threshold = constants::DEFAULT_MULTIPART_THRESHOLD;  // 5 MB
        uint64_t block_size = 1 * 1024 * 1024;                               // 1 MB (no API min; 50K block limit → ~48 GB max)
        size_t upload_concurrency = constants::DEFAULT_UPLOAD_CONCURRENCY;
    };

    explicit AzureStorageBackend(const Config& config)
        : config_(config) {
        net::HttpClientConfig http_config;
        http_config.user_agent = "meridian-azure/1.0";
        http_config.verify_ssl_by_default = config_.verify_ssl;
        http_client_ = std::make_unique<net::HttpClient>(http_config);
    }

    std::string type_name() const override { return "azure"; }

    bool exists(const std::string& key) const override {
        return head(key).has_value();
    }

    std::optional<ObjectMetadata> head(const std::string& key) const override {
        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::get(url);
        request.method = net::HttpMethod::HEAD;
        add_common_headers(request);
        sign_request(request, "HEAD", key);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            return std::nullopt;
        }

        ObjectMetadata meta;
        meta.size = response.headers.content_length().value_or(0);
        meta.etag = response.headers.get("ETag").value_or("");
        meta.content_type = response.headers.content_type().value_or("application/octet-stream");
        meta.storage_class = response.headers.get("x-ms-access-tier").value_or("Hot");

        return meta;
    }

    GetResult get(const std::string& key,
                  const GetOptions& options) const override {
        GetResult result;
        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::get(url);

        if (options.range_start || options.range_end) {
            size_t start = options.range_start.value_or(0);
            size_t end = options.range_end.value_or(0);
            if (end > 0) {
                request.headers.set("x-ms-range", "bytes=" + std::to_string(start) + "-" + std::to_string(end - 1));
            } else {
                request.headers.set("x-ms-range", "bytes=" + std::to_string(start) + "-");
            }
        }

        if (options.if_match) {
            request.headers.set("If-Match", *options.if_match);
        }

        add_common_headers(request);
        sign_request(request, "GET", key);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        result.success = true;
        result.data = std::move(response.body);
        result.metadata.size = response.headers.content_length().value_or(result.data.size());
        result.metadata.etag = response.headers.get("ETag").value_or("");
        return result;
    }

    std::vector<std::pair<std::string, GetResult>> get_batch(
        const std::vector<std::string>& keys,
        const GetOptions& options = {},
        size_t concurrency = 8) const override {

        std::vector<std::pair<std::string, GetResult>> results;
        results.reserve(keys.size());

        for (size_t batch_start = 0; batch_start < keys.size(); batch_start += concurrency) {
            size_t batch_end = std::min(batch_start + concurrency, keys.size());

            std::vector<std::future<std::pair<std::string, GetResult>>> futures;
            for (size_t i = batch_start; i < batch_end; ++i) {
                futures.push_back(std::async(std::launch::async,
                    [this, &keys, i, &options]() -> std::pair<std::string, GetResult> {
                        return {keys[i], get(keys[i], options)};
                    }));
            }

            for (auto& fut : futures) {
                results.push_back(fut.get());
            }
        }

        return results;
    }

    PutResult put(const std::string& key,
                  std::span<const uint8_t> data,
                  const PutOptions& options) override {
        PutResult result;

        if (data.size() > config_.multipart_threshold) {
            return put_block_blob(key, data, options);
        }

        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::put(url,
            std::vector<uint8_t>(data.begin(), data.end()));

        request.headers.set("x-ms-blob-type", "BlockBlob");

        if (!options.content_type.empty()) {
            request.headers.set_content_type(options.content_type);
        } else {
            request.headers.set_content_type("application/octet-stream");
        }

        for (const auto& [k, v] : options.metadata) {
            request.headers.set("x-ms-meta-" + k, v);
        }

        add_common_headers(request);
        sign_request(request, "PUT", key);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        result.success = true;
        result.etag = response.headers.get("ETag").value_or("");
        increment_counters(data.size());
        return result;
    }

    PutResult put_file(const std::string& key,
                       const std::filesystem::path& path,
                       const PutOptions& options) override {
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) {
            return {false, "", "Failed to open file"};
        }

        auto tellg_val = file.tellg();
        if (tellg_val < 0) {
            return {false, "", "Failed to determine file size"};
        }
        uint64_t size = static_cast<uint64_t>(tellg_val);
        file.seekg(0);

        std::vector<uint8_t> data(size);
        file.read(reinterpret_cast<char*>(data.data()), size);

        return put(key, data, options);
    }

    bool remove(const std::string& key) override {
        uint64_t size = 0;
        auto meta = head(key);
        if (meta) {
            size = meta->size;
        }

        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::del(url);
        add_common_headers(request);
        sign_request(request, "DELETE", key);

        auto response = http_client_->execute(request);
        bool success = response.ok() || response.status_code == 202;

        if (success && size > 0) {
            decrement_counters(size);
        }

        return success;
    }

    std::vector<std::string> remove_batch(
        const std::vector<std::string>& keys) override {
        // Azure doesn't have batch delete in the basic blob API;
        // do parallel individual DELETEs
        std::vector<std::string> failed;
        std::vector<std::future<std::pair<std::string, bool>>> futures;

        for (const auto& key : keys) {
            futures.push_back(std::async(std::launch::async,
                [this, &key]() -> std::pair<std::string, bool> {
                    return {key, remove(key)};
                }));
        }

        for (auto& fut : futures) {
            auto [key, success] = fut.get();
            if (!success) {
                failed.push_back(key);
            }
        }

        return failed;
    }

    ListResult list(const ListOptions& options) const override {
        ListResult result;

        std::string url = build_container_url();
        url += "?restype=container&comp=list";

        if (!options.prefix.empty()) {
            url += "&prefix=" + net::url_encode(options.prefix);
        }
        if (!options.delimiter.empty()) {
            url += "&delimiter=" + net::url_encode(options.delimiter);
        }
        url += "&maxresults=" + std::to_string(options.max_keys);
        if (!options.continuation_token.empty()) {
            url += "&marker=" + net::url_encode(options.continuation_token);
        }

        net::HttpRequest request = net::HttpRequest::get(url);
        add_common_headers(request);
        sign_request_url(request, "GET", "");

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        result.success = true;
        std::string body(response.body.begin(), response.body.end());

        // Parse Azure XML response
        result.continuation_token = xml::get_element(body, "NextMarker");
        result.truncated = !result.continuation_token.empty();

        for (const auto& range : xml::find_elements(body, "Blob")) {
            std::string content = body.substr(range.content_start,
                                               range.content_end - range.content_start);
            ListEntry entry;
            entry.is_directory = false;
            entry.key = xml::decode_entities(xml::get_element(content, "Name"));

            // Parse properties
            std::string props = xml::get_element(content, "Properties");
            if (!props.empty()) {
                std::string size_str = xml::get_element(props, "Content-Length");
                if (!size_str.empty()) entry.size = std::stoull(size_str);
                entry.etag = xml::decode_entities(xml::get_element(props, "Etag"));
                entry.storage_class = xml::get_element(props, "AccessTier");
            }

            result.entries.push_back(entry);
        }

        // Parse BlobPrefix elements (directories)
        for (const auto& range : xml::find_elements(body, "BlobPrefix")) {
            std::string content = body.substr(range.content_start,
                                               range.content_end - range.content_start);
            ListEntry entry;
            entry.key = xml::decode_entities(xml::get_element(content, "Name"));
            entry.is_directory = true;
            entry.size = 0;
            result.entries.push_back(entry);
        }

        return result;
    }

    bool copy(const std::string& source, const std::string& destination) override {
        auto url = build_url(destination);

        net::HttpRequest request = net::HttpRequest::put(url, std::vector<uint8_t>{});
        request.headers.set("x-ms-copy-source", build_url(source));

        add_common_headers(request);
        sign_request(request, "PUT", destination);

        auto response = http_client_->execute(request);
        return response.ok() || response.status_code == 202;
    }

    bool move(const std::string& source, const std::string& destination) override {
        if (!copy(source, destination)) {
            return false;
        }
        return remove(source);
    }

    std::optional<StorageTier> get_tier(const std::string& key) const override {
        auto meta = head(key);
        if (!meta) return std::nullopt;

        // Map Azure access tier to StorageTier
        if (meta->storage_class == "Hot" || meta->storage_class.empty()) {
            return StorageTier::Hot;
        } else if (meta->storage_class == "Cool") {
            return StorageTier::Warm;
        } else if (meta->storage_class == "Cold") {
            return StorageTier::Cold;
        } else if (meta->storage_class == "Archive") {
            return StorageTier::Archive;
        }

        return StorageTier::Hot;
    }

    bool set_tier(const std::string& key, StorageTier tier) override {
        std::string azure_tier;
        switch (tier) {
            case StorageTier::Hot: azure_tier = "Hot"; break;
            case StorageTier::Warm: azure_tier = "Cool"; break;
            case StorageTier::Cold: azure_tier = "Cold"; break;
            case StorageTier::Archive: azure_tier = "Archive"; break;
        }

        auto url = build_url(key) + "?comp=tier";

        net::HttpRequest request = net::HttpRequest::put(url, std::vector<uint8_t>{});
        request.headers.set("x-ms-access-tier", azure_tier);

        add_common_headers(request);
        sign_request(request, "PUT", key);

        auto response = http_client_->execute(request);
        return response.ok() || response.status_code == 200 || response.status_code == 202;
    }

    bool restore(const std::string& key, std::chrono::hours /*duration*/) override {
        // Azure rehydrates from Archive by changing tier back to Hot
        return set_tier(key, StorageTier::Hot);
    }

    bool is_restoring(const std::string& key) const override {
        auto url = build_url(key);

        net::HttpRequest request = net::HttpRequest::get(url);
        request.method = net::HttpMethod::HEAD;
        add_common_headers(request);
        sign_request(request, "HEAD", key);

        auto response = http_client_->execute(request);
        if (!response.ok()) return false;

        auto archive_status = response.headers.get("x-ms-archive-status");
        return archive_status && archive_status->find("rehydrate-pending") != std::string::npos;
    }

    uint64_t total_objects() const override {
        return object_count_.load(std::memory_order_relaxed);
    }

    uint64_t total_bytes() const override {
        return total_bytes_.load(std::memory_order_relaxed);
    }

    bool is_healthy() const override {
        ListOptions opts;
        opts.max_keys = 1;
        auto result = list(opts);
        return result.success;
    }

private:
    Config config_;
    std::unique_ptr<net::HttpClient> http_client_;

    mutable std::atomic<uint64_t> object_count_{0};
    mutable std::atomic<uint64_t> total_bytes_{0};

    void increment_counters(uint64_t size) const {
        object_count_.fetch_add(1, std::memory_order_relaxed);
        total_bytes_.fetch_add(size, std::memory_order_relaxed);
    }

    void decrement_counters(uint64_t size) const {
        object_count_.fetch_sub(1, std::memory_order_relaxed);
        total_bytes_.fetch_sub(size, std::memory_order_relaxed);
    }

    /// Percent-encode each path segment while preserving '/' as separators.
    static std::string url_encode_path(const std::string& path) {
        std::string result;
        size_t start = 0;
        while (start < path.size()) {
            auto slash = path.find('/', start);
            std::string segment;
            if (slash != std::string::npos) {
                segment = path.substr(start, slash - start);
                start = slash + 1;
            } else {
                segment = path.substr(start);
                start = path.size();
            }
            if (!result.empty()) {
                result += '/';
            }
            result += net::url_encode(segment);
        }
        return result;
    }

    std::string build_url(const std::string& key) const {
        std::string url;
        if (!config_.endpoint.empty()) {
            url = config_.endpoint + "/" + config_.container;
        } else {
            url = "https://" + config_.account_name + ".blob.core.windows.net/" + config_.container;
        }
        if (!key.empty()) {
            if (!config_.path_prefix.empty()) {
                url += "/" + url_encode_path(config_.path_prefix + key);
            } else {
                url += "/" + url_encode_path(key);
            }
        }
        return url;
    }

    std::string build_container_url() const {
        if (!config_.endpoint.empty()) {
            return config_.endpoint + "/" + config_.container;
        }
        return "https://" + config_.account_name + ".blob.core.windows.net/" + config_.container;
    }

    void add_common_headers(net::HttpRequest& request) const {
        // Azure requires x-ms-date and x-ms-version on all requests
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::tm tm_buf;
        meridian::portable_gmtime(&time_t_now, &tm_buf);
        char date_buf[128];
        strftime(date_buf, sizeof(date_buf), "%a, %d %b %Y %H:%M:%S GMT", &tm_buf);

        request.headers.set("x-ms-date", date_buf);
        request.headers.set("x-ms-version", "2020-10-02");
    }

    // Azure SharedKey signing
    void sign_request(net::HttpRequest& request, const std::string& method,
                      const std::string& key) const {
        if (!config_.sas_token.empty()) {
            // SAS token auth: append query params
            auto& url = request.url;
            url += (url.find('?') != std::string::npos ? "&" : "?") + config_.sas_token;
            return;
        }

        // SharedKey signing: HMAC-SHA256 of canonical string
        // StringToSign format:
        // VERB\nContent-Encoding\nContent-Language\nContent-Length\nContent-MD5\n
        // Content-Type\nDate\nIf-Modified-Since\nIf-Match\nIf-None-Match\n
        // If-Unmodified-Since\nRange\nx-ms-headers\nCanonicalizedResource
        std::string string_to_sign;
        string_to_sign += method + "\n";
        string_to_sign += "\n"; // Content-Encoding
        string_to_sign += "\n"; // Content-Language

        auto content_len = request.headers.content_length();
        if (content_len && *content_len > 0) {
            string_to_sign += std::to_string(*content_len) + "\n";
        } else if (!request.body.empty()) {
            string_to_sign += std::to_string(request.body.size()) + "\n";
        } else {
            string_to_sign += "\n";
        }

        string_to_sign += "\n"; // Content-MD5
        string_to_sign += request.headers.content_type().value_or("") + "\n";
        string_to_sign += "\n"; // Date (use x-ms-date instead)
        string_to_sign += "\n"; // If-Modified-Since
        string_to_sign += request.headers.get("If-Match").value_or("") + "\n";
        string_to_sign += request.headers.get("If-None-Match").value_or("") + "\n";
        string_to_sign += "\n"; // If-Unmodified-Since
        string_to_sign += request.headers.get("x-ms-range").value_or(
                           request.headers.get("Range").value_or("")) + "\n";

        // Canonicalized x-ms- headers (sorted)
        std::map<std::string, std::string> ms_headers;
        for (const auto& [name, value] : request.headers.all()) {
            std::string lower_name = name;
            std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
            if (lower_name.find("x-ms-") == 0) {
                ms_headers[lower_name] = value;
            }
        }
        for (const auto& [name, value] : ms_headers) {
            string_to_sign += name + ":" + value + "\n";
        }

        // Canonicalized resource
        string_to_sign += "/" + config_.account_name + "/" + config_.container;
        if (!key.empty()) {
            if (!config_.path_prefix.empty()) {
                string_to_sign += "/" + config_.path_prefix + key;
            } else {
                string_to_sign += "/" + key;
            }
        }

        // Parse and add query params (sorted)
        auto qpos = request.url.find('?');
        if (qpos != std::string::npos) {
            std::string query = request.url.substr(qpos + 1);
            std::map<std::string, std::string> params;
            size_t pos = 0;
            while (pos < query.size()) {
                auto amp = query.find('&', pos);
                std::string param = (amp != std::string::npos) ? query.substr(pos, amp - pos) : query.substr(pos);
                auto eq = param.find('=');
                if (eq != std::string::npos) {
                    params[param.substr(0, eq)] = param.substr(eq + 1);
                } else {
                    params[param] = "";
                }
                pos = (amp != std::string::npos) ? amp + 1 : query.size();
            }
            for (const auto& [pname, pval] : params) {
                string_to_sign += "\n" + pname + ":" + pval;
            }
        }

        // HMAC-SHA256 with base64-decoded account key
        auto decoded_key = base64_decode(config_.account_key.str());
        auto signature = hmac_sha256(decoded_key, string_to_sign);
        auto sig_b64 = base64_encode(signature);

        request.headers.set("Authorization", "SharedKey " + config_.account_name + ":" + sig_b64);
    }

    // Simplified sign for container-level operations (list)
    void sign_request_url(net::HttpRequest& request, const std::string& method,
                          const std::string& key) const {
        sign_request(request, method, key);
    }

    // Block blob upload for large files (parallel block staging)
    PutResult put_block_blob(const std::string& key,
                             std::span<const uint8_t> data,
                             const PutOptions& options) {
        PutResult result;

        // 1. Build block list
        struct BlockRange { int number = 0; size_t offset = 0; size_t size = 0; std::string block_id; };
        std::vector<BlockRange> blocks;
        size_t off = 0;
        int block_num = 0;
        while (off < data.size()) {
            size_t chunk_size = std::min(static_cast<size_t>(config_.block_size), data.size() - off);
            // Block ID must be base64-encoded, same length for all blocks
            char id_buf[16];
            snprintf(id_buf, sizeof(id_buf), "%06d", block_num);
            std::string block_id = base64_encode(
                std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(id_buf),
                                     reinterpret_cast<const uint8_t*>(id_buf) + strlen(id_buf)));
            blocks.push_back({block_num, off, chunk_size, block_id});
            off += chunk_size;
            ++block_num;
        }

        // 2. Upload blocks in parallel
        const size_t concurrency = config_.upload_concurrency;
        std::vector<std::string> block_ids;
        block_ids.reserve(blocks.size());
        std::atomic<bool> any_failed{false};

        for (size_t batch_start = 0; batch_start < blocks.size() && !any_failed; batch_start += concurrency) {
            size_t batch_end = std::min(batch_start + concurrency, blocks.size());
            std::vector<std::future<bool>> futures;

            for (size_t i = batch_start; i < batch_end; ++i) {
                auto& b = blocks[i];
                auto chunk = data.subspan(b.offset, b.size);
                futures.push_back(std::async(std::launch::async,
                    [this, &key, bid = b.block_id, chunk]() -> bool {
                        auto url = build_url(key) + "?comp=block&blockid=" + net::url_encode(bid);
                        net::HttpRequest request = net::HttpRequest::put(url,
                            std::vector<uint8_t>(chunk.begin(), chunk.end()));
                        add_common_headers(request);
                        sign_request(request, "PUT", key);
                        auto response = http_client_->execute(request);
                        return response.ok();
                    }));
            }

            for (auto& fut : futures) {
                if (!fut.get()) {
                    any_failed = true;
                }
            }
        }

        if (any_failed) {
            result.success = false;
            result.error_message = "Failed to upload one or more blocks";
            return result;
        }

        for (const auto& b : blocks) {
            block_ids.push_back(b.block_id);
        }

        // Commit block list
        std::ostringstream block_list_xml;
        block_list_xml << "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n";
        block_list_xml << "<BlockList>\n";
        for (const auto& bid : block_ids) {
            block_list_xml << "  <Latest>" << bid << "</Latest>\n";
        }
        block_list_xml << "</BlockList>";

        auto commit_url = build_url(key) + "?comp=blocklist";
        std::string body_str = block_list_xml.str();

        net::HttpRequest commit_req = net::HttpRequest::put(commit_url,
            std::vector<uint8_t>(body_str.begin(), body_str.end()));
        commit_req.headers.set_content_type("application/xml");

        if (!options.content_type.empty()) {
            commit_req.headers.set("x-ms-blob-content-type", options.content_type);
        }

        add_common_headers(commit_req);
        sign_request(commit_req, "PUT", key);

        auto commit_response = http_client_->execute(commit_req);
        if (!commit_response.ok()) {
            result.success = false;
            result.error_message = "Failed to commit block list";
            return result;
        }

        result.success = true;
        result.etag = commit_response.headers.get("ETag").value_or("");
        increment_counters(data.size());
        return result;
    }

    // Base64 encode/decode helpers
    static std::string base64_encode(const std::vector<uint8_t>& data) {
        static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string result;
        size_t i = 0;
        while (i < data.size()) {
            uint32_t a = i < data.size() ? data[i++] : 0;
            uint32_t b = i < data.size() ? data[i++] : 0;
            uint32_t c = i < data.size() ? data[i++] : 0;
            uint32_t triple = (a << 16) | (b << 8) | c;
            result += table[(triple >> 18) & 0x3F];
            result += table[(triple >> 12) & 0x3F];
            result += (i > data.size() + 1) ? '=' : table[(triple >> 6) & 0x3F];
            result += (i > data.size()) ? '=' : table[triple & 0x3F];
        }
        return result;
    }

    static std::string base64_encode(const std::string& s) {
        return base64_encode(std::vector<uint8_t>(s.begin(), s.end()));
    }

    static std::vector<uint8_t> base64_decode(const std::string& encoded) {
        static const int lookup[] = {
            -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
            -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,
            -1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
            -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1
        };
        std::vector<uint8_t> result;
        size_t i = 0;
        while (i < encoded.size()) {
            uint32_t a = (i < encoded.size() && encoded[i] != '=') ? lookup[static_cast<uint8_t>(encoded[i])] : 0; ++i;
            uint32_t b = (i < encoded.size() && encoded[i] != '=') ? lookup[static_cast<uint8_t>(encoded[i])] : 0; ++i;
            uint32_t c = (i < encoded.size() && encoded[i] != '=') ? lookup[static_cast<uint8_t>(encoded[i])] : 0; ++i;
            uint32_t d = (i < encoded.size() && encoded[i] != '=') ? lookup[static_cast<uint8_t>(encoded[i])] : 0; ++i;
            uint32_t triple = (a << 18) | (b << 12) | (c << 6) | d;
            result.push_back(static_cast<uint8_t>((triple >> 16) & 0xFF));
            if (i >= 3 && encoded[i - 2] != '=') result.push_back(static_cast<uint8_t>((triple >> 8) & 0xFF));
            if (i >= 4 && encoded[i - 1] != '=') result.push_back(static_cast<uint8_t>(triple & 0xFF));
        }
        return result;
    }

    // HMAC-SHA256 using OpenSSL
    static std::vector<uint8_t> hmac_sha256(const std::vector<uint8_t>& key, const std::string& data) {
        std::vector<uint8_t> result(32);
        unsigned int len = 0;
        HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
             reinterpret_cast<const unsigned char*>(data.data()), data.size(),
             result.data(), &len);
        result.resize(len);
        return result;
    }
};

// ============================================================================
// GCSStorageBackend - Google Cloud Storage implementation
// ============================================================================

class GCSStorageBackend : public StorageBackend {
public:
    struct Config {
        std::string project_id;
        std::string bucket;
        std::string path_prefix;
        std::string credentials_json;       // Service account JSON content
        std::string credentials_file;       // Or path to JSON key file
        std::string endpoint;               // Empty for Google, custom for emulator
        bool verify_ssl = true;
        uint64_t composite_threshold = constants::DEFAULT_MULTIPART_THRESHOLD;  // 5 MB
        uint64_t composite_chunk_size = 2 * 1024 * 1024;   // 2 MB (no API min; compose limit 32 per call)
        size_t upload_concurrency = constants::DEFAULT_UPLOAD_CONCURRENCY;
    };

    explicit GCSStorageBackend(const Config& config)
        : config_(config) {
        net::HttpClientConfig http_config;
        http_config.user_agent = "meridian-gcs/1.0";
        http_config.verify_ssl_by_default = config_.verify_ssl;
        http_client_ = std::make_unique<net::HttpClient>(http_config);

        // Load credentials from file if JSON not provided directly
        if (config_.credentials_json.empty() && !config_.credentials_file.empty()) {
            std::ifstream cred_file(config_.credentials_file);
            if (cred_file) {
                std::ostringstream ss;
                ss << cred_file.rdbuf();
                config_.credentials_json = ss.str();
            }
        }
    }

    std::string type_name() const override { return "gcs"; }

    bool exists(const std::string& key) const override {
        return head(key).has_value();
    }

    std::optional<ObjectMetadata> head(const std::string& key) const override {
        auto url = metadata_url(key);

        net::HttpRequest request = net::HttpRequest::get(url);
        add_auth_header(request);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            return std::nullopt;
        }

        ObjectMetadata meta;
        // GCS returns JSON metadata
        std::string body(response.body.begin(), response.body.end());
        meta.size = parse_json_uint64(body, "size");
        meta.etag = parse_json_string(body, "etag");
        meta.content_type = parse_json_string(body, "contentType");
        meta.storage_class = parse_json_string(body, "storageClass");

        return meta;
    }

    GetResult get(const std::string& key,
                  const GetOptions& options) const override {
        GetResult result;
        auto url = download_url(key);

        net::HttpRequest request = net::HttpRequest::get(url);

        if (options.range_start || options.range_end) {
            size_t start = options.range_start.value_or(0);
            size_t end = options.range_end.value_or(0);
            if (end > 0) {
                request.headers.set("Range", "bytes=" + std::to_string(start) + "-" + std::to_string(end - 1));
            } else {
                request.headers.set("Range", "bytes=" + std::to_string(start) + "-");
            }
        }

        if (options.if_match) {
            request.headers.set("If-Match", *options.if_match);
        }

        add_auth_header(request);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        result.success = true;
        result.data = std::move(response.body);
        result.metadata.size = response.headers.content_length().value_or(result.data.size());
        result.metadata.etag = response.headers.get("ETag").value_or("");
        return result;
    }

    std::vector<std::pair<std::string, GetResult>> get_batch(
        const std::vector<std::string>& keys,
        const GetOptions& options = {},
        size_t concurrency = 8) const override {

        std::vector<std::pair<std::string, GetResult>> results;
        results.reserve(keys.size());

        for (size_t batch_start = 0; batch_start < keys.size(); batch_start += concurrency) {
            size_t batch_end = std::min(batch_start + concurrency, keys.size());

            std::vector<std::future<std::pair<std::string, GetResult>>> futures;
            for (size_t i = batch_start; i < batch_end; ++i) {
                futures.push_back(std::async(std::launch::async,
                    [this, &keys, i, &options]() -> std::pair<std::string, GetResult> {
                        return {keys[i], get(keys[i], options)};
                    }));
            }

            for (auto& fut : futures) {
                results.push_back(fut.get());
            }
        }

        return results;
    }

    PutResult put(const std::string& key,
                  std::span<const uint8_t> data,
                  const PutOptions& options) override {
        PutResult result;

        if (data.size() > config_.composite_threshold) {
            return put_composite(key, data, options);
        }

        // Simple upload
        auto url = upload_url(key);

        net::HttpRequest request = net::HttpRequest::post(url,
            std::vector<uint8_t>(data.begin(), data.end()));

        if (!options.content_type.empty()) {
            request.headers.set_content_type(options.content_type);
        } else {
            request.headers.set_content_type("application/octet-stream");
        }

        add_auth_header(request);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        result.success = true;
        // Parse etag from JSON response
        std::string body(response.body.begin(), response.body.end());
        result.etag = parse_json_string(body, "etag");
        increment_counters(data.size());
        return result;
    }

    PutResult put_file(const std::string& key,
                       const std::filesystem::path& path,
                       const PutOptions& options) override {
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) {
            return {false, "", "Failed to open file"};
        }

        auto tellg_val = file.tellg();
        if (tellg_val < 0) {
            return {false, "", "Failed to determine file size"};
        }
        uint64_t size = static_cast<uint64_t>(tellg_val);
        file.seekg(0);

        std::vector<uint8_t> data(size);
        file.read(reinterpret_cast<char*>(data.data()), size);

        return put(key, data, options);
    }

    bool remove(const std::string& key) override {
        uint64_t size = 0;
        auto meta = head(key);
        if (meta) {
            size = meta->size;
        }

        auto url = metadata_url(key);

        net::HttpRequest request = net::HttpRequest::del(url);
        add_auth_header(request);

        auto response = http_client_->execute(request);
        bool success = response.ok() || response.status_code == 204;

        if (success && size > 0) {
            decrement_counters(size);
        }

        return success;
    }

    std::vector<std::string> remove_batch(
        const std::vector<std::string>& keys) override {
        std::vector<std::string> failed;
        std::vector<std::future<std::pair<std::string, bool>>> futures;

        for (const auto& key : keys) {
            futures.push_back(std::async(std::launch::async,
                [this, &key]() -> std::pair<std::string, bool> {
                    return {key, remove(key)};
                }));
        }

        for (auto& fut : futures) {
            auto [key, success] = fut.get();
            if (!success) {
                failed.push_back(key);
            }
        }

        return failed;
    }

    ListResult list(const ListOptions& options) const override {
        ListResult result;

        auto url = api_base() + "/b/" + config_.bucket + "/o?";

        std::vector<std::string> params;
        if (!options.prefix.empty()) {
            params.push_back("prefix=" + net::url_encode(options.prefix));
        }
        if (!options.delimiter.empty()) {
            params.push_back("delimiter=" + net::url_encode(options.delimiter));
        }
        params.push_back("maxResults=" + std::to_string(options.max_keys));
        if (!options.continuation_token.empty()) {
            params.push_back("pageToken=" + net::url_encode(options.continuation_token));
        }

        for (size_t i = 0; i < params.size(); ++i) {
            if (i > 0) url += "&";
            url += params[i];
        }

        net::HttpRequest request = net::HttpRequest::get(url);
        add_auth_header(request);

        auto response = http_client_->execute(request);

        if (!response.ok()) {
            result.success = false;
            result.error_message = response.error.empty()
                ? "HTTP " + std::to_string(response.status_code)
                : response.error;
            return result;
        }

        result.success = true;
        std::string body(response.body.begin(), response.body.end());

        // Parse JSON response: {items: [{name, size, etag, storageClass}], nextPageToken, prefixes}
        result.continuation_token = parse_json_string(body, "nextPageToken");
        result.truncated = !result.continuation_token.empty();

        // Parse items array (simplified JSON parsing)
        auto items_start = body.find("\"items\"");
        if (items_start != std::string::npos) {
            auto arr_start = body.find('[', items_start);
            if (arr_start != std::string::npos) {
                // Find matching bracket
                size_t depth = 1;
                size_t pos = arr_start + 1;
                while (pos < body.size() && depth > 0) {
                    if (body[pos] == '{') {
                        // Find the matching }
                        size_t obj_start = pos;
                        size_t obj_depth = 1;
                        ++pos;
                        while (pos < body.size() && obj_depth > 0) {
                            if (body[pos] == '{') ++obj_depth;
                            else if (body[pos] == '}') --obj_depth;
                            else if (body[pos] == '"') {
                                ++pos;
                                while (pos < body.size() && body[pos] != '"') {
                                    if (body[pos] == '\\') ++pos;
                                    ++pos;
                                }
                            }
                            ++pos;
                        }

                        std::string obj = body.substr(obj_start, pos - obj_start);
                        ListEntry entry;
                        entry.is_directory = false;
                        entry.key = parse_json_string(obj, "name");
                        entry.size = parse_json_uint64(obj, "size");
                        entry.etag = parse_json_string(obj, "etag");
                        entry.storage_class = parse_json_string(obj, "storageClass");
                        result.entries.push_back(entry);
                    } else if (body[pos] == ']') {
                        --depth;
                    }
                    ++pos;
                }
            }
        }

        // Parse prefixes array (directories)
        auto pref_start = body.find("\"prefixes\"");
        if (pref_start != std::string::npos) {
            auto parr_start = body.find('[', pref_start);
            if (parr_start != std::string::npos) {
                size_t pos = parr_start + 1;
                while (pos < body.size() && body[pos] != ']') {
                    if (body[pos] == '"') {
                        ++pos;
                        size_t str_start = pos;
                        while (pos < body.size() && body[pos] != '"') {
                            if (body[pos] == '\\') ++pos;
                            ++pos;
                        }
                        ListEntry entry;
                        entry.key = body.substr(str_start, pos - str_start);
                        entry.is_directory = true;
                        entry.size = 0;
                        result.entries.push_back(entry);
                    }
                    ++pos;
                }
            }
        }

        return result;
    }

    bool copy(const std::string& source, const std::string& destination) override {
        // GCS rewrite API
        auto url = api_base() + "/b/" + config_.bucket + "/o/" +
                   net::url_encode(make_key(source)) + "/rewriteTo/b/" +
                   config_.bucket + "/o/" + net::url_encode(make_key(destination));

        net::HttpRequest request = net::HttpRequest::post(url, std::vector<uint8_t>{});
        add_auth_header(request);

        auto response = http_client_->execute(request);
        return response.ok();
    }

    bool move(const std::string& source, const std::string& destination) override {
        if (!copy(source, destination)) {
            return false;
        }
        return remove(source);
    }

    std::optional<StorageTier> get_tier(const std::string& key) const override {
        auto meta = head(key);
        if (!meta) return std::nullopt;

        // Map GCS storage class to StorageTier
        if (meta->storage_class == "STANDARD" || meta->storage_class == "MULTI_REGIONAL" ||
            meta->storage_class == "REGIONAL" || meta->storage_class.empty()) {
            return StorageTier::Hot;
        } else if (meta->storage_class == "NEARLINE") {
            return StorageTier::Warm;
        } else if (meta->storage_class == "COLDLINE") {
            return StorageTier::Cold;
        } else if (meta->storage_class == "ARCHIVE") {
            return StorageTier::Archive;
        }

        return StorageTier::Hot;
    }

    bool set_tier(const std::string& key, StorageTier tier) override {
        std::string gcs_class;
        switch (tier) {
            case StorageTier::Hot: gcs_class = "STANDARD"; break;
            case StorageTier::Warm: gcs_class = "NEARLINE"; break;
            case StorageTier::Cold: gcs_class = "COLDLINE"; break;
            case StorageTier::Archive: gcs_class = "ARCHIVE"; break;
        }

        // Use rewrite to self to change storage class
        auto url = api_base() + "/b/" + config_.bucket + "/o/" +
                   net::url_encode(make_key(key)) + "/rewriteTo/b/" +
                   config_.bucket + "/o/" + net::url_encode(make_key(key));

        std::string body_json = "{\"storageClass\":\"" + gcs_class + "\"}";
        net::HttpRequest request = net::HttpRequest::post(url, body_json);
        request.headers.set_content_type("application/json");
        add_auth_header(request);

        auto response = http_client_->execute(request);
        return response.ok();
    }

    bool restore(const std::string& key, std::chrono::hours /*duration*/) override {
        // GCS restores by changing storage class to STANDARD (metadata-only, not async)
        return set_tier(key, StorageTier::Hot);
    }

    bool is_restoring(const std::string& /*key*/) const override {
        // GCS tier changes are metadata-only operations, not async rehydration
        return false;
    }

    uint64_t total_objects() const override {
        return object_count_.load(std::memory_order_relaxed);
    }

    uint64_t total_bytes() const override {
        return total_bytes_.load(std::memory_order_relaxed);
    }

    bool is_healthy() const override {
        ListOptions opts;
        opts.max_keys = 1;
        auto result = list(opts);
        return result.success;
    }

private:
    mutable Config config_;  // mutable for credential loading
    std::unique_ptr<net::HttpClient> http_client_;

    mutable std::atomic<uint64_t> object_count_{0};
    mutable std::atomic<uint64_t> total_bytes_{0};

    // OAuth2 token cache
    mutable std::mutex token_mutex_;
    mutable std::string cached_token_;
    mutable std::chrono::steady_clock::time_point token_expiry_;

    void increment_counters(uint64_t size) const {
        object_count_.fetch_add(1, std::memory_order_relaxed);
        total_bytes_.fetch_add(size, std::memory_order_relaxed);
    }

    void decrement_counters(uint64_t size) const {
        object_count_.fetch_sub(1, std::memory_order_relaxed);
        total_bytes_.fetch_sub(size, std::memory_order_relaxed);
    }

    std::string api_base() const {
        if (!config_.endpoint.empty()) {
            return config_.endpoint + "/storage/v1";
        }
        return "https://storage.googleapis.com/storage/v1";
    }

    std::string upload_base() const {
        if (!config_.endpoint.empty()) {
            return config_.endpoint + "/upload/storage/v1";
        }
        return "https://storage.googleapis.com/upload/storage/v1";
    }

    std::string make_key(const std::string& key) const {
        if (!config_.path_prefix.empty()) {
            return config_.path_prefix + key;
        }
        return key;
    }

    std::string metadata_url(const std::string& key) const {
        return api_base() + "/b/" + config_.bucket + "/o/" + net::url_encode(make_key(key));
    }

    std::string download_url(const std::string& key) const {
        return metadata_url(key) + "?alt=media";
    }

    std::string upload_url(const std::string& key) const {
        return upload_base() + "/b/" + config_.bucket + "/o?uploadType=media&name=" +
               net::url_encode(make_key(key));
    }

    // OAuth2 Bearer token from service account JSON
    void add_auth_header(net::HttpRequest& request) const {
        auto token = get_access_token();
        if (!token.empty()) {
            request.headers.set_bearer_token(token);
        }
    }

    std::string get_access_token() const {
        std::lock_guard lock(token_mutex_);

        // Return cached token if still valid (with 5-minute margin)
        auto now = std::chrono::steady_clock::now();
        if (!cached_token_.empty() && now < token_expiry_ - std::chrono::minutes(5)) {
            return cached_token_;
        }

        if (config_.credentials_json.empty()) {
            return "";  // No credentials configured
        }

        // Parse service account JSON
        std::string client_email = parse_json_string(config_.credentials_json, "client_email");
        std::string private_key = parse_json_string(config_.credentials_json, "private_key");
        std::string token_uri = parse_json_string(config_.credentials_json, "token_uri");

        if (client_email.empty() || private_key.empty()) {
            return "";
        }

        if (token_uri.empty()) {
            token_uri = "https://oauth2.googleapis.com/token";
        }

        // Build JWT
        auto now_time = std::chrono::system_clock::now();
        auto iat = std::chrono::duration_cast<std::chrono::seconds>(
            now_time.time_since_epoch()).count();
        auto exp = iat + 3600;

        std::string header_json = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
        std::string payload_json = "{\"iss\":\"" + client_email +
            "\",\"scope\":\"https://www.googleapis.com/auth/devstorage.full_control" +
            "\",\"aud\":\"" + token_uri +
            "\",\"iat\":" + std::to_string(iat) +
            ",\"exp\":" + std::to_string(exp) + "}";

        std::string header_b64 = base64url_encode(header_json);
        std::string payload_b64 = base64url_encode(payload_json);
        std::string signing_input = header_b64 + "." + payload_b64;

        // Sign with RSA private key
        std::string signature = rsa_sign_sha256(private_key, signing_input);
        if (signature.empty()) {
            return "";
        }
        std::string sig_b64 = base64url_encode_bytes(
            std::vector<uint8_t>(signature.begin(), signature.end()));

        std::string jwt = signing_input + "." + sig_b64;

        // Exchange JWT for access token
        std::string post_body = "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=" + jwt;

        net::HttpRequest token_request = net::HttpRequest::post(token_uri, post_body);
        token_request.headers.set_content_type("application/x-www-form-urlencoded");

        auto token_response = http_client_->execute(token_request);
        if (!token_response.ok()) {
            return "";
        }

        std::string response_body(token_response.body.begin(), token_response.body.end());
        cached_token_ = parse_json_string(response_body, "access_token");
        token_expiry_ = std::chrono::steady_clock::now() + std::chrono::minutes(55);

        return cached_token_;
    }

    // RSA-SHA256 signing using OpenSSL
    static std::string rsa_sign_sha256(const std::string& pem_key, const std::string& data) {
        // Parse PEM private key
        BIO* bio = BIO_new_mem_buf(pem_key.data(), static_cast<int>(pem_key.size()));
        if (!bio) return "";

        EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
        BIO_free(bio);
        if (!pkey) return "";

        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        if (!ctx) { EVP_PKEY_free(pkey); return ""; }

        std::string signature;
        if (EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, pkey) == 1) {
            if (EVP_DigestSignUpdate(ctx, data.data(), data.size()) == 1) {
                size_t sig_len = 0;
                EVP_DigestSignFinal(ctx, nullptr, &sig_len);
                signature.resize(sig_len);
                EVP_DigestSignFinal(ctx, reinterpret_cast<unsigned char*>(signature.data()), &sig_len);
                signature.resize(sig_len);
            }
        }

        EVP_MD_CTX_free(ctx);
        EVP_PKEY_free(pkey);
        return signature;
    }

    // Base64url encoding (no padding, URL-safe alphabet)
    static std::string base64url_encode(const std::string& input) {
        return base64url_encode_bytes(std::vector<uint8_t>(input.begin(), input.end()));
    }

    static std::string base64url_encode_bytes(const std::vector<uint8_t>& data) {
        static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string result;
        size_t i = 0;
        while (i < data.size()) {
            uint32_t a = i < data.size() ? data[i++] : 0;
            uint32_t b = i < data.size() ? data[i++] : 0;
            uint32_t c = i < data.size() ? data[i++] : 0;
            uint32_t triple = (a << 16) | (b << 8) | c;
            result += table[(triple >> 18) & 0x3F];
            result += table[(triple >> 12) & 0x3F];
            if (i > data.size() + 1) break;
            result += table[(triple >> 6) & 0x3F];
            if (i > data.size()) break;
            result += table[triple & 0x3F];
        }
        // Convert to URL-safe
        for (auto& ch : result) {
            if (ch == '+') ch = '-';
            else if (ch == '/') ch = '_';
        }
        // Remove padding
        while (!result.empty() && result.back() == '=') result.pop_back();
        return result;
    }

    // Parallel composite upload for large files.
    // Uploads chunks as temporary objects in parallel, then composes them
    // into the final object. GCS compose supports up to 32 sources per call;
    // we cascade when there are more chunks.
    PutResult put_composite(const std::string& key,
                            std::span<const uint8_t> data,
                            const PutOptions& options) {
        PutResult result;
        const std::string full_key = make_key(key);

        // 1. Build chunk list
        struct ChunkInfo { size_t index; size_t offset; size_t size; std::string temp_key; };
        std::vector<ChunkInfo> chunks;
        size_t off = 0;
        size_t idx = 0;
        while (off < data.size()) {
            size_t chunk_size = std::min(static_cast<size_t>(config_.composite_chunk_size), data.size() - off);
            std::string temp_key = full_key + ".__part_" + std::to_string(idx);
            chunks.push_back({idx, off, chunk_size, temp_key});
            off += chunk_size;
            ++idx;
        }

        // 2. Upload chunks as temporary objects in parallel
        const size_t concurrency = config_.upload_concurrency;
        std::atomic<bool> any_failed{false};

        for (size_t batch_start = 0; batch_start < chunks.size() && !any_failed; batch_start += concurrency) {
            size_t batch_end = std::min(batch_start + concurrency, chunks.size());
            std::vector<std::future<bool>> futures;

            for (size_t i = batch_start; i < batch_end; ++i) {
                auto& c = chunks[i];
                auto chunk = data.subspan(c.offset, c.size);
                futures.push_back(std::async(std::launch::async,
                    [this, temp_name = c.temp_key, chunk]() -> bool {
                        auto url = upload_base() + "/b/" + config_.bucket +
                                   "/o?uploadType=media&name=" + net::url_encode(temp_name);
                        net::HttpRequest request = net::HttpRequest::post(url,
                            std::vector<uint8_t>(chunk.begin(), chunk.end()));
                        request.headers.set_content_type("application/octet-stream");
                        add_auth_header(request);
                        auto response = http_client_->execute(request);
                        return response.ok();
                    }));
            }

            for (auto& fut : futures) {
                if (!fut.get()) any_failed = true;
            }
        }

        if (any_failed) {
            // Clean up any uploaded temp objects
            std::vector<std::string> temp_keys;
            for (const auto& c : chunks) temp_keys.push_back(c.temp_key);
            cleanup_temp_objects(temp_keys);
            result.success = false;
            result.error_message = "Failed to upload one or more chunks";
            return result;
        }

        // 3. Compose temp objects into the final object.
        //    GCS compose supports up to 32 sources per call; cascade if needed.
        std::vector<std::string> source_names;
        source_names.reserve(chunks.size());
        for (const auto& c : chunks) {
            source_names.push_back(c.temp_key);
        }

        static constexpr size_t GCS_MAX_COMPOSE = 32;

        while (source_names.size() > 1) {
            std::vector<std::string> next_sources;

            for (size_t i = 0; i < source_names.size(); i += GCS_MAX_COMPOSE) {
                size_t end = std::min(i + GCS_MAX_COMPOSE, source_names.size());
                std::vector<std::string> batch(source_names.begin() + i, source_names.begin() + end);

                // Last batch in the last cascade level → compose into the final key
                bool is_final = (next_sources.empty() && end == source_names.size() && batch.size() <= GCS_MAX_COMPOSE);
                std::string dest = is_final ? full_key
                    : (full_key + ".__comp_" + std::to_string(next_sources.size()));

                if (!compose_objects(dest, batch, options)) {
                    // Clean up everything
                    for (const auto& s : source_names) delete_object(s);
                    for (const auto& s : next_sources) delete_object(s);
                    result.success = false;
                    result.error_message = "Failed to compose objects";
                    return result;
                }

                if (!is_final) {
                    next_sources.push_back(dest);
                }

                // Delete source temp objects that are no longer needed
                for (const auto& s : batch) {
                    if (s != full_key) delete_object(s);
                }
            }

            if (next_sources.empty()) break;  // Final compose done
            source_names = std::move(next_sources);
        }

        result.success = true;
        // Fetch etag of composed object
        auto meta = head(key);
        if (meta) result.etag = meta->etag;
        increment_counters(data.size());
        return result;
    }

    bool compose_objects(const std::string& destination,
                         const std::vector<std::string>& sources,
                         const PutOptions& options) {
        auto url = api_base() + "/b/" + config_.bucket + "/o/" +
                   net::url_encode(destination) + "/compose";

        std::ostringstream json;
        json << "{\"destination\":{";
        if (!options.content_type.empty()) {
            json << "\"contentType\":\"" << options.content_type << "\"";
        }
        json << "},\"sourceObjects\":[";
        for (size_t i = 0; i < sources.size(); ++i) {
            if (i > 0) json << ",";
            json << "{\"name\":\"" << sources[i] << "\"}";
        }
        json << "]}";

        std::string body = json.str();
        net::HttpRequest request = net::HttpRequest::post(url, body);
        request.headers.set_content_type("application/json");
        add_auth_header(request);

        auto response = http_client_->execute(request);
        return response.ok();
    }

    void delete_object(const std::string& object_name) {
        auto url = api_base() + "/b/" + config_.bucket + "/o/" + net::url_encode(object_name);
        net::HttpRequest request = net::HttpRequest::del(url);
        add_auth_header(request);
        http_client_->execute(request);  // Best-effort cleanup
    }

    void cleanup_temp_objects(const std::vector<std::string>& temp_keys) {
        for (const auto& k : temp_keys) {
            delete_object(k);
        }
    }

    // Simple JSON string value parser (avoids dependency on json library)
    static std::string parse_json_string(const std::string& json, const std::string& key) {
        std::string search = "\"" + key + "\"";
        auto pos = json.find(search);
        if (pos == std::string::npos) return "";

        pos += search.size();
        // Skip whitespace and colon
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':' || json[pos] == '\t' || json[pos] == '\n' || json[pos] == '\r')) ++pos;

        if (pos >= json.size() || json[pos] != '"') return "";
        ++pos;

        std::string value;
        while (pos < json.size() && json[pos] != '"') {
            if (json[pos] == '\\' && pos + 1 < json.size()) {
                ++pos;
                switch (json[pos]) {
                    case 'n': value += '\n'; break;
                    case 't': value += '\t'; break;
                    case 'r': value += '\r'; break;
                    case '\\': value += '\\'; break;
                    case '"': value += '"'; break;
                    default: value += '\\'; value += json[pos]; break;
                }
            } else {
                value += json[pos];
            }
            ++pos;
        }

        return value;
    }

    static uint64_t parse_json_uint64(const std::string& json, const std::string& key) {
        // First try as string value (GCS returns size as string)
        auto str_val = parse_json_string(json, key);
        if (!str_val.empty()) {
            try { return std::stoull(str_val); } catch (...) {}
        }

        // Try as numeric value
        std::string search = "\"" + key + "\"";
        auto pos = json.find(search);
        if (pos == std::string::npos) return 0;

        pos += search.size();
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':' || json[pos] == '\t')) ++pos;

        if (pos < json.size() && (json[pos] >= '0' && json[pos] <= '9')) {
            try { return std::stoull(json.substr(pos)); } catch (...) {}
        }

        return 0;
    }
};

// ============================================================================
// AccessTrackingStorageBackend - Wrapper that records read access times
// ============================================================================

class AccessTrackingStorageBackend : public StorageBackend {
public:
    using AccessCallback = std::function<void(const std::string& key)>;

    AccessTrackingStorageBackend(std::unique_ptr<StorageBackend> backend,
                                  AccessCallback on_access)
        : backend_(std::move(backend))
        , on_access_(std::move(on_access)) {}

    std::string type_name() const override {
        return "access-tracked:" + backend_->type_name();
    }

    bool exists(const std::string& key) const override {
        bool result = backend_->exists(key);
        if (result && on_access_) {
            on_access_(key);
        }
        return result;
    }

    std::optional<ObjectMetadata> head(const std::string& key) const override {
        auto result = backend_->head(key);
        if (result && on_access_) {
            on_access_(key);
        }
        return result;
    }

    GetResult get(const std::string& key,
                  const GetOptions& options) const override {
        auto result = backend_->get(key, options);
        if (result.success && on_access_) {
            on_access_(key);
        }
        return result;
    }

    std::vector<std::pair<std::string, GetResult>> get_batch(
        const std::vector<std::string>& keys,
        const GetOptions& options = {},
        size_t concurrency = 8) const override {
        auto results = backend_->get_batch(keys, options, concurrency);
        if (on_access_) {
            for (const auto& [key, result] : results) {
                if (result.success) {
                    on_access_(key);
                }
            }
        }
        return results;
    }

    PutResult put(const std::string& key,
                  std::span<const uint8_t> data,
                  const PutOptions& options) override {
        return backend_->put(key, data, options);
    }

    PutResult put_file(const std::string& key,
                       const std::filesystem::path& path,
                       const PutOptions& options) override {
        return backend_->put_file(key, path, options);
    }

    bool remove(const std::string& key) override {
        return backend_->remove(key);
    }

    std::vector<std::string> remove_batch(
        const std::vector<std::string>& keys) override {
        return backend_->remove_batch(keys);
    }

    ListResult list(const ListOptions& options) const override {
        return backend_->list(options);
    }

    bool copy(const std::string& source, const std::string& destination) override {
        return backend_->copy(source, destination);
    }

    bool move(const std::string& source, const std::string& destination) override {
        return backend_->move(source, destination);
    }

    std::optional<StorageTier> get_tier(const std::string& key) const override {
        return backend_->get_tier(key);
    }

    bool set_tier(const std::string& key, StorageTier tier) override {
        return backend_->set_tier(key, tier);
    }

    bool restore(const std::string& key, std::chrono::hours duration) override {
        return backend_->restore(key, duration);
    }

    bool is_restoring(const std::string& key) const override {
        return backend_->is_restoring(key);
    }

    uint64_t total_objects() const override {
        return backend_->total_objects();
    }

    uint64_t total_bytes() const override {
        return backend_->total_bytes();
    }

    bool is_healthy() const override {
        return backend_->is_healthy();
    }

private:
    std::unique_ptr<StorageBackend> backend_;
    AccessCallback on_access_;
};

// ============================================================================
// CachingStorageBackend - Caching wrapper implementation
// ============================================================================

class CachingStorageBackend : public StorageBackend {
public:
    CachingStorageBackend(std::unique_ptr<StorageBackend> backend,
                          const std::filesystem::path& cache_path,
                          uint64_t max_cache_bytes)
        : backend_(std::move(backend))
        , cache_(std::make_unique<LocalStorageBackend>(cache_path))
        , max_cache_bytes_(max_cache_bytes) {}

    std::string type_name() const override {
        return "cached:" + backend_->type_name();
    }

    bool exists(const std::string& key) const override {
        // Check cache first
        if (cache_->exists(key)) {
            return true;
        }
        return backend_->exists(key);
    }

    std::optional<ObjectMetadata> head(const std::string& key) const override {
        // Try cache first
        auto cached = cache_->head(key);
        if (cached) {
            return cached;
        }
        return backend_->head(key);
    }

    GetResult get(const std::string& key,
                  const GetOptions& options) const override {
        // Try cache first (only for full reads without range)
        if (!options.range_start && !options.range_end) {
            auto cached = cache_->get(key, options);
            if (cached.success) {
                update_access_time(key);
                return cached;
            }
        }

        // Cache miss — try conditional GET if we have a stored ETag
        GetOptions fetch_options = options;
        std::string stored_etag;
        {
            std::lock_guard lock(lru_mutex_);
            auto it = etag_index_.find(key);
            if (it != etag_index_.end()) {
                stored_etag = it->second;
            }
        }

        // If we have an ETag and the data is still in the local cache
        // (which can happen if cache_->get() failed for range reads),
        // use conditional GET to avoid re-downloading
        if (!stored_etag.empty() && !options.range_start && !options.range_end) {
            fetch_options.if_match = stored_etag;  // If-None-Match semantics
            auto result = backend_->get(key, fetch_options);

            // 304 Not Modified — re-read from cache (data may have been
            // evicted from LocalStorageBackend but ETag index survived)
            if (!result.success && result.error_message.find("304") != std::string::npos) {
                auto cached = cache_->get(key, options);
                if (cached.success) {
                    update_access_time(key);
                    return cached;
                }
            }

            if (result.success) {
                // Cache the fresh data and update ETag
                auto* self = const_cast<CachingStorageBackend*>(this);
                self->evict_if_needed(result.data.size());
                self->cache_->put(key, result.data, {});
                {
                    std::lock_guard lock(lru_mutex_);
                    if (!result.metadata.etag.empty()) {
                        self->etag_index_[key] = result.metadata.etag;
                    }
                }
                return result;
            }
        }

        // Fetch from backend (no conditional)
        auto result = backend_->get(key, options);
        if (!result.success) {
            return result;
        }

        // Cache the result and store ETag (only for full reads)
        if (!options.range_start && !options.range_end) {
            auto* self = const_cast<CachingStorageBackend*>(this);
            self->evict_if_needed(result.data.size());
            self->cache_->put(key, result.data, {});
            if (!result.metadata.etag.empty()) {
                std::lock_guard lock(lru_mutex_);
                self->etag_index_[key] = result.metadata.etag;
            }
        }

        return result;
    }

    // Batch GET: serve cache hits locally, batch-fetch misses from backend
    std::vector<std::pair<std::string, GetResult>> get_batch(
        const std::vector<std::string>& keys,
        const GetOptions& options = {},
        size_t concurrency = 8) const override {

        std::vector<std::pair<std::string, GetResult>> results;
        results.reserve(keys.size());

        // Partition into cache hits and misses
        std::vector<size_t> miss_indices;
        for (size_t i = 0; i < keys.size(); ++i) {
            auto cached = cache_->get(keys[i], options);
            if (cached.success) {
                update_access_time(keys[i]);
                results.emplace_back(keys[i], std::move(cached));
            } else {
                // Placeholder — will be filled from backend
                results.emplace_back(keys[i], GetResult{});
                miss_indices.push_back(i);
            }
        }

        // Batch-fetch misses from backend
        if (!miss_indices.empty()) {
            std::vector<std::string> miss_keys;
            miss_keys.reserve(miss_indices.size());
            for (auto idx : miss_indices) {
                miss_keys.push_back(keys[idx]);
            }

            auto backend_results = backend_->get_batch(miss_keys, options, concurrency);

            auto* self = const_cast<CachingStorageBackend*>(this);
            for (size_t i = 0; i < backend_results.size(); ++i) {
                auto& [key, result] = backend_results[i];
                if (result.success && !options.range_start && !options.range_end) {
                    self->evict_if_needed(result.data.size());
                    self->cache_->put(key, result.data, {});
                    if (!result.metadata.etag.empty()) {
                        std::lock_guard lock(lru_mutex_);
                        self->etag_index_[key] = result.metadata.etag;
                    }
                }
                results[miss_indices[i]] = {key, std::move(result)};
            }
        }

        return results;
    }

    PutResult put(const std::string& key,
                  std::span<const uint8_t> data,
                  const PutOptions& options) override {
        // Write to backend first
        auto result = backend_->put(key, data, options);
        if (!result.success) {
            return result;
        }

        // Then cache and store ETag
        evict_if_needed(data.size());
        cache_->put(key, data, {});
        if (!result.etag.empty()) {
            std::lock_guard lock(lru_mutex_);
            etag_index_[key] = result.etag;
        }

        return result;
    }

    PutResult put_file(const std::string& key,
                       const std::filesystem::path& path,
                       const PutOptions& options) override {
        auto result = backend_->put_file(key, path, options);
        if (!result.success) {
            return result;
        }

        // Copy to cache
        auto size = std::filesystem::file_size(path);
        evict_if_needed(size);
        cache_->put_file(key, path, {});
        if (!result.etag.empty()) {
            std::lock_guard lock(lru_mutex_);
            etag_index_[key] = result.etag;
        }

        return result;
    }

    bool remove(const std::string& key) override {
        cache_->remove(key);
        {
            std::lock_guard lock(lru_mutex_);
            etag_index_.erase(key);
        }
        return backend_->remove(key);
    }

    std::vector<std::string> remove_batch(
        const std::vector<std::string>& keys) override {
        {
            std::lock_guard lock(lru_mutex_);
            for (const auto& key : keys) {
                cache_->remove(key);
                etag_index_.erase(key);
            }
        }
        return backend_->remove_batch(keys);
    }

    ListResult list(const ListOptions& options) const override {
        // List from backend (cache doesn't have complete listing)
        return backend_->list(options);
    }

    bool copy(const std::string& source, const std::string& destination) override {
        auto result = backend_->copy(source, destination);
        if (result) {
            // If source is cached, copy cache too
            if (cache_->exists(source)) {
                cache_->copy(source, destination);
            }
        }
        return result;
    }

    bool move(const std::string& source, const std::string& destination) override {
        auto result = backend_->move(source, destination);
        if (result) {
            cache_->move(source, destination);
        }
        return result;
    }

    std::optional<StorageTier> get_tier(const std::string& key) const override {
        return backend_->get_tier(key);
    }

    bool set_tier(const std::string& key, StorageTier tier) override {
        return backend_->set_tier(key, tier);
    }

    bool restore(const std::string& key, std::chrono::hours duration) override {
        return backend_->restore(key, duration);
    }

    bool is_restoring(const std::string& key) const override {
        return backend_->is_restoring(key);
    }

    uint64_t total_objects() const override {
        return backend_->total_objects();
    }

    uint64_t total_bytes() const override {
        return backend_->total_bytes();
    }

    bool is_healthy() const override {
        return backend_->is_healthy();
    }

    // Cache management
    uint64_t cache_size() const {
        return cache_->total_bytes();
    }

    uint64_t cache_objects() const {
        return cache_->total_objects();
    }

    void clear_cache() {
        // List all cached items and remove them
        ListOptions options;
        while (true) {
            auto result = cache_->list(options);
            if (!result.success) break;

            for (const auto& entry : result.entries) {
                cache_->remove(entry.key);
            }

            if (!result.truncated) break;
            options.continuation_token = result.continuation_token;
        }
    }

private:
    std::unique_ptr<StorageBackend> backend_;
    std::unique_ptr<StorageBackend> cache_;
    uint64_t max_cache_bytes_;
    mutable std::mutex lru_mutex_;
    mutable std::list<std::string> lru_list_;
    mutable std::unordered_map<std::string, std::list<std::string>::iterator> lru_map_;
    mutable std::unordered_map<std::string, std::string> etag_index_;  // key → ETag

    void update_access_time(const std::string& key) const {
        std::lock_guard lock(lru_mutex_);
        auto it = lru_map_.find(key);
        if (it != lru_map_.end()) {
            lru_list_.erase(it->second);
        }
        lru_list_.push_front(key);
        lru_map_[key] = lru_list_.begin();
    }

    void evict_if_needed(uint64_t new_bytes) {
        std::lock_guard lock(lru_mutex_);

        while (cache_->total_bytes() + new_bytes > max_cache_bytes_ &&
               !lru_list_.empty()) {
            auto key = lru_list_.back();
            lru_list_.pop_back();
            lru_map_.erase(key);
            cache_->remove(key);
        }
    }
};

// ============================================================================
// AsyncUploadBackend - Write-behind buffer for remote backends
// ============================================================================
//
// Writes go to a local staging directory (fast local I/O), then a background
// thread pool uploads them to the remote backend in parallel.  Reads check
// the staging area first, then fall through to the remote backend.

class AsyncUploadBackend : public StorageBackend {
public:
    AsyncUploadBackend(std::unique_ptr<StorageBackend> backend,
                       const std::filesystem::path& staging_path,
                       size_t upload_threads = 4)
        : backend_(std::move(backend))
        , staging_(std::make_unique<LocalStorageBackend>(staging_path))
        , shutdown_(false) {
        for (size_t i = 0; i < upload_threads; ++i) {
            workers_.emplace_back([this]() { upload_worker(); });
        }
    }

    ~AsyncUploadBackend() override {
        flush();
        {
            std::lock_guard lock(queue_mutex_);
            shutdown_ = true;
        }
        queue_cv_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
    }

    std::string type_name() const override {
        return "async:" + backend_->type_name();
    }

    bool exists(const std::string& key) const override {
        if (staging_->exists(key)) return true;
        // Also check in-flight uploads
        {
            std::lock_guard lock(queue_mutex_);
            if (inflight_.count(key)) return true;
        }
        return backend_->exists(key);
    }

    std::optional<ObjectMetadata> head(const std::string& key) const override {
        auto staged = staging_->head(key);
        if (staged) return staged;
        return backend_->head(key);
    }

    GetResult get(const std::string& key, const GetOptions& options) const override {
        // Check staging area first (may have data not yet uploaded)
        auto staged = staging_->get(key, options);
        if (staged.success) return staged;
        return backend_->get(key, options);
    }

    std::vector<std::pair<std::string, GetResult>> get_batch(
        const std::vector<std::string>& keys,
        const GetOptions& options = {},
        size_t concurrency = 8) const override {
        // Check staging for each key, batch-fetch the rest from backend
        std::vector<std::pair<std::string, GetResult>> results;
        results.reserve(keys.size());
        std::vector<size_t> miss_indices;

        for (size_t i = 0; i < keys.size(); ++i) {
            auto staged = staging_->get(keys[i], options);
            if (staged.success) {
                results.emplace_back(keys[i], std::move(staged));
            } else {
                results.emplace_back(keys[i], GetResult{});
                miss_indices.push_back(i);
            }
        }

        if (!miss_indices.empty()) {
            std::vector<std::string> miss_keys;
            miss_keys.reserve(miss_indices.size());
            for (auto idx : miss_indices) miss_keys.push_back(keys[idx]);

            auto backend_results = backend_->get_batch(miss_keys, options, concurrency);
            for (size_t i = 0; i < backend_results.size(); ++i) {
                results[miss_indices[i]] = std::move(backend_results[i]);
            }
        }

        return results;
    }

    PutResult put(const std::string& key, std::span<const uint8_t> data,
                  const PutOptions& options) override {
        // Write to local staging (fast)
        auto result = staging_->put(key, data, options);
        if (!result.success) return result;

        // Queue for async upload
        {
            std::lock_guard lock(queue_mutex_);
            upload_queue_.push(key);
            inflight_.insert(key);
        }
        queue_cv_.notify_one();

        return result;
    }

    PutResult put_file(const std::string& key, const std::filesystem::path& path,
                       const PutOptions& options) override {
        auto result = staging_->put_file(key, path, options);
        if (!result.success) return result;

        {
            std::lock_guard lock(queue_mutex_);
            upload_queue_.push(key);
            inflight_.insert(key);
        }
        queue_cv_.notify_one();

        return result;
    }

    bool remove(const std::string& key) override {
        // Wait for any in-flight upload to finish before removing, otherwise
        // the upload worker could re-upload the data after we delete it.
        flush_key(key);
        staging_->remove(key);
        return backend_->remove(key);
    }

    std::vector<std::string> remove_batch(const std::vector<std::string>& keys) override {
        for (const auto& key : keys) flush_key(key);
        for (const auto& key : keys) staging_->remove(key);
        return backend_->remove_batch(keys);
    }

    ListResult list(const ListOptions& options) const override {
        return backend_->list(options);
    }

    bool copy(const std::string& source, const std::string& destination) override {
        flush_key(source);
        return backend_->copy(source, destination);
    }

    bool move(const std::string& source, const std::string& destination) override {
        flush_key(source);
        return backend_->move(source, destination);
    }

    std::optional<StorageTier> get_tier(const std::string& key) const override {
        return backend_->get_tier(key);
    }

    bool set_tier(const std::string& key, StorageTier tier) override {
        return backend_->set_tier(key, tier);
    }

    bool restore(const std::string& key, std::chrono::hours duration) override {
        return backend_->restore(key, duration);
    }

    bool is_restoring(const std::string& key) const override {
        return backend_->is_restoring(key);
    }

    uint64_t total_objects() const override { return backend_->total_objects(); }
    uint64_t total_bytes() const override { return backend_->total_bytes(); }
    bool is_healthy() const override { return backend_->is_healthy(); }

    // Flush all pending uploads (blocks until complete)
    void flush() {
        // Wait for the queue to drain
        std::unique_lock lock(queue_mutex_);
        flush_cv_.wait(lock, [this]() {
            return upload_queue_.empty() && inflight_.empty();
        });
    }

private:
    std::unique_ptr<StorageBackend> backend_;
    std::unique_ptr<StorageBackend> staging_;

    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::condition_variable flush_cv_;
    std::queue<std::string> upload_queue_;
    std::unordered_set<std::string> inflight_;
    bool shutdown_;

    std::vector<std::thread> workers_;

    void upload_worker() {
        while (true) {
            std::string key;
            {
                std::unique_lock lock(queue_mutex_);
                queue_cv_.wait(lock, [this]() {
                    return !upload_queue_.empty() || shutdown_;
                });
                if (shutdown_ && upload_queue_.empty()) return;
                key = upload_queue_.front();
                upload_queue_.pop();
            }

            // Read from staging and upload to backend
            auto data = staging_->get(key);
            if (data.success) {
                auto put_result = backend_->put(key, data.data);
                if (put_result.success) {
                    staging_->remove(key);
                } else {
                    // Upload failed - re-queue for retry
                    std::cerr << "[async-upload] Upload failed for key=" << key
                              << " error=" << put_result.error_message
                              << ", re-queuing\n";
                    std::lock_guard lock(queue_mutex_);
                    upload_queue_.push(key);
                    // Keep in inflight_ so flush waits for retry
                    continue;
                }
            }

            {
                std::lock_guard lock(queue_mutex_);
                inflight_.erase(key);
                if (upload_queue_.empty() && inflight_.empty()) {
                    flush_cv_.notify_all();
                }
            }
        }
    }

    void flush_key(const std::string& key) {
        // If key is in flight, wait for it
        std::unique_lock lock(queue_mutex_);
        flush_cv_.wait(lock, [this, &key]() {
            return inflight_.count(key) == 0;
        });
    }
};

// ============================================================================
// StorageBackendFactory implementation
// ============================================================================

std::unique_ptr<StorageBackend> StorageBackendFactory::create(
    const std::string& type,
    const std::map<std::string, std::string>& config) {

    if (type == "local") {
        auto it = config.find("path");
        if (it == config.end()) {
            throw std::runtime_error("Local backend requires 'path' config");
        }
        return std::make_unique<LocalStorageBackend>(it->second);
    }

    if (type == "s3") {
        S3StorageBackend::Config s3_config;

        auto it = config.find("bucket");
        if (it == config.end()) {
            throw std::runtime_error("S3 backend requires 'bucket' config");
        }
        s3_config.bucket = it->second;

        if ((it = config.find("region")) != config.end()) {
            s3_config.region = it->second;
        }
        if ((it = config.find("endpoint")) != config.end()) {
            s3_config.endpoint = it->second;
        }
        if ((it = config.find("path_prefix")) != config.end()) {
            s3_config.path_prefix = it->second;
        }
        if ((it = config.find("access_key")) != config.end()) {
            s3_config.access_key = it->second;
        }
        if ((it = config.find("secret_key")) != config.end()) {
            s3_config.secret_key = it->second;
        }
        if ((it = config.find("use_path_style")) != config.end()) {
            s3_config.use_path_style = (it->second == "true" || it->second == "1");
        }
        if ((it = config.find("verify_ssl")) != config.end()) {
            s3_config.verify_ssl = (it->second == "true" || it->second == "1");
        }
        if ((it = config.find("unsigned_payload")) != config.end()) {
            s3_config.unsigned_payload = (it->second == "true" || it->second == "1");
        }
        if ((it = config.find("server_side_encryption")) != config.end()) {
            s3_config.server_side_encryption = (it->second == "true" || it->second == "1");
        }
        if ((it = config.find("sse_algorithm")) != config.end()) {
            s3_config.sse_algorithm = it->second;
        }
        if ((it = config.find("kms_key_id")) != config.end()) {
            s3_config.kms_key_id = it->second;
        }
        if ((it = config.find("session_token")) != config.end()) {
            s3_config.session_token = it->second;
        }
        if ((it = config.find("connect_timeout")) != config.end()) {
            try { s3_config.connect_timeout_secs = static_cast<uint32_t>(std::stoul(it->second)); } catch (...) {}
        }
        if ((it = config.find("request_timeout")) != config.end()) {
            try { s3_config.request_timeout_secs = static_cast<uint32_t>(std::stoul(it->second)); } catch (...) {}
        }
        if ((it = config.find("max_retries")) != config.end()) {
            try { s3_config.max_retries = static_cast<uint32_t>(std::stoul(it->second)); } catch (...) {}
        }

        return std::make_unique<S3StorageBackend>(s3_config);
    }

    if (type == "azure") {
        AzureStorageBackend::Config azure_config;

        auto it = config.find("account_name");
        if (it != config.end()) azure_config.account_name = it->second;
        if ((it = config.find("account_key")) != config.end()) azure_config.account_key = it->second;
        if ((it = config.find("sas_token")) != config.end()) azure_config.sas_token = it->second;

        it = config.find("container");
        if (it == config.end()) {
            throw std::runtime_error("Azure backend requires 'container' config");
        }
        azure_config.container = it->second;

        if ((it = config.find("endpoint")) != config.end()) azure_config.endpoint = it->second;
        if ((it = config.find("path_prefix")) != config.end()) azure_config.path_prefix = it->second;
        if ((it = config.find("verify_ssl")) != config.end()) {
            azure_config.verify_ssl = (it->second == "true" || it->second == "1");
        }
        if ((it = config.find("block_size")) != config.end()) {
            try { azure_config.block_size = std::stoull(it->second); } catch (...) {}
        }

        return std::make_unique<AzureStorageBackend>(azure_config);
    }

    if (type == "gcs") {
        GCSStorageBackend::Config gcs_config;

        auto it = config.find("bucket");
        if (it == config.end()) {
            throw std::runtime_error("GCS backend requires 'bucket' config");
        }
        gcs_config.bucket = it->second;

        if ((it = config.find("project_id")) != config.end()) gcs_config.project_id = it->second;
        if ((it = config.find("credentials_json")) != config.end()) gcs_config.credentials_json = it->second;
        if ((it = config.find("credentials_file")) != config.end()) gcs_config.credentials_file = it->second;
        if ((it = config.find("endpoint")) != config.end()) gcs_config.endpoint = it->second;
        if ((it = config.find("path_prefix")) != config.end()) gcs_config.path_prefix = it->second;
        if ((it = config.find("verify_ssl")) != config.end()) {
            gcs_config.verify_ssl = (it->second == "true" || it->second == "1");
        }

        return std::make_unique<GCSStorageBackend>(gcs_config);
    }

    throw std::runtime_error("Unknown storage backend type: " + type);
}

std::unique_ptr<StorageBackend> StorageBackendFactory::create_local(
    const std::filesystem::path& root_path) {
    return std::make_unique<LocalStorageBackend>(root_path);
}

std::unique_ptr<StorageBackend> StorageBackendFactory::create_s3(
    const std::string& bucket,
    const std::string& region,
    const std::string& endpoint,
    const std::string& access_key,
    const std::string& secret_key) {

    S3StorageBackend::Config config;
    config.bucket = bucket;
    config.region = region;
    config.endpoint = endpoint;
    config.access_key = access_key;
    config.secret_key = secret_key;

    return std::make_unique<S3StorageBackend>(config);
}

std::unique_ptr<StorageBackend> StorageBackendFactory::create_cached(
    std::unique_ptr<StorageBackend> backend,
    const std::filesystem::path& cache_path,
    uint64_t max_cache_bytes) {
    return std::make_unique<CachingStorageBackend>(
        std::move(backend), cache_path, max_cache_bytes);
}

std::unique_ptr<StorageBackend> StorageBackendFactory::create_async_upload(
    std::unique_ptr<StorageBackend> backend,
    const std::filesystem::path& staging_path,
    size_t upload_threads) {
    return std::make_unique<AsyncUploadBackend>(
        std::move(backend), staging_path, upload_threads);
}


std::unique_ptr<StorageBackend> StorageBackendFactory::create_azure(
    const std::string& account_name,
    const std::string& account_key,
    const std::string& container,
    const std::string& endpoint) {

    AzureStorageBackend::Config config;
    config.account_name = account_name;
    config.account_key = account_key;
    config.container = container;
    config.endpoint = endpoint;

    return std::make_unique<AzureStorageBackend>(config);
}

std::unique_ptr<StorageBackend> StorageBackendFactory::create_gcs(
    const std::string& bucket,
    const std::string& project_id,
    const std::string& credentials_json,
    const std::string& endpoint) {

    GCSStorageBackend::Config config;
    config.bucket = bucket;
    config.project_id = project_id;
    config.credentials_json = credentials_json;
    config.endpoint = endpoint;

    return std::make_unique<GCSStorageBackend>(config);
}

std::unique_ptr<StorageBackend> StorageBackendFactory::create_access_tracked(
    std::unique_ptr<StorageBackend> backend,
    std::function<void(const std::string&)> on_access) {
    return std::make_unique<AccessTrackingStorageBackend>(
        std::move(backend), std::move(on_access));
}

} // namespace meridian
