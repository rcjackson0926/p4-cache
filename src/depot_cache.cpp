#include "p4cache/depot_cache.hpp"
#include "meridian/storage/backend.hpp"
#include "meridian/core/thread_pool.hpp"

#include <cinttypes>
#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <lmdb.h>
#include <sstream>
#include <vector>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace p4cache {

namespace {

// File states stored in LMDB FileEntry
enum class FileState : uint8_t {
    Dirty = 0,
    Uploading = 1,
    Clean = 2,
};

// FileEntry value format (packed, variable length):
//   [8B size][8B last_access][8B created_at][1B state][storage_key bytes...]
struct FileEntry {
    uint64_t size = 0;
    uint64_t last_access = 0;
    uint64_t created_at = 0;
    FileState state = FileState::Dirty;
    std::string storage_key;
};

std::vector<uint8_t> encode_file_entry(const FileEntry& e) {
    std::vector<uint8_t> buf(25 + e.storage_key.size());
    memcpy(buf.data(), &e.size, 8);
    memcpy(buf.data() + 8, &e.last_access, 8);
    memcpy(buf.data() + 16, &e.created_at, 8);
    buf[24] = static_cast<uint8_t>(e.state);
    memcpy(buf.data() + 25, e.storage_key.data(), e.storage_key.size());
    return buf;
}

FileEntry decode_file_entry(MDB_val val) {
    FileEntry e;
    if (val.mv_size < 25) return e;
    const uint8_t* p = static_cast<const uint8_t*>(val.mv_data);
    memcpy(&e.size, p, 8);
    memcpy(&e.last_access, p + 8, 8);
    memcpy(&e.created_at, p + 16, 8);
    e.state = static_cast<FileState>(p[24]);
    if (val.mv_size > 25) {
        e.storage_key.assign(reinterpret_cast<const char*>(p + 25), val.mv_size - 25);
    }
    return e;
}

// Encode uint64_t as big-endian for LMDB key sorting
void encode_be64(uint8_t* buf, uint64_t val) {
    buf[0] = (val >> 56) & 0xFF;
    buf[1] = (val >> 48) & 0xFF;
    buf[2] = (val >> 40) & 0xFF;
    buf[3] = (val >> 32) & 0xFF;
    buf[4] = (val >> 24) & 0xFF;
    buf[5] = (val >> 16) & 0xFF;
    buf[6] = (val >> 8) & 0xFF;
    buf[7] = val & 0xFF;
}

uint64_t decode_be64(const uint8_t* buf) {
    return (uint64_t(buf[0]) << 56) | (uint64_t(buf[1]) << 48) |
           (uint64_t(buf[2]) << 40) | (uint64_t(buf[3]) << 32) |
           (uint64_t(buf[4]) << 24) | (uint64_t(buf[5]) << 16) |
           (uint64_t(buf[6]) << 8)  |  uint64_t(buf[7]);
}

// Build composite index key: [8B big-endian timestamp][path bytes]
std::vector<uint8_t> make_index_key(uint64_t timestamp, const std::string& path) {
    std::vector<uint8_t> key(8 + path.size());
    encode_be64(key.data(), timestamp);
    memcpy(key.data() + 8, path.data(), path.size());
    return key;
}

// Remove index entries for a file entry from the appropriate index DB
void remove_indexes(MDB_txn* txn, MDB_dbi dbi_dirty, MDB_dbi dbi_evict,
                    const std::string& path, const FileEntry& entry) {
    if (entry.state == FileState::Dirty) {
        auto key = make_index_key(entry.created_at, path);
        MDB_val k = {key.size(), key.data()};
        mdb_del(txn, dbi_dirty, &k, nullptr);
    } else if (entry.state == FileState::Clean) {
        auto key = make_index_key(entry.last_access, path);
        MDB_val k = {key.size(), key.data()};
        mdb_del(txn, dbi_evict, &k, nullptr);
    }
    // Uploading state has no index entry
}

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

// FNV-1a 64-bit hash (deterministic, portable)
uint64_t fnv1a_64(const std::string& s) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : s) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return hash;
}

}  // namespace

// --- Azure key sanitization ---

std::string sanitize_azure_key(const std::string& input) {
    // Step 1: Normalize backslashes to forward slashes, then split into segments.
    // Steps 2-3: Percent-encode control chars (0x00-0x1F, 0x7F) and
    //            C1 control code points (U+0080-U+009F, UTF-8: 0xC2 0x80-0x9F).
    // Step 5: Remove empty segments (from // or trailing /).
    std::vector<std::string> segments;
    std::string current;

    for (size_t i = 0; i < input.size(); ++i) {
        unsigned char uc = static_cast<unsigned char>(input[i]);
        char c = input[i];

        // Treat backslash as segment separator (same as forward slash)
        if (c == '/' || c == '\\') {
            if (!current.empty()) {
                segments.push_back(std::move(current));
                current.clear();
            }
            continue;
        }

        // C1 control code points: UTF-8 bytes 0xC2 0x80 through 0xC2 0x9F
        if (uc == 0xC2 && i + 1 < input.size()) {
            unsigned char next = static_cast<unsigned char>(input[i + 1]);
            if (next >= 0x80 && next <= 0x9F) {
                char hex[8];
                snprintf(hex, sizeof(hex), "%%C2%%%02X", next);
                current += hex;
                ++i;
                continue;
            }
        }

        // C0 control chars (0x00-0x1F) and DEL (0x7F)
        if (uc <= 0x1F || uc == 0x7F) {
            char hex[4];
            snprintf(hex, sizeof(hex), "%%%02X", uc);
            current += hex;
        } else {
            current += c;
        }
    }
    if (!current.empty()) {
        segments.push_back(std::move(current));
    }

    // Step 4: Encode trailing dots on each path segment.
    // Only the final '.' is encoded as %2E (e.g. "bar.." → "bar.%2E").
    for (auto& seg : segments) {
        if (!seg.empty() && seg.back() == '.') {
            seg.replace(seg.size() - 1, 1, "%2E");
        }
    }

    // Step 7: Enforce 254 path segment limit.
    // Flatten excess segments into the last allowed segment by replacing '/' with '_'.
    if (segments.size() > 254) {
        std::string combined;
        for (size_t i = 253; i < segments.size(); ++i) {
            if (i > 253) combined += '_';
            combined += segments[i];
        }
        segments.resize(254);
        segments[253] = combined;
    }

    // Join segments with '/'
    std::string result;
    for (size_t i = 0; i < segments.size(); ++i) {
        if (i > 0) result += '/';
        result += segments[i];
    }

    // Step 6: Enforce 1024-char limit.
    // Truncate to 1007 chars + '_' + 16-char hex hash of the full key.
    if (result.size() > 1024) {
        uint64_t h = fnv1a_64(result);
        char hash_buf[17];
        snprintf(hash_buf, sizeof(hash_buf), "%016" PRIx64, h);
        result = result.substr(0, 1007) + "_" + std::string(hash_buf, 16);
    }

    return result;
}

// --- Build backend from config ---

std::unique_ptr<meridian::StorageBackend> DepotCache::build_backend(const BackendConfig& cfg) {
    // Map "nfs" to the "local" backend type that StorageBackendFactory expects
    std::string factory_type = (cfg.type == "nfs") ? "local" : cfg.type;
    return meridian::StorageBackendFactory::create(factory_type, cfg.params);
}

DepotCache::DepotCache(const CacheConfig& config) : config_(config) {}

DepotCache::~DepotCache() {
    stop();

    if (mdb_env_) {
        mdb_env_close(mdb_env_);
        mdb_env_ = nullptr;
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

    // Initialize LMDB manifest
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

    // Initialize in-memory counters from LMDB
    init_counters();

    log_info("Cache initialized: %lu bytes tracked, read-only=%s",
             cache_bytes_.load(), config_.read_only ? "true" : "false");

    // Scan for untracked files (files written while daemon was down)
    if (!config_.read_only && !config_.skip_startup_scan) {
        scan_untracked_files();
    }

    running_ = true;

    // Start restore pool (used in both read-write and read-only modes)
    restore_pool_ = std::make_unique<meridian::ThreadPool>(config_.restore_threads);

    // Start upload coordinator (read-write mode only).
    // Single coordinator thread fetches dirty batches and dispatches to upload pool.
    if (!config_.read_only) {
        upload_pool_ = std::make_unique<meridian::ThreadPool>(config_.upload_concurrency);
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
    auto manifest_dir = config_.state_dir / "manifest";
    std::filesystem::create_directories(manifest_dir);

    int rc = mdb_env_create(&mdb_env_);
    if (rc) {
        throw std::runtime_error("mdb_env_create: " + std::string(mdb_strerror(rc)));
    }

    // 1 TB map size (virtual address space only; actual usage is sparse)
    rc = mdb_env_set_mapsize(mdb_env_, 1ULL * 1024 * 1024 * 1024 * 1024);
    if (rc) {
        mdb_env_close(mdb_env_);
        mdb_env_ = nullptr;
        throw std::runtime_error("mdb_env_set_mapsize: " + std::string(mdb_strerror(rc)));
    }

    // 3 named databases: files, dirty_queue, evict_order
    mdb_env_set_maxdbs(mdb_env_, 3);

    rc = mdb_env_open(mdb_env_, manifest_dir.c_str(), 0, 0664);
    if (rc) {
        mdb_env_close(mdb_env_);
        mdb_env_ = nullptr;
        throw std::runtime_error("mdb_env_open: " + std::string(mdb_strerror(rc)));
    }

    // Open named databases within a single transaction
    MDB_txn* txn = nullptr;
    rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
    if (rc) {
        throw std::runtime_error("mdb_txn_begin: " + std::string(mdb_strerror(rc)));
    }

    rc = mdb_dbi_open(txn, "files", MDB_CREATE, &dbi_files_);
    if (rc) { mdb_txn_abort(txn); throw std::runtime_error("open files db: " + std::string(mdb_strerror(rc))); }

    rc = mdb_dbi_open(txn, "dirty_queue", MDB_CREATE, &dbi_dirty_);
    if (rc) { mdb_txn_abort(txn); throw std::runtime_error("open dirty_queue db: " + std::string(mdb_strerror(rc))); }

    rc = mdb_dbi_open(txn, "evict_order", MDB_CREATE, &dbi_evict_);
    if (rc) { mdb_txn_abort(txn); throw std::runtime_error("open evict_order db: " + std::string(mdb_strerror(rc))); }

    rc = mdb_txn_commit(txn);
    if (rc) {
        throw std::runtime_error("mdb_txn_commit: " + std::string(mdb_strerror(rc)));
    }
}

void DepotCache::init_counters() {
    uint64_t dirty = 0, uploading = 0, clean = 0, bytes = 0;

    MDB_txn* txn = nullptr;
    int rc = mdb_txn_begin(mdb_env_, nullptr, MDB_RDONLY, &txn);
    if (rc) return;

    MDB_cursor* cursor = nullptr;
    rc = mdb_cursor_open(txn, dbi_files_, &cursor);
    if (rc) { mdb_txn_abort(txn); return; }

    MDB_val key, val;
    while (mdb_cursor_get(cursor, &key, &val, MDB_NEXT) == 0) {
        auto entry = decode_file_entry(val);
        bytes += entry.size;
        switch (entry.state) {
            case FileState::Dirty: ++dirty; break;
            case FileState::Uploading: ++uploading; break;
            case FileState::Clean: ++clean; break;
        }
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    count_dirty_ = dirty;
    count_uploading_ = uploading;
    count_clean_ = clean;
    cache_bytes_ = bytes;
}

void DepotCache::crash_recovery() {
    // Reset any 'uploading' entries back to 'dirty' (may not have completed)
    std::lock_guard<std::mutex> db_lock(db_mutex_);

    MDB_txn* txn = nullptr;
    int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
    if (rc) return;

    MDB_cursor* cursor = nullptr;
    rc = mdb_cursor_open(txn, dbi_files_, &cursor);
    if (rc) { mdb_txn_abort(txn); return; }

    size_t reset_count = 0;
    MDB_val key, val;
    while (mdb_cursor_get(cursor, &key, &val, MDB_NEXT) == 0) {
        auto entry = decode_file_entry(val);
        if (entry.state != FileState::Uploading) continue;

        std::string path(static_cast<const char*>(key.mv_data), key.mv_size);
        auto now = now_epoch();

        // uploading → dirty
        entry.state = FileState::Dirty;
        entry.created_at = now;
        auto new_val = encode_file_entry(entry);
        MDB_val nv = {new_val.size(), new_val.data()};
        mdb_cursor_put(cursor, &key, &nv, MDB_CURRENT);

        // Add to dirty_queue
        auto dk = make_index_key(static_cast<uint64_t>(now), path);
        MDB_val dkv = {dk.size(), dk.data()};
        MDB_val empty = {0, nullptr};
        mdb_put(txn, dbi_dirty_, &dkv, &empty, 0);

        ++reset_count;
    }

    mdb_cursor_close(cursor);
    rc = mdb_txn_commit(txn);
    if (rc) {
        log_error("Crash recovery commit failed: %s", mdb_strerror(rc));
    }

    log_info("Crash recovery: reset %zu uploading entries to dirty", reset_count);
}

void DepotCache::scan_untracked_files() {
    size_t found = 0;
    auto now = now_epoch();

    // Collect untracked files first, then batch-insert
    struct UntrackedFile {
        std::string rel_path;
        std::string storage_key;
        uint64_t file_size;
    };
    std::vector<UntrackedFile> untracked;

    // Read-only scan: check each file against LMDB
    for (auto& entry : std::filesystem::recursive_directory_iterator(
             config_.depot_path,
             std::filesystem::directory_options::skip_permission_denied)) {
        if (!entry.is_regular_file()) continue;

        auto path = entry.path();
        auto rel = relative_path(path);

        // Skip .p4cache directory
        if (rel.compare(0, 9, ".p4cache/") == 0 || rel == ".p4cache") continue;

        // Check if already tracked (read-only txn, no mutex needed)
        MDB_txn* rtxn = nullptr;
        int rc = mdb_txn_begin(mdb_env_, nullptr, MDB_RDONLY, &rtxn);
        if (rc) continue;

        MDB_val k = {rel.size(), const_cast<char*>(rel.data())};
        MDB_val v;
        rc = mdb_get(rtxn, dbi_files_, &k, &v);
        mdb_txn_abort(rtxn);

        if (rc == 0) continue;  // Already tracked

        UntrackedFile uf;
        uf.rel_path = rel;
        uf.storage_key = make_storage_key(rel);
        uf.file_size = entry.file_size();
        untracked.push_back(std::move(uf));
    }

    if (untracked.empty()) return;

    // Batch insert under write lock
    {
        std::lock_guard<std::mutex> db_lock(db_mutex_);

        MDB_txn* txn = nullptr;
        int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
        if (rc) return;

        for (auto& uf : untracked) {
            FileEntry fe;
            fe.size = uf.file_size;
            fe.last_access = static_cast<uint64_t>(now);
            fe.created_at = static_cast<uint64_t>(now);
            fe.state = FileState::Dirty;
            fe.storage_key = uf.storage_key;

            auto val = encode_file_entry(fe);
            MDB_val k = {uf.rel_path.size(), const_cast<char*>(uf.rel_path.data())};
            MDB_val v = {val.size(), val.data()};
            mdb_put(txn, dbi_files_, &k, &v, 0);

            // Add to dirty_queue
            auto dk = make_index_key(fe.created_at, uf.rel_path);
            MDB_val dkv = {dk.size(), dk.data()};
            MDB_val empty = {0, nullptr};
            mdb_put(txn, dbi_dirty_, &dkv, &empty, 0);

            cache_bytes_ += uf.file_size;
            count_dirty_++;
            ++found;
        }

        rc = mdb_txn_commit(txn);
        if (rc) {
            log_error("scan_untracked_files commit failed: %s", mdb_strerror(rc));
        }
    }

    if (found > 0) {
        log_info("Found %zu untracked files, registered as dirty", found);
    }
}

// --- Path helpers ---

std::string DepotCache::relative_path(const std::filesystem::path& abs_path) const {
    auto rel = std::filesystem::relative(abs_path, config_.depot_path);
    return rel.string();
}

std::string DepotCache::make_storage_key(const std::string& rel_path) const {
    // Apply Azure blob name sanitization when Azure is configured as either backend.
    // Sanitized keys are safe for all backends (only removes genuinely problematic
    // characters), so cross-backend scenarios work correctly.
    if (config_.primary.type == "azure" || config_.secondary.type == "azure") {
        return sanitize_azure_key(rel_path);
    }
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

    int64_t old_size = 0;
    bool had_entry = false;
    FileState old_state = FileState::Dirty;

    {
        std::lock_guard<std::mutex> db_lock(db_mutex_);

        MDB_txn* txn = nullptr;
        int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
        if (rc) return;

        MDB_val k = {rel.size(), const_cast<char*>(rel.data())};
        MDB_val v;

        // Check for existing entry
        rc = mdb_get(txn, dbi_files_, &k, &v);
        if (rc == 0) {
            auto entry = decode_file_entry(v);
            old_size = static_cast<int64_t>(entry.size);
            old_state = entry.state;
            had_entry = true;

            // Remove old index entries
            remove_indexes(txn, dbi_dirty_, dbi_evict_, rel, entry);
        }

        // Create new entry as dirty
        FileEntry new_entry;
        new_entry.size = file_size;
        new_entry.last_access = static_cast<uint64_t>(now);
        new_entry.created_at = static_cast<uint64_t>(now);
        new_entry.state = FileState::Dirty;
        new_entry.storage_key = storage_key;

        auto val = encode_file_entry(new_entry);
        MDB_val nv = {val.size(), val.data()};
        mdb_put(txn, dbi_files_, &k, &nv, 0);

        // Add to dirty_queue
        auto dk = make_index_key(new_entry.created_at, rel);
        MDB_val dkv = {dk.size(), dk.data()};
        MDB_val empty = {0, nullptr};
        mdb_put(txn, dbi_dirty_, &dkv, &empty, 0);

        rc = mdb_txn_commit(txn);
        if (rc) return;
    }

    // Update in-memory counters
    if (had_entry) {
        switch (old_state) {
            case FileState::Dirty: count_dirty_--; break;
            case FileState::Uploading: count_uploading_--; break;
            case FileState::Clean: count_clean_--; break;
        }
    }
    count_dirty_++;

    cache_bytes_ -= static_cast<uint64_t>(old_size);
    cache_bytes_ += file_size;

    // Wake up the upload coordinator
    upload_cv_.notify_one();

    // Check if we need to evict
    maybe_trigger_eviction();
}

bool DepotCache::on_file_open(const std::filesystem::path& path) {
    auto rel = relative_path(path);
    auto now = now_epoch();

    // Update LRU timestamp if tracked as clean
    {
        std::lock_guard<std::mutex> db_lock(db_mutex_);

        MDB_txn* txn = nullptr;
        int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
        if (rc) return true;

        MDB_val k = {rel.size(), const_cast<char*>(rel.data())};
        MDB_val v;
        rc = mdb_get(txn, dbi_files_, &k, &v);
        if (rc != 0) {
            mdb_txn_abort(txn);
            return true;  // Not tracked
        }

        auto entry = decode_file_entry(v);

        if (entry.state == FileState::Clean) {
            // Remove old evict_order entry
            auto old_ek = make_index_key(entry.last_access, rel);
            MDB_val oek = {old_ek.size(), old_ek.data()};
            mdb_del(txn, dbi_evict_, &oek, nullptr);

            // Update entry with new access time
            entry.last_access = static_cast<uint64_t>(now);
            auto new_val = encode_file_entry(entry);
            MDB_val nv = {new_val.size(), new_val.data()};
            mdb_put(txn, dbi_files_, &k, &nv, 0);

            // Add new evict_order entry
            auto new_ek = make_index_key(entry.last_access, rel);
            MDB_val nek = {new_ek.size(), new_ek.data()};
            MDB_val empty = {0, nullptr};
            mdb_put(txn, dbi_evict_, &nek, &empty, 0);
        }

        mdb_txn_commit(txn);
    }

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

            MDB_txn* txn = nullptr;
            int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
            if (rc) continue;

            // Collect dirty entries from the dirty_queue index
            MDB_cursor* cursor = nullptr;
            rc = mdb_cursor_open(txn, dbi_dirty_, &cursor);
            if (rc) { mdb_txn_abort(txn); continue; }

            struct QueueEntry {
                std::vector<uint8_t> index_key;  // For deletion
                std::string path;
            };
            std::vector<QueueEntry> queue_entries;

            MDB_val key, val;
            MDB_cursor_op op = MDB_FIRST;
            while (queue_entries.size() < config_.upload_batch_size &&
                   mdb_cursor_get(cursor, &key, &val, op) == 0) {
                op = MDB_NEXT;
                if (key.mv_size <= 8) continue;

                QueueEntry qe;
                qe.index_key.assign(static_cast<uint8_t*>(key.mv_data),
                                    static_cast<uint8_t*>(key.mv_data) + key.mv_size);
                qe.path.assign(static_cast<const char*>(key.mv_data) + 8, key.mv_size - 8);
                queue_entries.push_back(std::move(qe));
            }
            mdb_cursor_close(cursor);

            // For each queue entry: verify it's still dirty, mark as uploading
            for (auto& qe : queue_entries) {
                MDB_val fk = {qe.path.size(), const_cast<char*>(qe.path.data())};
                MDB_val fv;
                rc = mdb_get(txn, dbi_files_, &fk, &fv);
                if (rc != 0) {
                    // Stale index entry — delete it
                    MDB_val dk = {qe.index_key.size(), qe.index_key.data()};
                    mdb_del(txn, dbi_dirty_, &dk, nullptr);
                    continue;
                }

                auto entry = decode_file_entry(fv);
                if (entry.state != FileState::Dirty) {
                    // Stale index entry — delete it
                    MDB_val dk = {qe.index_key.size(), qe.index_key.data()};
                    mdb_del(txn, dbi_dirty_, &dk, nullptr);
                    continue;
                }

                // Mark as uploading
                entry.state = FileState::Uploading;
                auto new_val = encode_file_entry(entry);
                MDB_val nv = {new_val.size(), new_val.data()};
                mdb_put(txn, dbi_files_, &fk, &nv, 0);

                // Delete from dirty_queue
                MDB_val dk = {qe.index_key.size(), qe.index_key.data()};
                mdb_del(txn, dbi_dirty_, &dk, nullptr);

                DirtyEntry de;
                de.path = qe.path;
                de.storage_key = entry.storage_key;
                batch.push_back(std::move(de));
            }

            rc = mdb_txn_commit(txn);
            if (rc) {
                log_error("upload batch commit failed: %s", mdb_strerror(rc));
                continue;
            }

            // Update counters
            count_dirty_ -= batch.size();
            count_uploading_ += batch.size();
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

            MDB_txn* txn = nullptr;
            int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
            if (rc) continue;

            size_t succeeded = 0, failed = 0;
            auto now = now_epoch();

            for (size_t i = 0; i < results.size(); ++i) {
                auto& path = batch[i].path;
                MDB_val fk = {path.size(), const_cast<char*>(path.data())};
                MDB_val fv;
                rc = mdb_get(txn, dbi_files_, &fk, &fv);
                if (rc != 0) continue;

                auto entry = decode_file_entry(fv);

                if (results[i]) {
                    // Success: uploading → clean
                    entry.state = FileState::Clean;
                    entry.last_access = static_cast<uint64_t>(now);
                    auto new_val = encode_file_entry(entry);
                    MDB_val nv = {new_val.size(), new_val.data()};
                    mdb_put(txn, dbi_files_, &fk, &nv, 0);

                    // Add to evict_order
                    auto ek = make_index_key(entry.last_access, path);
                    MDB_val ekv = {ek.size(), ek.data()};
                    MDB_val empty = {0, nullptr};
                    mdb_put(txn, dbi_evict_, &ekv, &empty, 0);

                    ++succeeded;
                } else {
                    // Failure: uploading → dirty
                    entry.state = FileState::Dirty;
                    entry.created_at = static_cast<uint64_t>(now);
                    auto new_val = encode_file_entry(entry);
                    MDB_val nv = {new_val.size(), new_val.data()};
                    mdb_put(txn, dbi_files_, &fk, &nv, 0);

                    // Add back to dirty_queue
                    auto dk = make_index_key(entry.created_at, path);
                    MDB_val dkv = {dk.size(), dk.data()};
                    MDB_val empty = {0, nullptr};
                    mdb_put(txn, dbi_dirty_, &dkv, &empty, 0);

                    ++failed;
                }

                {
                    std::lock_guard lock(stats_mutex_);
                    if (results[i]) {
                        stats_.uploads_completed++;
                    } else {
                        stats_.uploads_failed++;
                    }
                }
            }

            rc = mdb_txn_commit(txn);
            if (rc) {
                log_error("upload results commit failed: %s", mdb_strerror(rc));
            }

            // Update counters
            count_uploading_ -= results.size();
            count_clean_ += succeeded;
            count_dirty_ += failed;
        }
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
            // Phase 0: Fetch candidates (oldest clean files) — read-only, no mutex
            struct EvictCandidate {
                std::string path;
                uint64_t size;
                uint64_t last_access;
            };
            std::vector<EvictCandidate> candidates;

            {
                MDB_txn* rtxn = nullptr;
                int rc = mdb_txn_begin(mdb_env_, nullptr, MDB_RDONLY, &rtxn);
                if (rc) break;

                MDB_cursor* cursor = nullptr;
                rc = mdb_cursor_open(rtxn, dbi_evict_, &cursor);
                if (rc) { mdb_txn_abort(rtxn); break; }

                MDB_val key, val;
                MDB_cursor_op op = MDB_FIRST;
                while (candidates.size() < 100 &&
                       mdb_cursor_get(cursor, &key, &val, op) == 0) {
                    op = MDB_NEXT;
                    if (key.mv_size <= 8) continue;

                    EvictCandidate c;
                    c.last_access = decode_be64(static_cast<const uint8_t*>(key.mv_data));
                    c.path.assign(static_cast<const char*>(key.mv_data) + 8, key.mv_size - 8);

                    // Look up size from files DB
                    MDB_val fk = {c.path.size(), const_cast<char*>(c.path.data())};
                    MDB_val fv;
                    rc = mdb_get(rtxn, dbi_files_, &fk, &fv);
                    if (rc == 0) {
                        auto entry = decode_file_entry(fv);
                        c.size = entry.size;
                        candidates.push_back(std::move(c));
                    }
                }

                mdb_cursor_close(cursor);
                mdb_txn_abort(rtxn);
            }

            if (candidates.empty()) {
                log_info("No clean files to evict, waiting for uploads");
                break;
            }

            // Phase 1: Delete files from disk (no lock needed)
            struct EvictedFile {
                std::string path;
                uint64_t last_access;
            };
            std::vector<EvictedFile> unlinked;

            for (auto& c : candidates) {
                auto full_path = config_.depot_path / c.path;
                int rc = ::unlink(full_path.c_str());
                if (rc == 0 || errno == ENOENT) {
                    // Remove empty parent directories up to depot_path
                    auto parent = full_path.parent_path();
                    while (parent != config_.depot_path && parent.has_parent_path()) {
                        if (::rmdir(parent.c_str()) != 0) break;
                        parent = parent.parent_path();
                    }
                    unlinked.push_back({c.path, c.last_access});
                }
            }

            // Phase 2: Delete DB entries (under write lock)
            if (!unlinked.empty()) {
                std::lock_guard<std::mutex> db_lock(db_mutex_);

                MDB_txn* txn = nullptr;
                int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
                if (rc) break;

                for (auto& ef : unlinked) {
                    MDB_val fk = {ef.path.size(), const_cast<char*>(ef.path.data())};
                    MDB_val fv;
                    rc = mdb_get(txn, dbi_files_, &fk, &fv);
                    if (rc != 0) continue;  // Already deleted

                    auto entry = decode_file_entry(fv);
                    if (entry.state != FileState::Clean) continue;  // State changed, skip

                    // Delete from files DB
                    mdb_del(txn, dbi_files_, &fk, nullptr);

                    // Delete from evict_order
                    auto ek = make_index_key(entry.last_access, ef.path);
                    MDB_val ekv = {ek.size(), ek.data()};
                    mdb_del(txn, dbi_evict_, &ekv, nullptr);

                    cache_bytes_ -= entry.size;
                    count_clean_--;

                    {
                        std::lock_guard lock(stats_mutex_);
                        stats_.evictions_performed++;
                    }

                    if (config_.verbose) {
                        log_info("Evicted: %s (%lu bytes)", ef.path.c_str(), entry.size);
                    }
                }

                rc = mdb_txn_commit(txn);
                if (rc) {
                    log_error("eviction commit failed: %s", mdb_strerror(rc));
                }
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
                            // Insert into manifest as clean
                            auto now = now_epoch();
                            std::lock_guard<std::mutex> db_lock(db_mutex_);

                            MDB_txn* txn = nullptr;
                            int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
                            if (rc == 0) {
                                FileEntry fe;
                                fe.size = get_result.data.size();
                                fe.last_access = static_cast<uint64_t>(now);
                                fe.created_at = static_cast<uint64_t>(now);
                                fe.state = FileState::Clean;
                                fe.storage_key = key_copy;

                                auto val = encode_file_entry(fe);
                                MDB_val k = {path_copy.size(), const_cast<char*>(path_copy.data())};
                                MDB_val v = {val.size(), val.data()};
                                mdb_put(txn, dbi_files_, &k, &v, 0);

                                // Add to evict_order
                                auto ek = make_index_key(fe.last_access, path_copy);
                                MDB_val ekv = {ek.size(), ek.data()};
                                MDB_val empty = {0, nullptr};
                                mdb_put(txn, dbi_evict_, &ekv, &empty, 0);

                                mdb_txn_commit(txn);
                            }

                            cache_bytes_ += get_result.data.size();
                            count_clean_++;
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

    // Wait for the restore to complete
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

    // Check manifest for known storage key (read-only txn, no mutex)
    std::string storage_key;
    {
        MDB_txn* rtxn = nullptr;
        int rc = mdb_txn_begin(mdb_env_, nullptr, MDB_RDONLY, &rtxn);
        if (rc == 0) {
            MDB_val k = {relative_path.size(), const_cast<char*>(relative_path.data())};
            MDB_val v;
            rc = mdb_get(rtxn, dbi_files_, &k, &v);
            if (rc == 0) {
                auto entry = decode_file_entry(v);
                storage_key = entry.storage_key;
            }
            mdb_txn_abort(rtxn);
        }
        if (storage_key.empty()) {
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

        MDB_txn* txn = nullptr;
        int rc = mdb_txn_begin(mdb_env_, nullptr, 0, &txn);
        if (rc == 0) {
            // Check for existing entry to remove old indexes
            MDB_val k = {relative_path.size(), const_cast<char*>(relative_path.data())};
            MDB_val v;
            rc = mdb_get(txn, dbi_files_, &k, &v);
            if (rc == 0) {
                auto old_entry = decode_file_entry(v);
                remove_indexes(txn, dbi_dirty_, dbi_evict_, relative_path, old_entry);

                // Adjust counters for old entry
                switch (old_entry.state) {
                    case FileState::Dirty: count_dirty_--; break;
                    case FileState::Uploading: count_uploading_--; break;
                    case FileState::Clean: count_clean_--; break;
                }
                cache_bytes_ -= old_entry.size;
            }

            FileEntry fe;
            fe.size = get_result.data.size();
            fe.last_access = static_cast<uint64_t>(now);
            fe.created_at = static_cast<uint64_t>(now);
            fe.state = FileState::Clean;
            fe.storage_key = storage_key;

            auto val = encode_file_entry(fe);
            MDB_val nv = {val.size(), val.data()};
            mdb_put(txn, dbi_files_, &k, &nv, 0);

            // Add to evict_order
            auto ek = make_index_key(fe.last_access, relative_path);
            MDB_val ekv = {ek.size(), ek.data()};
            MDB_val empty = {0, nullptr};
            mdb_put(txn, dbi_evict_, &ekv, &empty, 0);

            mdb_txn_commit(txn);
        }
    }

    cache_bytes_ += get_result.data.size();
    count_clean_++;
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

        // Read in-memory counters (no DB query needed)
        uint64_t dirty = count_dirty_.load();
        uint64_t uploading = count_uploading_.load();
        uint64_t clean = count_clean_.load();
        uint64_t total = dirty + uploading + clean;

        auto s = get_stats();
        double cache_gb = static_cast<double>(cache_bytes_.load()) / (1024.0 * 1024 * 1024);
        double max_gb = static_cast<double>(config_.max_cache_bytes) / (1024.0 * 1024 * 1024);

        log_info("[stats] cache=%.1f/%.1f GB | files: %lu total, %lu dirty, %lu uploading, "
                 "%lu clean | uploads: %lu ok, %lu fail | restores: %lu ok, "
                 "%lu fail (%lu secondary) | evictions: %lu | shim: %lu fetch, %lu miss",
                 cache_gb, max_gb, total, dirty, uploading, clean,
                 s.uploads_completed, s.uploads_failed,
                 s.restores_completed, s.restores_failed, s.secondary_restores,
                 s.evictions_performed,
                 s.shim_fetches, s.shim_not_found);
    }
}

}  // namespace p4cache
