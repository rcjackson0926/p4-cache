#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

namespace meridian {

/**
 * Thread-safe LRU (Least Recently Used) cache with configurable capacity.
 *
 * Designed for scalability to 100+ billion entries by:
 * - Bounded memory usage with configurable max entries
 * - O(1) lookup, insert, and eviction
 * - Thread-safe with shared mutex for concurrent reads
 * - Optional size-based eviction (entry sizes can vary)
 *
 * Template parameters:
 * - Key: Key type (must be hashable)
 * - Value: Value type
 * - KeyHash: Hash function for keys (defaults to std::hash<Key>)
 *
 * Usage:
 *   LRUCache<std::string, MyData> cache(10000); // max 10000 entries
 *   cache.put("key", value);
 *   auto result = cache.get("key");
 */
template<typename Key, typename Value, typename KeyHash = std::hash<Key>>
class LRUCache {
public:
    struct CacheStats {
        size_t hits = 0;
        size_t misses = 0;
        size_t evictions = 0;
        size_t current_entries = 0;
        size_t max_entries = 0;

        double hit_rate() const {
            size_t total = hits + misses;
            return total > 0 ? static_cast<double>(hits) / total : 0.0;
        }
    };

    /**
     * Create LRU cache with maximum entry count.
     * @param max_entries Maximum number of entries (0 = unlimited, not recommended)
     */
    explicit LRUCache(size_t max_entries = 10000)
        : max_entries_(max_entries) {}

    /**
     * Get value by key. Returns nullopt if not found.
     * Moves accessed entry to front of LRU list.
     */
    std::optional<Value> get(const Key& key) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            ++stats_.misses;
            return std::nullopt;
        }

        // Move to front (most recently used)
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        ++stats_.hits;

        return it->second->second;
    }

    /**
     * Get value by key with shared lock (no LRU update).
     * Faster for read-heavy workloads where LRU precision is not critical.
     */
    std::optional<Value> peek(const Key& key) const {
        std::shared_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            return std::nullopt;
        }

        return it->second->second;
    }

    /**
     * Check if key exists without affecting LRU order.
     */
    bool contains(const Key& key) const {
        std::shared_lock lock(mutex_);
        return cache_map_.find(key) != cache_map_.end();
    }

    /**
     * Insert or update value. Evicts LRU entry if at capacity.
     */
    void put(const Key& key, const Value& value) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Update existing entry
            it->second->second = value;
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
            return;
        }

        // Evict if at capacity
        if (max_entries_ > 0 && cache_map_.size() >= max_entries_) {
            evict_lru_locked();
        }

        // Insert new entry at front
        lru_list_.emplace_front(key, value);
        cache_map_[key] = lru_list_.begin();
        stats_.current_entries = cache_map_.size();
    }

    /**
     * Insert or update value (move semantics).
     */
    void put(const Key& key, Value&& value) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Update existing entry
            it->second->second = std::move(value);
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
            return;
        }

        // Evict if at capacity
        if (max_entries_ > 0 && cache_map_.size() >= max_entries_) {
            evict_lru_locked();
        }

        // Insert new entry at front
        lru_list_.emplace_front(key, std::move(value));
        cache_map_[key] = lru_list_.begin();
        stats_.current_entries = cache_map_.size();
    }

    /**
     * Remove entry by key.
     * @return true if entry was removed, false if not found
     */
    bool remove(const Key& key) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            return false;
        }

        lru_list_.erase(it->second);
        cache_map_.erase(it);
        stats_.current_entries = cache_map_.size();
        return true;
    }

    /**
     * Clear all entries.
     */
    void clear() {
        std::unique_lock lock(mutex_);
        lru_list_.clear();
        cache_map_.clear();
        stats_.current_entries = 0;
    }

    /**
     * Get current number of entries.
     */
    size_t size() const {
        std::shared_lock lock(mutex_);
        return cache_map_.size();
    }

    /**
     * Check if cache is empty.
     */
    bool empty() const {
        std::shared_lock lock(mutex_);
        return cache_map_.empty();
    }

    /**
     * Get maximum number of entries.
     */
    size_t max_entries() const {
        return max_entries_;
    }

    /**
     * Set maximum number of entries. Evicts if over new limit.
     */
    void set_max_entries(size_t max_entries) {
        std::unique_lock lock(mutex_);
        max_entries_ = max_entries;

        // Evict if over new limit
        while (max_entries_ > 0 && cache_map_.size() > max_entries_) {
            evict_lru_locked();
        }
    }

    /**
     * Alias for set_max_entries for API compatibility.
     */
    void set_max_size(size_t max_entries) {
        set_max_entries(max_entries);
    }

    /**
     * Alias for max_entries for API compatibility.
     */
    size_t max_size() const {
        return max_entries_;
    }

    /**
     * Get cache statistics.
     */
    CacheStats stats() const {
        std::shared_lock lock(mutex_);
        CacheStats s = stats_;
        s.current_entries = cache_map_.size();
        s.max_entries = max_entries_;
        return s;
    }

    /**
     * Alias for stats() for API compatibility.
     */
    CacheStats metrics() const {
        return stats();
    }

    /**
     * Reset statistics counters.
     */
    void reset_stats() {
        std::unique_lock lock(mutex_);
        stats_.hits = 0;
        stats_.misses = 0;
        stats_.evictions = 0;
    }

    /**
     * Get or load: returns cached value or loads using provided function.
     * Thread-safe: re-checks after loading to handle concurrent loaders for the same key.
     */
    template<typename Loader>
    std::optional<Value> get_or_load(const Key& key, Loader&& loader) {
        // First check with shared lock
        {
            std::shared_lock lock(mutex_);
            auto it = cache_map_.find(key);
            if (it != cache_map_.end()) {
                // Found - but need unique lock to update LRU
                lock.unlock();
                return get(key);
            }
        }

        // Not found - load outside the lock (may be expensive)
        auto value = loader(key);
        if (value) {
            // Re-check under unique lock: another thread may have inserted
            // the same key while we were loading (TOCTOU race).
            std::unique_lock lock(mutex_);
            auto it = cache_map_.find(key);
            if (it != cache_map_.end()) {
                // Another thread already inserted - use existing value and update LRU
                lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
                ++stats_.hits;
                return it->second->second;
            }

            // Still not present - insert our loaded value
            if (max_entries_ > 0 && cache_map_.size() >= max_entries_) {
                evict_lru_locked();
            }
            lru_list_.emplace_front(key, *value);
            cache_map_[key] = lru_list_.begin();
            stats_.current_entries = cache_map_.size();
        } else {
            std::unique_lock lock(mutex_);
            ++stats_.misses;
        }
        return value;
    }

    /**
     * Iterate over all entries (for debugging/persistence).
     * Callback receives (key, value) pairs.
     */
    template<typename Callback>
    void for_each(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        for (const auto& [key, value] : lru_list_) {
            callback(key, value);
        }
    }

private:
    using ListType = std::list<std::pair<Key, Value>>;
    using MapType = std::unordered_map<Key, typename ListType::iterator, KeyHash>;

    void evict_lru_locked() {
        if (lru_list_.empty()) return;

        // Remove least recently used (back of list)
        auto& lru = lru_list_.back();
        cache_map_.erase(lru.first);
        lru_list_.pop_back();
        ++stats_.evictions;
    }

    size_t max_entries_;
    ListType lru_list_;           // Front = most recent, Back = least recent
    MapType cache_map_;           // Key -> iterator into lru_list_
    mutable std::shared_mutex mutex_;
    mutable CacheStats stats_;
};

/**
 * LRU cache with size-based eviction (for variable-sized entries).
 *
 * Each entry has an associated size, and eviction happens when
 * total size exceeds max_size_bytes.
 */
template<typename Key, typename Value, typename KeyHash = std::hash<Key>>
class SizedLRUCache {
public:
    struct CacheStats {
        size_t hits = 0;
        size_t misses = 0;
        size_t evictions = 0;
        size_t current_entries = 0;
        size_t current_size = 0;
        size_t max_size = 0;

        double hit_rate() const {
            size_t total = hits + misses;
            return total > 0 ? static_cast<double>(hits) / total : 0.0;
        }
    };

    /**
     * Create size-based LRU cache.
     * @param max_size_bytes Maximum total size of all entries
     * @param max_entries Maximum number of entries (0 = no limit on count)
     */
    explicit SizedLRUCache(size_t max_size_bytes, size_t max_entries = 0)
        : max_size_bytes_(max_size_bytes)
        , max_entries_(max_entries) {}

    /**
     * Get value by key.
     */
    std::optional<Value> get(const Key& key) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            ++stats_.misses;
            return std::nullopt;
        }

        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        ++stats_.hits;

        return it->second->value;
    }

    /**
     * Check if key exists.
     */
    bool contains(const Key& key) const {
        std::shared_lock lock(mutex_);
        return cache_map_.find(key) != cache_map_.end();
    }

    /**
     * Insert or update value with associated size.
     */
    void put(const Key& key, const Value& value, size_t entry_size) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Update existing - adjust size
            current_size_ -= it->second->size;
            it->second->value = value;
            it->second->size = entry_size;
            current_size_ += entry_size;
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        } else {
            // Evict if needed
            while ((max_size_bytes_ > 0 && current_size_ + entry_size > max_size_bytes_) ||
                   (max_entries_ > 0 && cache_map_.size() >= max_entries_)) {
                if (lru_list_.empty()) break;
                evict_lru_locked();
            }

            // Insert new entry
            lru_list_.emplace_front(Entry{key, value, entry_size});
            cache_map_[key] = lru_list_.begin();
            current_size_ += entry_size;
        }

        stats_.current_entries = cache_map_.size();
        stats_.current_size = current_size_;
    }

    /**
     * Remove entry by key.
     */
    bool remove(const Key& key) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            return false;
        }

        current_size_ -= it->second->size;
        lru_list_.erase(it->second);
        cache_map_.erase(it);

        stats_.current_entries = cache_map_.size();
        stats_.current_size = current_size_;
        return true;
    }

    /**
     * Clear all entries.
     */
    void clear() {
        std::unique_lock lock(mutex_);
        lru_list_.clear();
        cache_map_.clear();
        current_size_ = 0;
        stats_.current_entries = 0;
        stats_.current_size = 0;
    }

    size_t size() const {
        std::shared_lock lock(mutex_);
        return cache_map_.size();
    }

    size_t current_size_bytes() const {
        std::shared_lock lock(mutex_);
        return current_size_;
    }

    CacheStats stats() const {
        std::shared_lock lock(mutex_);
        CacheStats s = stats_;
        s.current_entries = cache_map_.size();
        s.current_size = current_size_;
        s.max_size = max_size_bytes_;
        return s;
    }

private:
    struct Entry {
        Key key;
        Value value;
        size_t size;
    };

    using ListType = std::list<Entry>;
    using MapType = std::unordered_map<Key, typename ListType::iterator, KeyHash>;

    void evict_lru_locked() {
        if (lru_list_.empty()) return;

        auto& lru = lru_list_.back();
        current_size_ -= lru.size;
        cache_map_.erase(lru.key);
        lru_list_.pop_back();
        ++stats_.evictions;
    }

    size_t max_size_bytes_;
    size_t max_entries_;
    size_t current_size_ = 0;
    ListType lru_list_;
    MapType cache_map_;
    mutable std::shared_mutex mutex_;
    mutable CacheStats stats_;
};

/**
 * LRU cache with TTL (time-to-live) support.
 * Entries expire after a configurable duration.
 */
template<typename Key, typename Value, typename KeyHash = std::hash<Key>>
class TTLCache {
public:
    using Clock = std::chrono::steady_clock;
    using Duration = Clock::duration;
    using TimePoint = Clock::time_point;

    struct CacheStats {
        size_t hits = 0;
        size_t misses = 0;
        size_t evictions = 0;
        size_t expirations = 0;
        size_t current_entries = 0;
        size_t max_entries = 0;

        double hit_rate() const {
            size_t total = hits + misses;
            return total > 0 ? static_cast<double>(hits) / total : 0.0;
        }
    };

    /**
     * Create TTL cache.
     * @param ttl Time-to-live for entries
     * @param max_entries Maximum number of entries (0 = no limit)
     */
    explicit TTLCache(Duration ttl, size_t max_entries = 0)
        : ttl_(ttl), max_entries_(max_entries) {}

    /**
     * Get value by key. Returns nullopt if not found or expired.
     */
    std::optional<Value> get(const Key& key) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            ++stats_.misses;
            return std::nullopt;
        }

        // Check expiration
        if (Clock::now() > it->second.expires_at) {
            // Expired - remove
            lru_list_.erase(it->second.list_it);
            cache_map_.erase(it);
            ++stats_.expirations;
            ++stats_.misses;
            return std::nullopt;
        }

        // Move to front (most recently used)
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second.list_it);
        ++stats_.hits;

        return it->second.value;
    }

    /**
     * Insert or update value.
     */
    void put(const Key& key, const Value& value) {
        std::unique_lock lock(mutex_);

        auto now = Clock::now();
        auto expires_at = now + ttl_;

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Update existing
            it->second.value = value;
            it->second.expires_at = expires_at;
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second.list_it);
            return;
        }

        // Evict if at capacity
        if (max_entries_ > 0 && cache_map_.size() >= max_entries_) {
            evict_lru_locked();
        }

        // Insert new entry
        lru_list_.push_front(key);
        cache_map_[key] = Entry{value, expires_at, lru_list_.begin()};
        stats_.current_entries = cache_map_.size();
    }

    bool remove(const Key& key) {
        std::unique_lock lock(mutex_);

        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            return false;
        }

        lru_list_.erase(it->second.list_it);
        cache_map_.erase(it);
        stats_.current_entries = cache_map_.size();
        return true;
    }

    void clear() {
        std::unique_lock lock(mutex_);
        lru_list_.clear();
        cache_map_.clear();
        stats_.current_entries = 0;
    }

    size_t size() const {
        std::shared_lock lock(mutex_);
        return cache_map_.size();
    }

    CacheStats stats() const {
        std::shared_lock lock(mutex_);
        CacheStats s = stats_;
        s.current_entries = cache_map_.size();
        s.max_entries = max_entries_;
        return s;
    }

private:
    struct Entry {
        Value value;
        TimePoint expires_at;
        typename std::list<Key>::iterator list_it;
    };

    void evict_lru_locked() {
        if (lru_list_.empty()) return;

        auto& lru_key = lru_list_.back();
        cache_map_.erase(lru_key);
        lru_list_.pop_back();
        ++stats_.evictions;
    }

    Duration ttl_;
    size_t max_entries_;
    std::list<Key> lru_list_;
    std::unordered_map<Key, Entry, KeyHash> cache_map_;
    mutable std::shared_mutex mutex_;
    mutable CacheStats stats_;
};

} // namespace meridian
