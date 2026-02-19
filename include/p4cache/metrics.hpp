#pragma once

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

namespace p4cache {

class DepotCache;
class DepotWatcher;

/// RAII timer that observes a histogram with elapsed duration on destruction.
class ScopedTimer {
public:
    explicit ScopedTimer(prometheus::Histogram& histogram)
        : histogram_(histogram)
        , start_(std::chrono::steady_clock::now()) {}

    ~ScopedTimer() {
        auto elapsed = std::chrono::steady_clock::now() - start_;
        double secs = std::chrono::duration<double>(elapsed).count();
        histogram_.Observe(secs);
    }

    ScopedTimer(const ScopedTimer&) = delete;
    ScopedTimer& operator=(const ScopedTimer&) = delete;

private:
    prometheus::Histogram& histogram_;
    std::chrono::steady_clock::time_point start_;
};

/// Exports p4-cache metrics to a Prometheus textfile for node_exporter pickup.
///
/// Owns a prometheus::Registry with all metric families. A background writer
/// thread periodically serializes the registry to a .prom file using atomic
/// temp+rename.
class MetricsExporter {
public:
    /// @param prom_file_path  Path to the .prom output file.
    /// @param write_interval  How often to write the file (default 15s).
    /// @param labels          Constant labels applied to all metrics.
    MetricsExporter(const std::filesystem::path& prom_file_path,
                    std::chrono::seconds write_interval,
                    const std::map<std::string, std::string>& labels);
    ~MetricsExporter();

    MetricsExporter(const MetricsExporter&) = delete;
    MetricsExporter& operator=(const MetricsExporter&) = delete;

    /// Set pointers for gauge snapshots.
    void set_cache(DepotCache* cache) { cache_ = cache; }
    void set_watcher(DepotWatcher* watcher) { watcher_ = watcher; }

    /// Start the background writer thread.
    void start();

    /// Stop the background writer thread (writes one final snapshot).
    void stop();

    // --- Counter accessors ---
    prometheus::Counter& uploads_success() { return *uploads_success_; }
    prometheus::Counter& uploads_failure() { return *uploads_failure_; }
    prometheus::Counter& upload_bytes_total() { return *upload_bytes_total_; }
    prometheus::Counter& restores_success() { return *restores_success_; }
    prometheus::Counter& restores_failure() { return *restores_failure_; }
    prometheus::Counter& restore_bytes_total() { return *restore_bytes_total_; }
    prometheus::Counter& restores_secondary_total() { return *restores_secondary_total_; }
    prometheus::Counter& evictions_total() { return *evictions_total_; }
    prometheus::Counter& eviction_bytes_total() { return *eviction_bytes_total_; }
    prometheus::Counter& shim_fetched() { return *shim_fetched_; }
    prometheus::Counter& shim_not_found() { return *shim_not_found_; }
    prometheus::Counter& watcher_writes() { return *watcher_writes_; }
    prometheus::Counter& watcher_errors() { return *watcher_errors_; }
    prometheus::Counter& watcher_self_filtered() { return *watcher_self_filtered_; }

    // --- Histogram accessors ---
    prometheus::Histogram& upload_duration() { return *upload_duration_; }
    prometheus::Histogram& restore_duration() { return *restore_duration_; }
    prometheus::Histogram& eviction_batch_duration() { return *eviction_batch_duration_; }
    prometheus::Histogram& shim_request_duration() { return *shim_request_duration_; }

private:
    void writer_loop();
    void update_gauges();
    void write_file();

    std::filesystem::path prom_file_path_;
    std::chrono::seconds write_interval_;

    std::shared_ptr<prometheus::Registry> registry_;

    // Pointers for gauge snapshots (not owned)
    DepotCache* cache_ = nullptr;
    DepotWatcher* watcher_ = nullptr;

    // Previous watcher stats for delta computation
    uint64_t prev_watcher_writes_ = 0;
    uint64_t prev_watcher_errors_ = 0;
    uint64_t prev_watcher_self_filtered_ = 0;

    // --- Counters ---
    prometheus::Counter* uploads_success_;
    prometheus::Counter* uploads_failure_;
    prometheus::Counter* upload_bytes_total_;
    prometheus::Counter* restores_success_;
    prometheus::Counter* restores_failure_;
    prometheus::Counter* restore_bytes_total_;
    prometheus::Counter* restores_secondary_total_;
    prometheus::Counter* evictions_total_;
    prometheus::Counter* eviction_bytes_total_;
    prometheus::Counter* shim_fetched_;
    prometheus::Counter* shim_not_found_;
    prometheus::Counter* watcher_writes_;
    prometheus::Counter* watcher_errors_;
    prometheus::Counter* watcher_self_filtered_;

    // --- Gauges ---
    prometheus::Gauge* files_dirty_;
    prometheus::Gauge* files_uploading_;
    prometheus::Gauge* files_clean_;
    prometheus::Gauge* files_total_;
    prometheus::Gauge* cache_bytes_;
    prometheus::Gauge* cache_max_bytes_;
    prometheus::Gauge* upload_queue_pending_;
    prometheus::Gauge* restore_queue_pending_;

    // --- Histograms ---
    prometheus::Histogram* upload_duration_;
    prometheus::Histogram* restore_duration_;
    prometheus::Histogram* eviction_batch_duration_;
    prometheus::Histogram* shim_request_duration_;

    // Writer thread
    std::thread writer_thread_;
    std::mutex cv_mutex_;
    std::condition_variable cv_;
    bool running_ = false;
};

}  // namespace p4cache
