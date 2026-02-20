#include "p4cache/metrics.hpp"
#include "p4cache/depot_cache.hpp"
#include "p4cache/depot_watcher.hpp"

#include <fstream>
#include <prometheus/text_serializer.h>

namespace p4cache {

MetricsExporter::MetricsExporter(const std::filesystem::path& prom_file_path,
                                 std::chrono::seconds write_interval,
                                 const std::map<std::string, std::string>& labels)
    : prom_file_path_(prom_file_path)
    , write_interval_(write_interval)
    , registry_(std::make_shared<prometheus::Registry>()) {

    // --- Counters ---

    auto& uploads_family = prometheus::BuildCounter()
        .Name("p4cache_uploads_total")
        .Help("Total uploads completed")
        .Labels(labels)
        .Register(*registry_);
    uploads_success_ = &uploads_family.Add({{"result", "success"}});
    uploads_failure_ = &uploads_family.Add({{"result", "failure"}});

    upload_bytes_total_ = &prometheus::BuildCounter()
        .Name("p4cache_upload_bytes_total")
        .Help("Total bytes uploaded")
        .Labels(labels)
        .Register(*registry_)
        .Add({});

    auto& restores_family = prometheus::BuildCounter()
        .Name("p4cache_restores_total")
        .Help("Total restores completed")
        .Labels(labels)
        .Register(*registry_);
    restores_success_ = &restores_family.Add({{"result", "success"}});
    restores_failure_ = &restores_family.Add({{"result", "failure"}});

    restore_bytes_total_ = &prometheus::BuildCounter()
        .Name("p4cache_restore_bytes_total")
        .Help("Total bytes restored")
        .Labels(labels)
        .Register(*registry_)
        .Add({});

    restores_secondary_total_ = &prometheus::BuildCounter()
        .Name("p4cache_restores_secondary_total")
        .Help("Total restores from secondary backend")
        .Labels(labels)
        .Register(*registry_)
        .Add({});

    evictions_total_ = &prometheus::BuildCounter()
        .Name("p4cache_evictions_total")
        .Help("Total files evicted")
        .Labels(labels)
        .Register(*registry_)
        .Add({});

    eviction_bytes_total_ = &prometheus::BuildCounter()
        .Name("p4cache_eviction_bytes_total")
        .Help("Total bytes evicted")
        .Labels(labels)
        .Register(*registry_)
        .Add({});

    auto& shim_family = prometheus::BuildCounter()
        .Name("p4cache_shim_requests_total")
        .Help("Total shim fetch requests")
        .Labels(labels)
        .Register(*registry_);
    shim_fetched_ = &shim_family.Add({{"result", "fetched"}});
    shim_not_found_ = &shim_family.Add({{"result", "not_found"}});

    auto& watcher_family = prometheus::BuildCounter()
        .Name("p4cache_watcher_events_total")
        .Help("Total watcher events")
        .Labels(labels)
        .Register(*registry_);
    watcher_writes_ = &watcher_family.Add({{"type", "write"}});
    watcher_errors_ = &watcher_family.Add({{"type", "error"}});
    watcher_self_filtered_ = &watcher_family.Add({{"type", "self_filtered"}});

    access_events_total_ = &prometheus::BuildCounter()
        .Name("p4cache_access_events_total")
        .Help("Total access log events received")
        .Labels(labels)
        .Register(*registry_)
        .Add({});

    access_batches_total_ = &prometheus::BuildCounter()
        .Name("p4cache_access_batches_total")
        .Help("Total access log batches written to LMDB")
        .Labels(labels)
        .Register(*registry_)
        .Add({});

    // --- Gauges ---

    auto gauge_reg = [&](const std::string& name, const std::string& help) -> prometheus::Gauge& {
        return prometheus::BuildGauge()
            .Name(name)
            .Help(help)
            .Labels(labels)
            .Register(*registry_)
            .Add({});
    };

    files_dirty_ = &gauge_reg("p4cache_files_dirty", "Files in dirty state");
    files_uploading_ = &gauge_reg("p4cache_files_uploading", "Files being uploaded");
    files_clean_ = &gauge_reg("p4cache_files_clean", "Files in clean state");
    files_total_ = &gauge_reg("p4cache_files_total", "Total tracked files");
    cache_bytes_ = &gauge_reg("p4cache_cache_bytes", "Current cache size in bytes");
    cache_max_bytes_ = &gauge_reg("p4cache_cache_max_bytes", "Maximum cache size in bytes");
    upload_queue_pending_ = &gauge_reg("p4cache_upload_queue_pending", "Pending upload tasks");
    restore_queue_pending_ = &gauge_reg("p4cache_restore_queue_pending", "Pending restore tasks");
    access_db_entries_ = &gauge_reg("p4cache_access_db_entries", "Access log database entries");

    // --- Histograms ---

    upload_duration_ = &prometheus::BuildHistogram()
        .Name("p4cache_upload_duration_seconds")
        .Help("Upload duration in seconds")
        .Labels(labels)
        .Register(*registry_)
        .Add({}, prometheus::Histogram::BucketBoundaries{
            0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300});

    restore_duration_ = &prometheus::BuildHistogram()
        .Name("p4cache_restore_duration_seconds")
        .Help("Restore duration in seconds")
        .Labels(labels)
        .Register(*registry_)
        .Add({}, prometheus::Histogram::BucketBoundaries{
            0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60});

    eviction_batch_duration_ = &prometheus::BuildHistogram()
        .Name("p4cache_eviction_batch_duration_seconds")
        .Help("Eviction batch duration in seconds")
        .Labels(labels)
        .Register(*registry_)
        .Add({}, prometheus::Histogram::BucketBoundaries{
            0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30});

    shim_request_duration_ = &prometheus::BuildHistogram()
        .Name("p4cache_shim_request_duration_seconds")
        .Help("Shim request duration in seconds")
        .Labels(labels)
        .Register(*registry_)
        .Add({}, prometheus::Histogram::BucketBoundaries{
            0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30});

    access_batch_duration_ = &prometheus::BuildHistogram()
        .Name("p4cache_access_batch_duration_seconds")
        .Help("Access log batch write duration in seconds")
        .Labels(labels)
        .Register(*registry_)
        .Add({}, prometheus::Histogram::BucketBoundaries{
            0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10});
}

MetricsExporter::~MetricsExporter() {
    stop();
}

void MetricsExporter::start() {
    {
        std::lock_guard lock(cv_mutex_);
        if (running_) return;
        running_ = true;
    }
    writer_thread_ = std::thread(&MetricsExporter::writer_loop, this);
}

void MetricsExporter::stop() {
    bool was_running = false;
    {
        std::lock_guard lock(cv_mutex_);
        was_running = running_;
        running_ = false;
    }
    if (was_running) {
        cv_.notify_all();
        if (writer_thread_.joinable()) {
            writer_thread_.join();
        }
    }
    // Always write a final snapshot
    update_gauges();
    write_file();
}

void MetricsExporter::writer_loop() {
    while (true) {
        {
            std::unique_lock lock(cv_mutex_);
            cv_.wait_for(lock, write_interval_, [this] { return !running_; });
            if (!running_) break;
        }
        update_gauges();
        write_file();
    }
}

void MetricsExporter::update_gauges() {
    if (cache_) {
        files_dirty_->Set(static_cast<double>(cache_->count_dirty()));
        files_uploading_->Set(static_cast<double>(cache_->count_uploading()));
        files_clean_->Set(static_cast<double>(cache_->count_clean()));
        files_total_->Set(static_cast<double>(
            cache_->count_dirty() + cache_->count_uploading() + cache_->count_clean()));
        cache_bytes_->Set(static_cast<double>(cache_->current_cache_bytes()));
        cache_max_bytes_->Set(static_cast<double>(cache_->max_cache_bytes()));
        upload_queue_pending_->Set(static_cast<double>(cache_->upload_pending()));
        restore_queue_pending_->Set(static_cast<double>(cache_->restore_pending()));
        access_db_entries_->Set(static_cast<double>(cache_->access_db_entries()));
    }

    if (watcher_) {
        auto ws = watcher_->get_stats();
        // Increment counters by deltas since last snapshot
        if (ws.write_events > prev_watcher_writes_) {
            watcher_writes_->Increment(
                static_cast<double>(ws.write_events - prev_watcher_writes_));
            prev_watcher_writes_ = ws.write_events;
        }
        if (ws.errors > prev_watcher_errors_) {
            watcher_errors_->Increment(
                static_cast<double>(ws.errors - prev_watcher_errors_));
            prev_watcher_errors_ = ws.errors;
        }
        if (ws.self_filtered > prev_watcher_self_filtered_) {
            watcher_self_filtered_->Increment(
                static_cast<double>(ws.self_filtered - prev_watcher_self_filtered_));
            prev_watcher_self_filtered_ = ws.self_filtered;
        }
    }
}

void MetricsExporter::write_file() {
    auto tmp_path = prom_file_path_;
    tmp_path += ".tmp";

    prometheus::TextSerializer serializer;
    auto metrics = registry_->Collect();

    std::ofstream ofs(tmp_path, std::ios::trunc);
    if (!ofs) return;
    ofs << serializer.Serialize(metrics);
    ofs.close();
    if (!ofs.good()) return;

    std::error_code ec;
    std::filesystem::rename(tmp_path, prom_file_path_, ec);
}

}  // namespace p4cache
