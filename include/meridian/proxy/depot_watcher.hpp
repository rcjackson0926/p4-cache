#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
#include <thread>

namespace meridian::proxy {

/// Watches a depot directory for file system events using Linux fanotify.
///
/// Monitors FAN_CLOSE_WRITE events to detect new/modified depot files for storage upload.
/// In read-only mode, no fanotify monitoring is needed (the LD_PRELOAD shim handles
/// all read-side interception for evicted stubs and cold files).
///
/// NOTE: We intentionally do NOT use FAN_OPEN_PERM. FAN_OPEN_PERM with FAN_MARK_MOUNT
/// blocks ALL opens on the entire mount (not just depot files), stalling the filesystem
/// when the daemon is busy. Evicted file restores are handled by the LD_PRELOAD shim.
class DepotWatcher {
public:
    /// Callback for FAN_CLOSE_WRITE events.
    /// @param path Absolute path of the written file.
    using WriteCallback = std::function<void(const std::filesystem::path& path)>;

    /// Callback for FAN_OPEN_PERM events.
    /// @param path Absolute path of the file being opened.
    /// @return true if the open should be allowed (FAN_ALLOW), false to deny (FAN_DENY).
    using OpenCallback = std::function<bool(const std::filesystem::path& path)>;

    /// @param depot_path Directory to watch (must exist).
    /// @param read_only If true, only register FAN_OPEN_PERM (no FAN_CLOSE_WRITE).
    explicit DepotWatcher(const std::filesystem::path& depot_path, bool read_only = false);
    ~DepotWatcher();

    DepotWatcher(const DepotWatcher&) = delete;
    DepotWatcher& operator=(const DepotWatcher&) = delete;

    void set_write_callback(WriteCallback cb);
    void set_open_callback(OpenCallback cb);

    /// Start the event loop in a background thread.
    void start();

    /// Stop the event loop and join the background thread.
    void stop();

    bool is_running() const { return running_.load(std::memory_order_relaxed); }

    struct Stats {
        uint64_t write_events = 0;
        uint64_t open_events = 0;
        uint64_t open_blocked = 0;   // Opens that required S3 restore
        uint64_t self_filtered = 0;  // Events from our own PID (ignored)
        uint64_t errors = 0;
    };
    Stats get_stats() const;

private:
    void event_loop();
    std::string read_proc_fd_path(int fd) const;

    std::filesystem::path depot_path_;
    bool read_only_ = false;
    int fanotify_fd_ = -1;
    std::thread event_thread_;
    std::atomic<bool> running_{false};
    int stop_pipe_[2] = {-1, -1};  // Self-pipe for wakeup

    WriteCallback write_cb_;
    OpenCallback open_cb_;

    mutable std::mutex stats_mutex_;
    Stats stats_;
};

}  // namespace meridian::proxy
