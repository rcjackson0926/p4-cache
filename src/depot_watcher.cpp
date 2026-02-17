#include "p4cache/depot_watcher.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <poll.h>
#include <sys/fanotify.h>
#include <unistd.h>

namespace p4cache {

DepotWatcher::DepotWatcher(const std::filesystem::path& depot_path, bool read_only)
    : depot_path_(std::filesystem::canonical(depot_path)), read_only_(read_only) {}

DepotWatcher::~DepotWatcher() {
    stop();
    if (fanotify_fd_ >= 0) {
        close(fanotify_fd_);
    }
    if (stop_pipe_[0] >= 0) {
        close(stop_pipe_[0]);
        close(stop_pipe_[1]);
    }
}

void DepotWatcher::set_write_callback(WriteCallback cb) {
    write_cb_ = std::move(cb);
}

void DepotWatcher::set_open_callback(OpenCallback cb) {
    open_cb_ = std::move(cb);
}

void DepotWatcher::start() {
    if (running_.load()) return;

    // Create self-pipe for clean shutdown
    if (pipe2(stop_pipe_, O_CLOEXEC | O_NONBLOCK) < 0) {
        throw std::runtime_error("pipe2 failed: " + std::string(strerror(errno)));
    }

    // In read-only mode, no fanotify monitoring is needed.
    // The LD_PRELOAD shim handles all read-side interception for evicted stubs
    // and cold files. We still run the event loop thread (for clean shutdown via
    // the stop pipe), but don't initialize fanotify.
    if (!read_only_) {
        // FAN_CLASS_CONTENT + FAN_NONBLOCK for non-blocking notification reads.
        // We only use FAN_CLOSE_WRITE (not FAN_OPEN_PERM — see header comment).
        unsigned int fan_init_flags = FAN_CLOEXEC | FAN_CLASS_CONTENT | FAN_NONBLOCK;
        fanotify_fd_ = fanotify_init(fan_init_flags, O_RDONLY | O_LARGEFILE | O_CLOEXEC);
        if (fanotify_fd_ < 0) {
            throw std::runtime_error(
                "fanotify_init failed (requires CAP_SYS_ADMIN): " + std::string(strerror(errno)));
        }

        if (fanotify_mark(fanotify_fd_, FAN_MARK_ADD | FAN_MARK_MOUNT,
                          FAN_CLOSE_WRITE, AT_FDCWD, depot_path_.c_str()) < 0) {
            close(fanotify_fd_);
            fanotify_fd_ = -1;
            throw std::runtime_error(
                "fanotify_mark failed for " + depot_path_.string() + ": " + std::string(strerror(errno)));
        }
    }

    running_ = true;
    event_thread_ = std::thread(&DepotWatcher::event_loop, this);
}

void DepotWatcher::stop() {
    if (!running_.exchange(false)) return;

    // Signal the event loop to stop via the self-pipe
    if (stop_pipe_[1] >= 0) {
        char c = 1;
        (void)write(stop_pipe_[1], &c, 1);
    }

    if (event_thread_.joinable()) {
        event_thread_.join();
    }
}

DepotWatcher::Stats DepotWatcher::get_stats() const {
    std::lock_guard lock(stats_mutex_);
    return stats_;
}

std::string DepotWatcher::read_proc_fd_path(int fd) const {
    char path_buf[PATH_MAX];
    char proc_path[64];
    snprintf(proc_path, sizeof(proc_path), "/proc/self/fd/%d", fd);
    ssize_t len = readlink(proc_path, path_buf, sizeof(path_buf) - 1);
    if (len <= 0) return {};
    path_buf[len] = '\0';
    return std::string(path_buf);
}

void DepotWatcher::event_loop() {
    const pid_t my_pid = getpid();
    const std::string depot_prefix = depot_path_.string();
    const std::string cache_dir = (depot_path_ / ".p4cache").string();

    alignas(struct fanotify_event_metadata) char buf[8192];

    // In read-only mode, fanotify_fd_ is -1. Just wait on the stop pipe.
    const int nfds = (fanotify_fd_ >= 0) ? 2 : 1;
    struct pollfd fds[2];
    if (fanotify_fd_ >= 0) {
        fds[0].fd = fanotify_fd_;
        fds[0].events = POLLIN;
        fds[1].fd = stop_pipe_[0];
        fds[1].events = POLLIN;
    } else {
        fds[0].fd = stop_pipe_[0];
        fds[0].events = POLLIN;
    }

    const int stop_idx = (fanotify_fd_ >= 0) ? 1 : 0;
    const int fan_idx = 0;  // Only valid when fanotify_fd_ >= 0

    while (running_.load(std::memory_order_relaxed)) {
        int ret = poll(fds, nfds, 1000);
        if (ret < 0) {
            if (errno == EINTR) continue;
            std::lock_guard lock(stats_mutex_);
            stats_.errors++;
            break;
        }
        if (ret == 0) continue;

        // Check stop signal
        if (fds[stop_idx].revents & POLLIN) break;

        // No fanotify in read-only mode
        if (fanotify_fd_ < 0) continue;

        if (!(fds[fan_idx].revents & POLLIN)) continue;

        ssize_t len = read(fanotify_fd_, buf, sizeof(buf));
        if (len <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
            std::lock_guard lock(stats_mutex_);
            stats_.errors++;
            continue;
        }

        auto* metadata = reinterpret_cast<struct fanotify_event_metadata*>(buf);
        while (FAN_EVENT_OK(metadata, len)) {
            if (metadata->vers != FANOTIFY_METADATA_VERSION) {
                if (metadata->fd >= 0) close(metadata->fd);
                metadata = FAN_EVENT_NEXT(metadata, len);
                continue;
            }

            // Skip events from our own process (e.g., when we restore files)
            if (metadata->pid == my_pid) {
                {
                    std::lock_guard lock(stats_mutex_);
                    stats_.self_filtered++;
                }
                if (metadata->fd >= 0) close(metadata->fd);
                metadata = FAN_EVENT_NEXT(metadata, len);
                continue;
            }

            // Resolve the path from the file descriptor
            std::string file_path;
            if (metadata->fd >= 0) {
                file_path = read_proc_fd_path(metadata->fd);
            }

            // Filter: only process files under our depot path, skip .p4cache/ dir
            bool in_depot = !file_path.empty() &&
                            file_path.size() > depot_prefix.size() &&
                            file_path.compare(0, depot_prefix.size(), depot_prefix) == 0;
            bool in_cache_dir = in_depot &&
                                file_path.size() > cache_dir.size() &&
                                file_path.compare(0, cache_dir.size(), cache_dir) == 0;

            if (in_depot && !in_cache_dir && (metadata->mask & FAN_CLOSE_WRITE)) {
                {
                    std::lock_guard lock(stats_mutex_);
                    stats_.write_events++;
                }
                if (write_cb_) {
                    try {
                        write_cb_(std::filesystem::path(file_path));
                    } catch (...) {}
                }
            }

            if (metadata->fd >= 0) close(metadata->fd);
            metadata = FAN_EVENT_NEXT(metadata, len);
        }
    }
}

}  // namespace p4cache
