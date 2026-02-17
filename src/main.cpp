#include "meridian/proxy/depot_watcher.hpp"
#include "meridian/proxy/depot_cache.hpp"
#include "meridian/proxy/cache_config.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace {
volatile std::sig_atomic_t g_shutdown_requested = 0;
meridian::proxy::DepotCache* g_cache = nullptr;

void signal_handler(int sig) {
    (void)sig;
    g_shutdown_requested = 1;
}

bool daemonize() {
    pid_t pid = fork();
    if (pid < 0) return false;
    if (pid > 0) _exit(0);  // Parent exits

    if (setsid() < 0) return false;

    // Second fork to prevent acquiring a controlling terminal
    pid = fork();
    if (pid < 0) return false;
    if (pid > 0) _exit(0);

    // Redirect stdin to /dev/null; stdout/stderr will be redirected
    // to log file after this function returns.
    close(STDIN_FILENO);
    open("/dev/null", O_RDONLY);  // stdin = fd 0

    return true;
}

void write_pid_file(const std::filesystem::path& path) {
    std::ofstream ofs(path);
    if (ofs) {
        ofs << getpid() << "\n";
    }
}
}  // namespace

int main(int argc, char* argv[]) {
    auto config_opt = meridian::proxy::CacheConfig::from_args(argc, argv);
    if (!config_opt) {
        return 1;
    }
    auto config = std::move(*config_opt);

    auto err = config.validate();
    if (!err.empty()) {
        std::cerr << "Configuration error: " << err << "\n";
        return 1;
    }

    // Daemonize if requested (before log redirect so we fork first)
    if (config.daemonize) {
        if (!daemonize()) {
            std::cerr << "Failed to daemonize" << std::endl;
            return 1;
        }
    }

    // Redirect log output if log file specified (after daemonize)
    if (!config.log_file.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(
            std::filesystem::path(config.log_file).parent_path(), ec);
        FILE* log = fopen(config.log_file.c_str(), "a");
        if (log) {
            dup2(fileno(log), STDOUT_FILENO);
            dup2(fileno(log), STDERR_FILENO);
            fclose(log);
        }
    }

    std::cout << "p4-cache starting..." << std::endl;
    std::cout << "  depot-path: " << config.depot_path << std::endl;
    std::cout << "  primary-type: " << config.primary.type << std::endl;
    for (auto& [k, v] : config.primary.params) {
        // Mask secrets in log output
        if (k.find("key") != std::string::npos || k.find("secret") != std::string::npos ||
            k.find("token") != std::string::npos || k.find("credential") != std::string::npos) {
            std::cout << "  primary-" << k << ": ****" << std::endl;
        } else {
            std::cout << "  primary-" << k << ": " << v << std::endl;
        }
    }
    if (!config.secondary.empty()) {
        std::cout << "  secondary-type: " << config.secondary.type << std::endl;
        for (auto& [k, v] : config.secondary.params) {
            if (k.find("key") != std::string::npos || k.find("secret") != std::string::npos ||
                k.find("token") != std::string::npos || k.find("credential") != std::string::npos) {
                std::cout << "  secondary-" << k << ": ****" << std::endl;
            } else {
                std::cout << "  secondary-" << k << ": " << v << std::endl;
            }
        }
    }
    std::cout << "  read-only: " << (config.read_only ? "yes" : "no") << std::endl;
    std::cout << "  max-cache: " << (config.max_cache_bytes / (1024 * 1024 * 1024)) << " GB" << std::endl;

    if (config.read_only) {
        std::cout << "  mode: READ-ONLY (replica)" << std::endl;
        std::cout << "    upload workers: disabled" << std::endl;
        std::cout << "    write monitoring: disabled" << std::endl;
    } else {
        std::cout << "  mode: READ-WRITE (primary)" << std::endl;
        std::cout << "  upload-threads: " << config.upload_threads << std::endl;
        std::cout << "  upload-concurrency: " << config.upload_concurrency << std::endl;
    }
    std::cout << "  restore-threads: " << config.restore_threads << std::endl;

    // Create state directory and write PID file
    if (!config.pid_file.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(
            std::filesystem::path(config.pid_file).parent_path(), ec);
        write_pid_file(config.pid_file);
    }

    // Install signal handlers
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    // Create and start the cache engine
    meridian::proxy::DepotCache cache(config);
    g_cache = &cache;

    err = cache.start();
    if (!err.empty()) {
        std::cerr << "Failed to start cache: " << err << std::endl;
        return 1;
    }

    // Create and start the depot watcher
    meridian::proxy::DepotWatcher watcher(config.depot_path, config.read_only);

    if (!config.read_only) {
        watcher.set_write_callback([&cache](const std::filesystem::path& path) {
            cache.on_file_written(path);
        });
    }

    // NOTE: We do NOT use FAN_OPEN_PERM / set_open_callback here.
    // FAN_OPEN_PERM with FAN_MARK_MOUNT blocks ALL opens on the entire mount
    // (not just depot files), stalling the filesystem when the daemon is busy.
    // Evicted stub restoration is handled by the LD_PRELOAD shim instead,
    // which only intercepts P4d's opens via Unix socket to the daemon.

    try {
        watcher.start();
    } catch (const std::exception& e) {
        std::cerr << "Failed to start depot watcher: " << e.what() << std::endl;
        cache.stop();
        return 1;
    }

    std::cout << "p4-cache running (PID " << getpid() << ")" << std::endl;

    // Wait until shutdown signal, then stop outside signal context.
    while (!g_shutdown_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    cache.stop();
    cache.wait();

    // Cleanup
    watcher.stop();
    g_cache = nullptr;

    // Remove PID file
    if (!config.pid_file.empty()) {
        unlink(config.pid_file.c_str());
    }

    std::cout << "p4-cache exited cleanly" << std::endl;
    return 0;
}
