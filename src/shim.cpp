// LD_PRELOAD shim for p4-cache cold-read support.
//
// This shared library (libp4shim.so) is loaded into P4d's process via LD_PRELOAD.
// It intercepts open() and openat() calls. When the real syscall returns ENOENT
// and the path is under the depot directory, it asks the p4-cache daemon to
// fetch the file from storage. If successful, it retries the open.
//
// Environment variables:
//   P4CACHE_DEPOT  - Depot path prefix to intercept
//   P4CACHE_SOCK   - Unix socket path to daemon
//
// Usage: LD_PRELOAD=libp4shim.so P4CACHE_DEPOT=/mnt/nvme/depot p4d ...

#include <climits>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <unordered_set>

namespace {

// Original libc functions
using open_fn = int (*)(const char*, int, ...);
using openat_fn = int (*)(int, const char*, int, ...);

open_fn real_open = nullptr;
openat_fn real_openat = nullptr;

// Configuration from environment
const char* depot_path = nullptr;
size_t depot_path_len = 0;
std::string sock_path;

// --- Access log recording ---
constexpr size_t ACCESS_BUF_SIZE = 32768;         // 32KB per thread
constexpr size_t ACCESS_FLUSH_THRESHOLD = 30720;   // Flush at ~94% full

struct AccessBuffer {
    char data[ACCESS_BUF_SIZE];
    size_t pos = 0;
};

thread_local AccessBuffer* access_buf = nullptr;
int access_dgram_fd = -1;
struct sockaddr_un access_daemon_addr;
socklen_t access_daemon_addr_len = 0;
bool access_initialized = false;

// Negative cache: paths we know aren't in storage (avoid repeated lookups)
// Thread-local to avoid locking overhead
thread_local std::unordered_set<std::string>* negative_cache = nullptr;
constexpr size_t NEGATIVE_CACHE_MAX = 10000;

// Initialization flag
bool initialized = false;
std::mutex init_mutex;

bool starts_with(const char* str, const char* prefix, size_t prefix_len) {
    return strncmp(str, prefix, prefix_len) == 0;
}

std::unordered_set<std::string>& get_negative_cache() {
    if (!negative_cache) {
        negative_cache = new std::unordered_set<std::string>();
    }
    return *negative_cache;
}

/// Connect to the daemon's Unix socket and send a FETCH request.
/// Returns true if the file was fetched successfully.
bool request_fetch(const char* path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path.c_str(), sizeof(addr.sun_path) - 1);

    if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    // Set socket timeout
    struct timeval tv;
    tv.tv_sec = 60;  // 60 second timeout for storage fetch
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    // Build the relative path (strip depot prefix + trailing slash)
    const char* rel = path + depot_path_len;
    if (*rel == '/') rel++;

    // Send "FETCH <relative-path>\n"
    std::string request = "FETCH ";
    request += rel;
    request += "\n";

    ssize_t sent = write(fd, request.c_str(), request.size());
    if (sent != static_cast<ssize_t>(request.size())) {
        close(fd);
        return false;
    }

    // Read response
    char buf[256];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);

    if (n <= 0) return false;
    buf[n] = '\0';

    // Check for "OK " prefix
    return strncmp(buf, "OK ", 3) == 0;
}

void init_access_socket() {
    if (access_initialized) return;

    access_dgram_fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (access_dgram_fd < 0) return;

    memset(&access_daemon_addr, 0, sizeof(access_daemon_addr));
    access_daemon_addr.sun_family = AF_UNIX;

    const char* access_sock_env = getenv("P4CACHE_ACCESS_SOCK");
    if (access_sock_env) {
        strncpy(access_daemon_addr.sun_path, access_sock_env,
                sizeof(access_daemon_addr.sun_path) - 1);
    } else if (depot_path) {
        std::string path = std::string(depot_path, depot_path_len) + "/.p4cache/access.sock";
        strncpy(access_daemon_addr.sun_path, path.c_str(),
                sizeof(access_daemon_addr.sun_path) - 1);
    } else {
        close(access_dgram_fd);
        access_dgram_fd = -1;
        return;
    }

    access_daemon_addr_len = sizeof(access_daemon_addr);
    access_initialized = true;
}

void flush_access_buffer() {
    if (!access_buf || access_buf->pos == 0) return;
    if (access_dgram_fd < 0) {
        access_buf->pos = 0;
        return;
    }

    // Fire-and-forget: silently drop on EAGAIN/EWOULDBLOCK
    sendto(access_dgram_fd, access_buf->data, access_buf->pos, MSG_DONTWAIT,
           reinterpret_cast<struct sockaddr*>(&access_daemon_addr), access_daemon_addr_len);
    access_buf->pos = 0;
}

void record_access(const char* pathname) {
    if (!depot_path || access_dgram_fd < 0) return;

    // Must be under depot path
    if (!starts_with(pathname, depot_path, depot_path_len)) return;

    // Strip depot prefix
    const char* rel = pathname + depot_path_len;
    if (*rel == '/') rel++;

    // Skip .p4cache directory
    if (strncmp(rel, ".p4cache/", 9) == 0 || strcmp(rel, ".p4cache") == 0) return;

    size_t rel_len = strlen(rel);
    if (rel_len == 0) return;

    // Get/create thread-local buffer
    if (!access_buf) {
        access_buf = new AccessBuffer();
    }

    // Check if path + newline fits
    size_t needed = rel_len + 1;  // path + '\n'
    if (access_buf->pos + needed > ACCESS_BUF_SIZE) {
        flush_access_buffer();
        // If single path is too large for buffer, skip it
        if (needed > ACCESS_BUF_SIZE) return;
    }

    memcpy(access_buf->data + access_buf->pos, rel, rel_len);
    access_buf->pos += rel_len;
    access_buf->data[access_buf->pos++] = '\n';

    if (access_buf->pos >= ACCESS_FLUSH_THRESHOLD) {
        flush_access_buffer();
    }
}

void ensure_initialized() {
    if (initialized) return;

    std::lock_guard<std::mutex> lock(init_mutex);
    // Double-checked locking pattern - recheck after acquiring mutex
    // cppcheck-suppress identicalConditionAfterEarlyExit
    if (initialized) return;

    // Load real functions via dlsym
    real_open = reinterpret_cast<open_fn>(dlsym(RTLD_NEXT, "open"));
    real_openat = reinterpret_cast<openat_fn>(dlsym(RTLD_NEXT, "openat"));

    // Read configuration from environment
    depot_path = getenv("P4CACHE_DEPOT");
    if (depot_path) {
        depot_path_len = strlen(depot_path);
        // Strip trailing slash
        while (depot_path_len > 1 && depot_path[depot_path_len - 1] == '/') {
            depot_path_len--;
        }
    }

    const char* sock_env = getenv("P4CACHE_SOCK");
    if (sock_env) {
        sock_path = sock_env;
    } else if (depot_path) {
        sock_path = std::string(depot_path, depot_path_len) + "/.p4cache/shim.sock";
    }

    initialized = true;

    // Initialize access log socket (after depot_path is set)
    init_access_socket();
}

/// Check if this path should be intercepted on ENOENT.
bool should_intercept(const char* pathname) {
    if (!depot_path || depot_path_len == 0) return false;
    if (!pathname) return false;

    // Must be under our depot path
    if (!starts_with(pathname, depot_path, depot_path_len)) return false;

    // Must have content after the prefix (not the depot dir itself)
    size_t path_len = strlen(pathname);
    if (path_len <= depot_path_len + 1) return false;

    // Skip .p4cache directory
    const char* rel = pathname + depot_path_len;
    if (*rel == '/') rel++;
    if (strncmp(rel, ".p4cache/", 9) == 0 || strcmp(rel, ".p4cache") == 0) {
        return false;
    }

    // Check negative cache
    auto& cache = get_negative_cache();
    if (cache.count(pathname)) return false;

    return true;
}

void add_to_negative_cache(const char* pathname) {
    auto& cache = get_negative_cache();
    if (cache.size() >= NEGATIVE_CACHE_MAX) {
        cache.clear();  // Simple eviction: clear when full
    }
    cache.insert(pathname);
}

}  // namespace

// Hook open()
extern "C" int open(const char* pathname, int flags, ...) {
    ensure_initialized();

    // Extract mode argument (needed for O_CREAT)
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) {
        va_list args;
        va_start(args, flags);
        mode = static_cast<mode_t>(va_arg(args, int));
        va_end(args);
    }

    // Try the real open first
    int fd = real_open(pathname, flags, mode);

    if (fd >= 0) {                    // File exists — fast path
        record_access(pathname);
        return fd;
    }
    if (errno != ENOENT) return fd;   // Non-ENOENT error — pass through

    // ENOENT — check if we should intercept
    if (!should_intercept(pathname)) {
        errno = ENOENT;
        return -1;
    }

    // Ask daemon to fetch from storage
    if (request_fetch(pathname)) {
        // File should now exist — retry
        return real_open(pathname, flags, mode);
    }

    // Not in storage either — add to negative cache
    add_to_negative_cache(pathname);
    errno = ENOENT;
    return -1;
}

// Hook openat()
extern "C" int openat(int dirfd, const char* pathname, int flags, ...) {
    ensure_initialized();

    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) {
        va_list args;
        va_start(args, flags);
        mode = static_cast<mode_t>(va_arg(args, int));
        va_end(args);
    }

    int fd = real_openat(dirfd, pathname, flags, mode);

    if (fd >= 0) {                    // File exists — fast path
        // Only record absolute paths (P4d uses absolute paths for depot files;
        // resolving CWD for relative paths would be too expensive)
        if (pathname[0] == '/') {
            record_access(pathname);
        }
        return fd;
    }
    if (errno != ENOENT) return fd;   // Non-ENOENT error — pass through

    // Only intercept absolute paths or paths relative to AT_FDCWD
    if (pathname[0] != '/' && dirfd != AT_FDCWD) {
        errno = ENOENT;
        return -1;
    }

    // For AT_FDCWD relative paths, resolve to absolute
    std::string abs_path;
    if (pathname[0] != '/') {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd))) {
            abs_path = std::string(cwd) + "/" + pathname;
            pathname = abs_path.c_str();
        } else {
            errno = ENOENT;
            return -1;
        }
    }

    if (!should_intercept(pathname)) {
        errno = ENOENT;
        return -1;
    }

    if (request_fetch(pathname)) {
        return real_openat(dirfd, pathname, flags, mode);
    }

    add_to_negative_cache(pathname);
    errno = ENOENT;
    return -1;
}
