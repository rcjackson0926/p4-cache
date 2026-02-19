#include "meridian/net/http.hpp"
#include "meridian/core/compat.hpp"
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <queue>
#include <thread>
#include <condition_variable>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <atomic>
#include <future>
#include <unordered_set>

namespace meridian::net {

// ============================================================================
// Environment-based configuration helpers (N-C9 FIX)
// ============================================================================

// Get connection pool size from environment or use default
static size_t get_connection_pool_size() {
    if (const char* env = std::getenv("MERIDIAN_CONNECTION_POOL_SIZE")) {
        try {
            size_t size = std::stoul(env);
            // Sanity check: at least 1, at most 1000
            if (size >= 1 && size <= 1000) {
                return size;
            }
            std::cerr << "warning: MERIDIAN_CONNECTION_POOL_SIZE=" << env
                      << " out of range [1,1000], using default\n";
        } catch (const std::exception&) {
            std::cerr << "warning: invalid MERIDIAN_CONNECTION_POOL_SIZE=" << env
                      << ", using default\n";
        }
    }
    return 10;  // reasonable default
}

// Get request timeout from environment or use default
static std::chrono::seconds get_request_timeout() {
    if (const char* env = std::getenv("MERIDIAN_REQUEST_TIMEOUT")) {
        try {
            unsigned long secs = std::stoul(env);
            // Sanity check: at least 5 seconds, at most 1 hour
            if (secs >= 5 && secs <= 3600) {
                return std::chrono::seconds(secs);
            }
            std::cerr << "warning: MERIDIAN_REQUEST_TIMEOUT=" << env
                      << " out of range [5,3600], using default\n";
        } catch (const std::exception&) {
            std::cerr << "warning: invalid MERIDIAN_REQUEST_TIMEOUT=" << env
                      << ", using default\n";
        }
    }
    return std::chrono::seconds(30);  // reasonable default
}

// ============================================================================
// Utility functions
// ============================================================================

const char* http_method_to_string(HttpMethod method) {
    switch (method) {
        case HttpMethod::GET: return "GET";
        case HttpMethod::POST: return "POST";
        case HttpMethod::PUT: return "PUT";
        case HttpMethod::DELETE: return "DELETE";
        case HttpMethod::PATCH: return "PATCH";
        case HttpMethod::HEAD: return "HEAD";
        case HttpMethod::OPTIONS: return "OPTIONS";
    }
    return "GET";
}

bool is_success_status(int status) {
    return status >= 200 && status < 300;
}

bool is_redirect_status(int status) {
    return status >= 300 && status < 400;
}

bool is_client_error_status(int status) {
    return status >= 400 && status < 500;
}

bool is_server_error_status(int status) {
    return status >= 500 && status < 600;
}

bool is_retryable_status(int status) {
    // Retry on server errors and rate limiting
    return status == 429 || status == 502 || status == 503 || status == 504;
}

std::string url_encode(const std::string& str) {
    std::ostringstream encoded;
    encoded << std::hex << std::uppercase;

    for (unsigned char c : str) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            encoded << c;
        } else {
            encoded << '%' << std::setw(2) << std::setfill('0') << static_cast<int>(c);
        }
    }

    return encoded.str();
}

static int hex_digit(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

std::string url_decode(const std::string& str) {
    std::string decoded;
    decoded.reserve(str.size());

    for (size_t i = 0; i < str.size(); ++i) {
        if (str[i] == '%' && i + 2 < str.size()) {
            int h1 = hex_digit(str[i + 1]);
            int h2 = hex_digit(str[i + 2]);
            if (h1 >= 0 && h2 >= 0) {
                int value = (h1 << 4) | h2;
                // Reject embedded null bytes (%00) to prevent truncation
                if (value == 0) {
                    // Skip the %00 sequence entirely
                    i += 2;
                    continue;
                }
                decoded += static_cast<char>(value);
                i += 2;
                continue;
            }
            // Invalid hex sequence — keep the literal '%'
        } else if (str[i] == '+') {
            decoded += ' ';
            continue;
        }
        decoded += str[i];
    }

    return decoded;
}

static const char* base64_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64_encode(const std::vector<uint8_t>& data) {
    std::string result;
    result.reserve((data.size() + 2) / 3 * 4);

    size_t i = 0;
    while (i < data.size()) {
        uint32_t octet_a = i < data.size() ? data[i++] : 0;
        uint32_t octet_b = i < data.size() ? data[i++] : 0;
        uint32_t octet_c = i < data.size() ? data[i++] : 0;

        uint32_t triple = (octet_a << 16) + (octet_b << 8) + octet_c;

        result += base64_chars[(triple >> 18) & 0x3F];
        result += base64_chars[(triple >> 12) & 0x3F];
        result += (i > data.size() + 1) ? '=' : base64_chars[(triple >> 6) & 0x3F];
        result += (i > data.size()) ? '=' : base64_chars[triple & 0x3F];
    }

    return result;
}

std::string base64_encode(const std::string& str) {
    return base64_encode(std::vector<uint8_t>(str.begin(), str.end()));
}

std::vector<uint8_t> base64_decode(const std::string& encoded) {
    std::vector<uint8_t> result;
    result.reserve(encoded.size() * 3 / 4);

    std::vector<int> decode_table(256, -1);
    for (int i = 0; i < 64; ++i) {
        decode_table[static_cast<unsigned char>(base64_chars[i])] = i;
    }

    uint32_t val = 0;
    int bits = 0;

    for (char c : encoded) {
        if (c == '=' || c == '\n' || c == '\r') continue;
        if (decode_table[static_cast<unsigned char>(c)] < 0) continue;

        val = (val << 6) | decode_table[static_cast<unsigned char>(c)];
        bits += 6;

        if (bits >= 8) {
            bits -= 8;
            result.push_back(static_cast<uint8_t>((val >> bits) & 0xFF));
        }
    }

    return result;
}

// ============================================================================
// HttpHeaders
// ============================================================================

std::string HttpHeaders::normalize_name(const std::string& name) {
    std::string result = name;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

void HttpHeaders::set(const std::string& name, const std::string& value) {
    std::string key = normalize_name(name);
    headers_[key] = {value};
}

void HttpHeaders::add(const std::string& name, const std::string& value) {
    std::string key = normalize_name(name);
    headers_[key].push_back(value);
}

void HttpHeaders::remove(const std::string& name) {
    headers_.erase(normalize_name(name));
}

std::optional<std::string> HttpHeaders::get(const std::string& name) const {
    auto it = headers_.find(normalize_name(name));
    if (it != headers_.end() && !it->second.empty()) {
        return it->second[0];
    }
    return std::nullopt;
}

std::vector<std::string> HttpHeaders::get_all(const std::string& name) const {
    auto it = headers_.find(normalize_name(name));
    if (it != headers_.end()) {
        return it->second;
    }
    return {};
}

bool HttpHeaders::has(const std::string& name) const {
    return headers_.find(normalize_name(name)) != headers_.end();
}

std::vector<HttpHeaders::HeaderPair> HttpHeaders::all() const {
    std::vector<HeaderPair> result;
    for (const auto& [name, values] : headers_) {
        for (const auto& value : values) {
            result.emplace_back(name, value);
        }
    }
    return result;
}

void HttpHeaders::set_content_type(const std::string& content_type) {
    set("Content-Type", content_type);
}

void HttpHeaders::set_content_length(size_t length) {
    set("Content-Length", std::to_string(length));
}

void HttpHeaders::set_authorization(const std::string& scheme, const std::string& credentials) {
    set("Authorization", scheme + " " + credentials);
}

void HttpHeaders::set_bearer_token(const std::string& token) {
    set_authorization("Bearer", token);
}

void HttpHeaders::set_basic_auth(const std::string& username, const std::string& password) {
    std::string credentials = username + ":" + password;
    set_authorization("Basic", base64_encode(credentials));
}

std::optional<std::string> HttpHeaders::content_type() const {
    return get("Content-Type");
}

std::optional<size_t> HttpHeaders::content_length() const {
    auto val = get("Content-Length");
    if (val) {
        try {
            return std::stoull(*val);
        } catch (const std::invalid_argument&) {
            // Invalid Content-Length header format
        } catch (const std::out_of_range&) {
            // Content-Length value out of range
        }
    }
    return std::nullopt;
}

// ============================================================================
// HttpRequest
// ============================================================================

HttpRequest HttpRequest::get(const std::string& url) {
    HttpRequest req;
    req.method = HttpMethod::GET;
    req.url = url;
    return req;
}

HttpRequest HttpRequest::post(const std::string& url, const std::vector<uint8_t>& body) {
    HttpRequest req;
    req.method = HttpMethod::POST;
    req.url = url;
    req.body = body;
    return req;
}

HttpRequest HttpRequest::post(const std::string& url, const std::string& body) {
    return post(url, std::vector<uint8_t>(body.begin(), body.end()));
}

HttpRequest HttpRequest::put(const std::string& url, const std::vector<uint8_t>& body) {
    HttpRequest req;
    req.method = HttpMethod::PUT;
    req.url = url;
    req.body = body;
    return req;
}

HttpRequest HttpRequest::del(const std::string& url) {
    HttpRequest req;
    req.method = HttpMethod::DELETE;
    req.url = url;
    return req;
}

void HttpRequest::set_json_body(const std::string& json) {
    body = std::vector<uint8_t>(json.begin(), json.end());
    headers.set_content_type("application/json");
}

// ============================================================================
// HttpResponse
// ============================================================================

std::string HttpResponse::body_string() const {
    return std::string(body.begin(), body.end());
}

// ============================================================================
// ParsedUrl
// ============================================================================

std::optional<ParsedUrl> ParsedUrl::parse(const std::string& url) {
    ParsedUrl result;

    size_t pos = 0;

    // Scheme
    size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        return std::nullopt;
    }
    result.scheme = url.substr(0, scheme_end);
    pos = scheme_end + 3;

    // Userinfo (optional)
    size_t at_pos = url.find('@', pos);
    size_t slash_pos = url.find('/', pos);
    if (at_pos != std::string::npos && (slash_pos == std::string::npos || at_pos < slash_pos)) {
        result.userinfo = url.substr(pos, at_pos - pos);
        pos = at_pos + 1;
    }

    // Host and port
    size_t host_end = url.find_first_of("/?#", pos);
    if (host_end == std::string::npos) {
        host_end = url.size();
    }

    std::string host_port = url.substr(pos, host_end - pos);
    size_t colon_pos = host_port.rfind(':');

    // Check for IPv6 address
    if (host_port.front() == '[') {
        size_t bracket_end = host_port.find(']');
        if (bracket_end != std::string::npos) {
            result.host = host_port.substr(1, bracket_end - 1);
            if (bracket_end + 1 < host_port.size() && host_port[bracket_end + 1] == ':') {
                try {
                    result.port = std::stoi(host_port.substr(bracket_end + 2));
                } catch (const std::exception&) {
                    return std::nullopt;
                }
            }
        }
    } else if (colon_pos != std::string::npos) {
        result.host = host_port.substr(0, colon_pos);
        try {
            result.port = std::stoi(host_port.substr(colon_pos + 1));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    } else {
        result.host = host_port;
    }

    pos = host_end;

    // Path
    if (pos < url.size() && url[pos] == '/') {
        size_t path_end = url.find_first_of("?#", pos);
        if (path_end == std::string::npos) {
            path_end = url.size();
        }
        result.path = url.substr(pos, path_end - pos);
        pos = path_end;
    }

    // Query
    if (pos < url.size() && url[pos] == '?') {
        size_t query_end = url.find('#', pos);
        if (query_end == std::string::npos) {
            query_end = url.size();
        }
        result.query = url.substr(pos + 1, query_end - pos - 1);
        pos = query_end;
    }

    // Fragment
    if (pos < url.size() && url[pos] == '#') {
        result.fragment = url.substr(pos + 1);
    }

    return result;
}

std::string ParsedUrl::to_string() const {
    std::ostringstream oss;
    oss << scheme << "://";

    if (!userinfo.empty()) {
        oss << userinfo << "@";
    }

    if (host.find(':') != std::string::npos) {
        // IPv6
        oss << "[" << host << "]";
    } else {
        oss << host;
    }

    if (port != 0) {
        oss << ":" << port;
    }

    oss << path;

    if (!query.empty()) {
        oss << "?" << query;
    }

    if (!fragment.empty()) {
        oss << "#" << fragment;
    }

    return oss.str();
}

int ParsedUrl::effective_port() const {
    if (port != 0) return port;
    if (scheme == "https") return 443;
    if (scheme == "http") return 80;
    return 0;
}

// ============================================================================
// CURL callback functions
// ============================================================================

// Context for bounded response accumulation
struct WriteCallbackContext {
    std::vector<uint8_t>* response;
    size_t max_size;
    size_t current_size;
    bool size_exceeded;
};

static size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* ctx = static_cast<WriteCallbackContext*>(userdata);
    size_t bytes = size * nmemb;

    // Check if adding this data would exceed the limit
    if (ctx->max_size > 0 && ctx->current_size + bytes > ctx->max_size) {
        ctx->size_exceeded = true;
        return 0;  // Return 0 to signal error and abort transfer
    }

    ctx->response->insert(ctx->response->end(), ptr, ptr + bytes);
    ctx->current_size += bytes;
    return bytes;
}

static size_t header_callback(char* buffer, size_t size, size_t nitems, void* userdata) {
    auto* headers = static_cast<HttpHeaders*>(userdata);
    size_t bytes = size * nitems;

    std::string line(buffer, bytes);

    // Remove trailing CRLF
    while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
        line.pop_back();
    }

    // Skip empty lines and status line
    if (line.empty() || line.starts_with("HTTP/")) {
        return bytes;
    }

    // Parse header
    size_t colon = line.find(':');
    if (colon != std::string::npos) {
        std::string name = line.substr(0, colon);
        std::string value = line.substr(colon + 1);

        // Trim leading whitespace from value
        size_t start = value.find_first_not_of(" \t");
        if (start != std::string::npos) {
            value = value.substr(start);
        }

        headers->add(name, value);
    }

    return bytes;
}

static size_t read_callback(char* buffer, size_t size, size_t nitems, void* userdata) {
    struct ReadData {
        const uint8_t* data;
        size_t size;
        size_t pos;
    };

    auto* rd = static_cast<ReadData*>(userdata);
    size_t max_bytes = size * nitems;
    size_t remaining = rd->size - rd->pos;
    size_t to_copy = std::min(max_bytes, remaining);

    if (to_copy > 0) {
        std::memcpy(buffer, rd->data + rd->pos, to_copy);
        rd->pos += to_copy;
    }

    return to_copy;
}

static int progress_callback(void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                             curl_off_t ultotal, curl_off_t ulnow) {
    auto* callback = static_cast<HttpProgressCallback*>(clientp);
    if (*callback) {
        HttpProgress progress;
        progress.download_total = static_cast<size_t>(dltotal);
        progress.download_now = static_cast<size_t>(dlnow);
        progress.upload_total = static_cast<size_t>(ultotal);
        progress.upload_now = static_cast<size_t>(ulnow);

        // Return non-zero to abort
        if (!(*callback)(progress)) {
            return 1;
        }
    }
    return 0;
}

// ============================================================================
// HttpClient Implementation
// ============================================================================

class HttpClient::Impl {
public:
    explicit Impl(const HttpClientConfig& config)
        : config_(config) {
        // Initialize CURL globally (thread-safe)
        static std::once_flag curl_init_flag;
        std::call_once(curl_init_flag, []() {
            curl_global_init(CURL_GLOBAL_ALL);
        });

        // N-C9 FIX: Apply environment-based overrides for pool size
        size_t env_pool_size = get_connection_pool_size();
        if (config_.max_total_connections == 256) {  // Default value from HttpClientConfig
            config_.max_total_connections = env_pool_size * 10;  // Scale total by pool size
        }
        if (config_.max_connections_per_host == 32) {  // Default value from HttpClientConfig
            config_.max_connections_per_host = env_pool_size;
        }

        // N-C9 FIX: Apply environment-based overrides for timeouts
        auto env_timeout = get_request_timeout();
        if (config_.default_total_timeout == std::chrono::milliseconds{300000}) {  // Default
            config_.default_total_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(env_timeout);
        }

        // Create multi handle for connection pooling and reuse
        multi_handle_ = curl_multi_init();
        if (multi_handle_) {
            // Configure connection pool settings
            curl_multi_setopt(multi_handle_, CURLMOPT_MAXCONNECTS,
                              static_cast<long>(config_.max_total_connections));
            curl_multi_setopt(multi_handle_, CURLMOPT_MAX_HOST_CONNECTIONS,
                              static_cast<long>(config_.max_connections_per_host));
            // Enable pipelining/multiplexing for HTTP/2
            curl_multi_setopt(multi_handle_, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
        }

        // Start health check thread for connection pool maintenance
        health_check_running_ = true;
        health_check_thread_ = std::thread([this]() { health_check_loop(); });
    }

    ~Impl() {
        shutdown();
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            if (!health_check_running_) return;
            health_check_running_ = false;
        }
        pool_cv_.notify_all();

        if (health_check_thread_.joinable()) {
            health_check_thread_.join();
        }

        // Clean up pooled handles
        std::lock_guard<std::mutex> lock(pool_mutex_);
        for (CURL* handle : idle_handles_) {
            curl_easy_cleanup(handle);
        }
        idle_handles_.clear();

        if (multi_handle_) {
            curl_multi_cleanup(multi_handle_);
            multi_handle_ = nullptr;
        }
    }

private:
    // Acquire a handle from the pool or create a new one
    CURL* acquire_handle() {
        std::lock_guard<std::mutex> lock(pool_mutex_);

        if (!idle_handles_.empty()) {
            CURL* handle = idle_handles_.back();
            idle_handles_.pop_back();
            stats_.idle_connections = idle_handles_.size();
            stats_.active_connections++;
            return handle;
        }

        // Create new handle if under limit
        if (stats_.active_connections < config_.max_total_connections) {
            CURL* handle = curl_easy_init();
            if (handle) {
                stats_.active_connections++;
            }
            return handle;
        }

        return nullptr;  // At capacity
    }

    // Return a handle to the pool
    void release_handle(CURL* handle) {
        if (!handle) return;

        std::lock_guard<std::mutex> lock(pool_mutex_);
        stats_.active_connections--;

        if (!health_check_running_) {
            curl_easy_cleanup(handle);
            return;
        }

        // Reset handle for reuse
        curl_easy_reset(handle);

        // Keep in pool if under limit
        if (idle_handles_.size() < config_.max_total_connections) {
            idle_handles_.push_back(handle);
            stats_.idle_connections = idle_handles_.size();
        } else {
            curl_easy_cleanup(handle);
        }
    }

    void health_check_loop() {
        while (true) {
            {
                std::unique_lock<std::mutex> lock(pool_mutex_);
                pool_cv_.wait_for(lock, config_.health_check_interval, [this] {
                    return !health_check_running_;
                });

                if (!health_check_running_) break;

                // Clean up excess idle connections
                while (idle_handles_.size() > config_.max_connections_per_host) {
                    CURL* handle = idle_handles_.back();
                    idle_handles_.pop_back();
                    curl_easy_cleanup(handle);
                }
                stats_.idle_connections = idle_handles_.size();
            }
        }
    }

public:

    HttpResponse execute(const HttpRequest& request) {
        HttpResponse response;

        // Acquire handle from pool
        CURL* curl = acquire_handle();
        if (!curl) {
            response.error = "Failed to acquire connection from pool";
            response.is_network_error = true;
            return response;
        }

        // URL
        curl_easy_setopt(curl, CURLOPT_URL, request.url.c_str());

        // Method
        switch (request.method) {
            case HttpMethod::GET:
                curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
                break;
            case HttpMethod::POST:
                curl_easy_setopt(curl, CURLOPT_POST, 1L);
                break;
            case HttpMethod::PUT:
                curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
                break;
            case HttpMethod::DELETE:
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
                break;
            case HttpMethod::PATCH:
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
                break;
            case HttpMethod::HEAD:
                curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
                break;
            case HttpMethod::OPTIONS:
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "OPTIONS");
                break;
        }

        // Headers
        struct curl_slist* headers_list = nullptr;
        for (const auto& [name, value] : request.headers.all()) {
            std::string header = name + ": " + value;
            headers_list = curl_slist_append(headers_list, header.c_str());
        }

        // User agent
        if (!config_.user_agent.empty()) {
            curl_easy_setopt(curl, CURLOPT_USERAGENT, config_.user_agent.c_str());
        }

        if (headers_list) {
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers_list);
        }

        // Request body
        struct {
            const uint8_t* data;
            size_t size;
            size_t pos;
        } read_data = {request.body.data(), request.body.size(), 0};

        if (!request.body.empty()) {
            if (request.method == HttpMethod::PUT) {
                curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
                curl_easy_setopt(curl, CURLOPT_READDATA, &read_data);
                curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE,
                                 static_cast<curl_off_t>(request.body.size()));
            } else {
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.data());
                curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,
                                 static_cast<long>(request.body.size()));
            }
        } else if (request.method == HttpMethod::PUT) {
            // Empty-body PUT: explicitly set Content-Length: 0 so servers
            // like MinIO don't reject with HTTP 411 (Length Required).
            curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(0));
        }

        // Response callbacks with bounded size
        std::vector<uint8_t> response_body;
        WriteCallbackContext write_ctx{&response_body, config_.max_response_size, 0, false};
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &write_ctx);

        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_callback);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response.headers);

        // Progress callback
        HttpProgressCallback progress_cb = request.progress_callback;
        if (progress_cb) {
            curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, progress_callback);
            curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &progress_cb);
            curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        }

        // Timeouts
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS,
                         static_cast<long>(request.connect_timeout.count()));
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS,
                         static_cast<long>(request.total_timeout.count()));

        // TCP keep-alive — prevents idle connections from being killed by
        // firewalls/load balancers, keeping the pool warm between bursts
        if (config_.tcp_keepalive) {
            curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
            curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE,
                             static_cast<long>(config_.tcp_keepalive_idle.count()));
            curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL,
                             static_cast<long>(config_.tcp_keepalive_interval.count()));
        }

        // SSL options - SECURITY: Production mode completely disallows SSL bypass
        // Check if SSL verification is being disabled
        bool ssl_verify_enabled = request.verify_ssl && config_.verify_ssl_by_default;
        bool ssl_bypass_requested = !ssl_verify_enabled;

        if (ssl_bypass_requested) {
            // SECURITY: Check if in production mode - production mode completely blocks SSL bypass
            static bool production_mode = []() {
                auto mode = std::getenv("MERIDIAN_CLUSTER_MODE");
                return mode && std::string(mode) == "production";
            }();

            if (production_mode) {
                // Log warning and ignore the bypass request - SSL bypass is never allowed in production
                static bool production_bypass_warning_shown = false;
                if (!production_bypass_warning_shown) {
                    std::cerr << "SECURITY WARNING: SSL verification bypass requested but ignored in production mode.\n"
                              << "SSL verification cannot be disabled when MERIDIAN_CLUSTER_MODE=production.\n";
                    production_bypass_warning_shown = true;
                }
                // Force SSL verification enabled - bypass is completely blocked in production
                ssl_verify_enabled = true;
            } else {
                // Non-production mode: honor explicit config settings, allow env var overrides
                static bool ssl_warning_shown = false;

                // Check environment variables for global overrides
                const char* allow_insecure = std::getenv("MERIDIAN_ALLOW_INSECURE_SSL");
                const char* testing_mode = std::getenv("MERIDIAN_TESTING");
                bool env_allows_bypass = (testing_mode != nullptr && std::string(testing_mode) != "0") ||
                                         (allow_insecure != nullptr && std::string(allow_insecure) != "0");

                // In non-production, honor explicit verify_ssl=false from config
                // The config setting (request.verify_ssl=false) indicates explicit user intent
                // Only show warning once, don't require environment variables
                if (!ssl_warning_shown) {
                    if (env_allows_bypass) {
                        std::cerr << "SECURITY WARNING: SSL verification disabled via environment variable - NOT FOR PRODUCTION USE.\n"
                                  << "This exposes connections to man-in-the-middle attacks.\n";
                    } else {
                        std::cerr << "SECURITY WARNING: SSL verification disabled via configuration.\n"
                                  << "This exposes connections to man-in-the-middle attacks.\n"
                                  << "For production use, enable SSL verification or use trusted certificates.\n";
                    }
                    ssl_warning_shown = true;
                }
                // Apply bypass - honor the config setting (ssl_verify_enabled remains false)
            }
        }

        if (ssl_verify_enabled) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
        } else {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        }

        if (!request.ca_bundle_path.empty()) {
            curl_easy_setopt(curl, CURLOPT_CAINFO, request.ca_bundle_path.c_str());
        } else if (!config_.default_ca_bundle.empty()) {
            curl_easy_setopt(curl, CURLOPT_CAINFO, config_.default_ca_bundle.c_str());
        }

        if (!request.client_cert_path.empty()) {
            curl_easy_setopt(curl, CURLOPT_SSLCERT, request.client_cert_path.c_str());
        }
        if (!request.client_key_path.empty()) {
            curl_easy_setopt(curl, CURLOPT_SSLKEY, request.client_key_path.c_str());
        }

        // Byte range
        if (request.byte_range) {
            std::string range = std::to_string(request.byte_range->first) + "-" +
                                std::to_string(request.byte_range->second);
            curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());
        }

        // Proxy
        if (!config_.proxy_url.empty()) {
            curl_easy_setopt(curl, CURLOPT_PROXY, config_.proxy_url.c_str());
            if (!config_.proxy_username.empty()) {
                std::string proxy_auth = config_.proxy_username + ":" + config_.proxy_password;
                curl_easy_setopt(curl, CURLOPT_PROXYUSERPWD, proxy_auth.c_str());
            }
        }

        // Follow redirects
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10L);

        // DNS cache
        curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT,
                         static_cast<long>(config_.dns_cache_timeout.count()));

        // Verbose
        if (config_.verbose) {
            curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
        }

        // Execute
        auto start_time = std::chrono::steady_clock::now();
        CURLcode res = curl_easy_perform(curl);
        auto end_time = std::chrono::steady_clock::now();

        response.total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        if (res == CURLE_OK) {
            // Check if response size limit was exceeded
            if (write_ctx.size_exceeded) {
                response.error = "Response body exceeded maximum size limit of " +
                                 std::to_string(config_.max_response_size) + " bytes";
                response.is_network_error = false;
                response.status_code = 413;  // Payload Too Large

                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.total_requests++;
                stats_.failed_requests++;
            } else {
                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.status_code);

                double connect_time;
                curl_easy_getinfo(curl, CURLINFO_CONNECT_TIME, &connect_time);
                response.connect_time = std::chrono::milliseconds(
                    static_cast<long>(connect_time * 1000));

                response.body = std::move(response_body);

                // Update stats
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.total_requests++;
            }
        } else if (res == CURLE_WRITE_ERROR && write_ctx.size_exceeded) {
            // Response size limit exceeded
            response.error = "Response body exceeded maximum size limit of " +
                             std::to_string(config_.max_response_size) + " bytes";
            response.is_network_error = false;
            response.status_code = 413;

            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.total_requests++;
            stats_.failed_requests++;
        } else {
            response.error = curl_easy_strerror(res);
            response.is_network_error = true;

            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.total_requests++;
            stats_.failed_requests++;
        }

        // Cleanup headers
        if (headers_list) {
            curl_slist_free_all(headers_list);
        }

        // Return handle to pool
        release_handle(curl);

        return response;
    }

    HttpResponse execute_with_retry(const HttpRequest& request) {
        int retries = 0;
        auto delay = request.initial_retry_delay;

        while (true) {
            HttpResponse response = execute(request);

            // Success or non-retryable error
            if (!response.is_network_error && !is_retryable_status(response.status_code)) {
                return response;
            }

            // Check retry limit
            if (retries >= request.max_retries) {
                return response;
            }

            // Wait before retry
            std::this_thread::sleep_for(delay);

            // Exponential backoff
            delay = std::chrono::milliseconds(
                static_cast<long>(delay.count() * request.retry_backoff_multiplier));
            retries++;
        }
    }

    const HttpClientConfig& config() const { return config_; }

    PoolStats pool_stats() const {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        return stats_;
    }

private:
    HttpClientConfig config_;
    CURLM* multi_handle_ = nullptr;
    mutable std::mutex stats_mutex_;
    PoolStats stats_;

    // Connection pool
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
    std::vector<CURL*> idle_handles_;
    std::atomic<bool> health_check_running_{false};
    std::thread health_check_thread_;
};

// ============================================================================
// HttpAsyncOperation Implementation
// ============================================================================

class HttpAsyncOperationImpl : public HttpAsyncOperation {
public:
    HttpAsyncOperationImpl(HttpClient* client, const HttpRequest& request)
        : client_(client), request_(request), complete_(false), cancelled_(false) {
        // Start async operation in a thread
        thread_ = std::thread([this]() {
            response_ = client_->execute(request_);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                complete_ = true;
            }
            cv_.notify_all();
        });
    }

    ~HttpAsyncOperationImpl() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    bool is_complete() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return complete_;
    }

    HttpResponse wait() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return complete_; });
        if (thread_.joinable()) {
            thread_.join();
        }
        return response_;
    }

    void cancel() override {
        std::lock_guard<std::mutex> lock(mutex_);
        cancelled_ = true;
    }

private:
    HttpClient* client_;
    HttpRequest request_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
    bool complete_;
    bool cancelled_;
    HttpResponse response_;
};

// ============================================================================
// HttpClient public interface
// ============================================================================

HttpClient::HttpClient(const HttpClientConfig& config)
    : impl_(std::make_unique<Impl>(config)) {}

HttpClient::~HttpClient() = default;

HttpClient::HttpClient(HttpClient&&) noexcept = default;
HttpClient& HttpClient::operator=(HttpClient&&) noexcept = default;

HttpResponse HttpClient::execute(const HttpRequest& request) {
    return impl_->execute(request);
}

HttpResponse HttpClient::execute_with_retry(const HttpRequest& request) {
    return impl_->execute_with_retry(request);
}

std::unique_ptr<HttpAsyncOperation> HttpClient::execute_async(const HttpRequest& request) {
    return std::make_unique<HttpAsyncOperationImpl>(this, request);
}

HttpResponse HttpClient::get(const std::string& url, const HttpHeaders& headers) {
    HttpRequest req = HttpRequest::get(url);
    req.headers = headers;
    return execute(req);
}

HttpResponse HttpClient::post(const std::string& url, const std::vector<uint8_t>& body,
                               const HttpHeaders& headers) {
    HttpRequest req = HttpRequest::post(url, body);
    req.headers = headers;
    return execute(req);
}

HttpResponse HttpClient::post_json(const std::string& url, const std::string& json,
                                    const HttpHeaders& headers) {
    HttpRequest req = HttpRequest::post(url, json);
    req.headers = headers;
    req.headers.set_content_type("application/json");
    return execute(req);
}

HttpResponse HttpClient::put(const std::string& url, const std::vector<uint8_t>& body,
                              const HttpHeaders& headers) {
    HttpRequest req = HttpRequest::put(url, body);
    req.headers = headers;
    return execute(req);
}

HttpResponse HttpClient::del(const std::string& url, const HttpHeaders& headers) {
    HttpRequest req = HttpRequest::del(url);
    req.headers = headers;
    return execute(req);
}

HttpResponse HttpClient::download(const std::string& url, const std::string& path,
                                   HttpProgressCallback progress) {
    HttpRequest req = HttpRequest::get(url);
    req.progress_callback = progress;

    HttpResponse response = execute(req);

    if (response.ok()) {
        std::ofstream file(path, std::ios::binary);
        if (file) {
            file.write(reinterpret_cast<const char*>(response.body.data()),
                       static_cast<std::streamsize>(response.body.size()));
        } else {
            response.error = "Failed to open file for writing: " + path;
            response.is_network_error = false;
        }
    }

    return response;
}

HttpResponse HttpClient::upload(const std::string& url, const std::string& path,
                                 HttpProgressCallback progress) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        HttpResponse response;
        response.error = "Failed to open file for reading: " + path;
        response.is_network_error = false;
        return response;
    }

    auto size = file.tellg();
    if (size < 0) {
        HttpResponse response;
        response.status_code = 0;
        response.error = "Failed to determine file size: " + path;
        response.is_network_error = false;
        return response;
    }
    file.seekg(0);

    std::vector<uint8_t> data(static_cast<size_t>(size));
    file.read(reinterpret_cast<char*>(data.data()), size);

    HttpRequest req = HttpRequest::put(url, data);
    req.progress_callback = progress;

    return execute(req);
}

const HttpClientConfig& HttpClient::config() const {
    return impl_->config();
}

HttpClient::PoolStats HttpClient::pool_stats() const {
    return impl_->pool_stats();
}

// ============================================================================
// H7.2 FIX: Batch/parallel request execution using CURL multi handle
// ============================================================================

std::vector<HttpResponse> HttpClient::execute_batch(const std::vector<HttpRequest>& requests) {
    if (requests.empty()) {
        return {};
    }

    // For single request, just use normal execute
    if (requests.size() == 1) {
        return {execute(requests[0])};
    }

    std::vector<HttpResponse> responses(requests.size());

    // Get the multi handle from impl
    CURLM* multi_handle = curl_multi_init();
    if (!multi_handle) {
        // Fall back to sequential execution
        for (size_t i = 0; i < requests.size(); ++i) {
            responses[i] = execute(requests[i]);
        }
        return responses;
    }

    // Configure multi handle for connection reuse and pipelining
    curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
    curl_multi_setopt(multi_handle, CURLMOPT_MAX_HOST_CONNECTIONS,
                      static_cast<long>(impl_->config().max_connections_per_host));

    // Track easy handles and their associated data
    struct RequestContext {
        CURL* easy = nullptr;
        size_t index = 0;
        std::vector<uint8_t> response_body;
        HttpHeaders response_headers;
        struct curl_slist* request_headers = nullptr;
        WriteCallbackContext write_ctx{nullptr, 0, 0, false};
    };
    std::vector<RequestContext> contexts(requests.size());

    // Set up each request
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto& ctx = contexts[i];
        ctx.index = i;

        CURL* easy = curl_easy_init();
        if (!easy) {
            responses[i].error = "Failed to create CURL handle";
            responses[i].is_network_error = true;
            continue;
        }
        ctx.easy = easy;

        // URL
        curl_easy_setopt(easy, CURLOPT_URL, request.url.c_str());

        // Method
        switch (request.method) {
            case HttpMethod::GET:
                curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
                break;
            case HttpMethod::POST:
                curl_easy_setopt(easy, CURLOPT_POST, 1L);
                break;
            case HttpMethod::PUT:
                curl_easy_setopt(easy, CURLOPT_UPLOAD, 1L);
                break;
            case HttpMethod::DELETE:
                curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "DELETE");
                break;
            case HttpMethod::PATCH:
                curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "PATCH");
                break;
            case HttpMethod::HEAD:
                curl_easy_setopt(easy, CURLOPT_NOBODY, 1L);
                break;
            case HttpMethod::OPTIONS:
                curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "OPTIONS");
                break;
        }

        // Headers
        for (const auto& [name, value] : request.headers.all()) {
            std::string header = name + ": " + value;
            ctx.request_headers = curl_slist_append(ctx.request_headers, header.c_str());
        }
        if (ctx.request_headers) {
            curl_easy_setopt(easy, CURLOPT_HTTPHEADER, ctx.request_headers);
        }

        // User agent
        if (!impl_->config().user_agent.empty()) {
            curl_easy_setopt(easy, CURLOPT_USERAGENT, impl_->config().user_agent.c_str());
        }

        // Request body (for POST/PUT/PATCH)
        if (!request.body.empty()) {
            curl_easy_setopt(easy, CURLOPT_POSTFIELDS, request.body.data());
            curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE,
                             static_cast<long>(request.body.size()));
        }

        // Response body callback
        ctx.write_ctx.response = &ctx.response_body;
        ctx.write_ctx.max_size = impl_->config().max_response_size;
        ctx.write_ctx.current_size = 0;
        ctx.write_ctx.size_exceeded = false;
        curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(easy, CURLOPT_WRITEDATA, &ctx.write_ctx);

        // Response headers callback
        curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, header_callback);
        curl_easy_setopt(easy, CURLOPT_HEADERDATA, &ctx.response_headers);

        // Timeouts
        curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS,
                         static_cast<long>(request.connect_timeout.count()));
        curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,
                         static_cast<long>(request.total_timeout.count()));

        // TCP keep-alive for multi-handle path
        if (impl_->config().tcp_keepalive) {
            curl_easy_setopt(easy, CURLOPT_TCP_KEEPALIVE, 1L);
            curl_easy_setopt(easy, CURLOPT_TCP_KEEPIDLE,
                             static_cast<long>(impl_->config().tcp_keepalive_idle.count()));
            curl_easy_setopt(easy, CURLOPT_TCP_KEEPINTVL,
                             static_cast<long>(impl_->config().tcp_keepalive_interval.count()));
        }

        // SSL - HIGH FIX: Apply the same production mode SSL bypass protection
        // as the single-request execute() path. Previously, execute_batch()
        // allowed SSL verification to be skipped without the production mode
        // check, creating an inconsistency that could be exploited.
        {
            bool ssl_verify_enabled = request.verify_ssl && impl_->config().verify_ssl_by_default;
            if (!ssl_verify_enabled) {
                // Check production mode - block SSL bypass in production
                static bool production_mode = []() {
                    auto mode = std::getenv("MERIDIAN_CLUSTER_MODE");
                    return mode && std::string(mode) == "production";
                }();

                if (production_mode) {
                    // Force SSL verification in production mode
                    ssl_verify_enabled = true;
                }
            }

            if (ssl_verify_enabled) {
                curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER, 1L);
                curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST, 2L);
            } else {
                curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER, 0L);
                curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST, 0L);
            }
        }

        // Follow redirects
        curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(easy, CURLOPT_MAXREDIRS, 10L);

        // Store index for later retrieval
        curl_easy_setopt(easy, CURLOPT_PRIVATE, reinterpret_cast<void*>(i));

        // Add to multi handle
        curl_multi_add_handle(multi_handle, easy);
    }

    // Execute all requests in parallel
    int still_running = 0;
    do {
        CURLMcode mc = curl_multi_perform(multi_handle, &still_running);
        if (mc == CURLM_OK && still_running > 0) {
            // Wait for activity with timeout
            int numfds;
            mc = curl_multi_wait(multi_handle, nullptr, 0, 1000, &numfds);
        }
        if (mc != CURLM_OK) {
            break;
        }
    } while (still_running > 0);

    // Process completed transfers
    CURLMsg* msg;
    int msgs_left;
    while ((msg = curl_multi_info_read(multi_handle, &msgs_left))) {
        if (msg->msg == CURLMSG_DONE) {
            CURL* easy = msg->easy_handle;

            // Get the index from private data
            void* private_data;
            curl_easy_getinfo(easy, CURLINFO_PRIVATE, &private_data);
            size_t idx = reinterpret_cast<size_t>(private_data);

            auto& response = responses[idx];
            auto& ctx = contexts[idx];

            if (msg->data.result == CURLE_OK) {
                if (ctx.write_ctx.size_exceeded) {
                    response.error = "Response body exceeded maximum size limit";
                    response.is_network_error = false;
                    response.status_code = 413;
                } else {
                    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response.status_code);
                    response.body = std::move(ctx.response_body);
                    response.headers = std::move(ctx.response_headers);

                    double connect_time;
                    curl_easy_getinfo(easy, CURLINFO_CONNECT_TIME, &connect_time);
                    response.connect_time = std::chrono::milliseconds(
                        static_cast<long>(connect_time * 1000));

                    double total_time;
                    curl_easy_getinfo(easy, CURLINFO_TOTAL_TIME, &total_time);
                    response.total_time = std::chrono::milliseconds(
                        static_cast<long>(total_time * 1000));
                }
            } else {
                response.error = curl_easy_strerror(msg->data.result);
                response.is_network_error = true;
            }
        }
    }

    // Cleanup
    for (auto& ctx : contexts) {
        if (ctx.easy) {
            curl_multi_remove_handle(multi_handle, ctx.easy);
            curl_easy_cleanup(ctx.easy);
        }
        if (ctx.request_headers) {
            curl_slist_free_all(ctx.request_headers);
        }
    }
    curl_multi_cleanup(multi_handle);

    return responses;
}

HttpClient::BatchOperation HttpClient::execute_batch_async(const std::vector<HttpRequest>& requests) {
    BatchOperation op;
    op.futures.reserve(requests.size());

    for (const auto& request : requests) {
        op.futures.push_back(std::async(std::launch::async, [this, request]() {
            return execute(request);
        }));
    }

    return op;
}

std::vector<HttpResponse> HttpClient::BatchOperation::wait_all() {
    std::vector<HttpResponse> responses;
    responses.reserve(futures.size());

    for (auto& future : futures) {
        responses.push_back(future.get());
    }

    return responses;
}

bool HttpClient::BatchOperation::all_complete() const {
    for (const auto& future : futures) {
        if (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
            return false;
        }
    }
    return true;
}

// ============================================================================
// AwsSigV4Signer
// ============================================================================

AwsSigV4Signer::AwsSigV4Signer(const std::string& access_key_id,
                               const std::string& secret_access_key,
                               const std::string& region,
                               const std::string& service)
    : access_key_id_(access_key_id)
    , secret_access_key_(secret_access_key)
    , region_(region)
    , service_(service) {}

static std::string sha256_hex(const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.c_str()), data.size(), hash);

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        oss << std::setw(2) << static_cast<int>(hash[i]);
    }
    return oss.str();
}

static std::string sha256_hex(const std::vector<uint8_t>& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(data.data(), data.size(), hash);

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        oss << std::setw(2) << static_cast<int>(hash[i]);
    }
    return oss.str();
}

static std::vector<uint8_t> hmac_sha256(const std::vector<uint8_t>& key,
                                        const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    unsigned int hash_len;

    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash, &hash_len);

    return std::vector<uint8_t>(hash, hash + hash_len);
}

static std::vector<uint8_t> hmac_sha256(const std::string& key, const std::string& data) {
    return hmac_sha256(std::vector<uint8_t>(key.begin(), key.end()), data);
}

static std::string hmac_sha256_hex(const std::vector<uint8_t>& key, const std::string& data) {
    auto hash = hmac_sha256(key, data);

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (const auto b : hash) {
        oss << std::setw(2) << static_cast<int>(b);
    }
    return oss.str();
}

static std::string get_current_datetime() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm;
    meridian::portable_gmtime(&time, &tm);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%dT%H%M%SZ");
    return oss.str();
}

static std::string get_current_date() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm;
    meridian::portable_gmtime(&time, &tm);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d");
    return oss.str();
}

// Helper to build canonical query string for AWS SigV4
// Query params are already URL-encoded in the URL, so we just need to sort them
// and ensure params without values have the format "key=" (not just "key")
static std::string build_canonical_query_string(const std::string& query) {
    if (query.empty()) {
        return "";
    }

    // Parse query parameters into key-value pairs (already encoded)
    std::map<std::string, std::string> params;
    size_t pos = 0;
    while (pos < query.size()) {
        size_t amp = query.find('&', pos);
        if (amp == std::string::npos) amp = query.size();

        std::string param = query.substr(pos, amp - pos);
        size_t eq = param.find('=');
        if (eq != std::string::npos) {
            // Already has key=value format
            params[param.substr(0, eq)] = param.substr(eq + 1);
        } else {
            // Parameter without value (e.g., "uploads") - add empty value
            params[param] = "";
        }
        pos = amp + 1;
    }

    // Build sorted query string (values already encoded, don't re-encode)
    std::ostringstream oss;
    bool first = true;
    for (const auto& [key, value] : params) {
        if (!first) oss << "&";
        oss << key << "=" << value;
        first = false;
    }
    return oss.str();
}

std::string AwsSigV4Signer::get_canonical_request(const HttpRequest& request,
                                                   const std::string& signed_headers,
                                                   const std::string& payload_hash) const {
    auto url = ParsedUrl::parse(request.url);
    if (!url) return "";

    std::ostringstream oss;

    // Method
    oss << http_method_to_string(request.method) << "\n";

    // Canonical URI
    std::string path = url->path.empty() ? "/" : url->path;
    oss << path << "\n";

    // Canonical query string (sorted and URL-encoded per AWS SigV4 spec)
    oss << build_canonical_query_string(url->query) << "\n";

    // Canonical headers
    std::map<std::string, std::string> sorted_headers;
    for (const auto& [name, value] : request.headers.all()) {
        std::string lower_name = name;
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        sorted_headers[lower_name] = value;
    }

    for (const auto& [name, value] : sorted_headers) {
        oss << name << ":" << value << "\n";
    }
    oss << "\n";

    // Signed headers
    oss << signed_headers << "\n";

    // Payload hash
    oss << payload_hash;

    return oss.str();
}

std::string AwsSigV4Signer::get_string_to_sign(const std::string& datetime,
                                                const std::string& date,
                                                const std::string& canonical_request) const {
    std::ostringstream oss;
    oss << "AWS4-HMAC-SHA256\n";
    oss << datetime << "\n";
    oss << date << "/" << region_ << "/" << service_ << "/aws4_request\n";
    oss << sha256_hex(canonical_request);
    return oss.str();
}

std::string AwsSigV4Signer::calculate_signature(const std::string& date,
                                                 const std::string& string_to_sign) const {
    auto k_date = hmac_sha256("AWS4" + secret_access_key_, date);
    auto k_region = hmac_sha256(k_date, region_);
    auto k_service = hmac_sha256(k_region, service_);
    auto k_signing = hmac_sha256(k_service, "aws4_request");

    return hmac_sha256_hex(k_signing, string_to_sign);
}

void AwsSigV4Signer::sign(HttpRequest& request) const {
    std::string datetime = get_current_datetime();
    std::string date = get_current_date();

    // Parse URL to get host
    auto url = ParsedUrl::parse(request.url);
    if (!url) return;

    // Add required headers
    request.headers.set("Host", url->host);
    request.headers.set("X-Amz-Date", datetime);

    // Reuse pre-set payload hash (e.g. UNSIGNED-PAYLOAD) or compute it
    std::string payload_hash;
    if (auto existing = request.headers.get("X-Amz-Content-Sha256"); existing && !existing->empty()) {
        payload_hash = *existing;
    } else if (auto existing2 = request.headers.get("x-amz-content-sha256"); existing2 && !existing2->empty()) {
        payload_hash = *existing2;
    } else {
        payload_hash = request.body.empty() ? sha256_hex("") : sha256_hex(request.body);
    }
    request.headers.set("X-Amz-Content-Sha256", payload_hash);

    // Build signed headers list - use set for O(1) dedup, then sort
    std::unordered_set<std::string> header_set;
    for (const auto& [name, value] : request.headers.all()) {
        std::string lower = name;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        header_set.insert(std::move(lower));
    }
    std::vector<std::string> header_names(header_set.begin(), header_set.end());
    std::sort(header_names.begin(), header_names.end());

    std::string signed_headers;
    // Estimate size: each header name plus semicolon separator
    size_t est_size = header_names.size();  // for separators
    for (const auto& h : header_names) est_size += h.size();
    signed_headers.reserve(est_size);
    for (size_t i = 0; i < header_names.size(); ++i) {
        if (i > 0) signed_headers += ";";
        signed_headers += header_names[i];
    }

    // Build canonical request
    std::string canonical_request = get_canonical_request(request, signed_headers, payload_hash);

    // Build string to sign
    std::string string_to_sign = get_string_to_sign(datetime, date, canonical_request);

    // Calculate signature
    std::string signature = calculate_signature(date, string_to_sign);

    // Build Authorization header
    std::ostringstream auth;
    auth << "AWS4-HMAC-SHA256 ";
    auth << "Credential=" << access_key_id_ << "/" << date << "/" << region_ << "/"
         << service_ << "/aws4_request, ";
    auth << "SignedHeaders=" << signed_headers << ", ";
    auth << "Signature=" << signature;

    request.headers.set("Authorization", auth.str());
}

void AwsSigV4Signer::sign_with_token(HttpRequest& request,
                                      const std::string& session_token) const {
    request.headers.set("X-Amz-Security-Token", session_token);
    sign(request);
}

} // namespace meridian::net
