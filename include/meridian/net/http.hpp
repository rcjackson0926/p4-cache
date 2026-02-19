#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <optional>
#include <functional>
#include <chrono>
#include <cstdint>
#include <future>

namespace meridian::net {

// HTTP methods
enum class HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
    HEAD,
    OPTIONS
};

const char* http_method_to_string(HttpMethod method);

// HTTP status codes
enum class HttpStatus {
    // Informational
    Continue = 100,
    SwitchingProtocols = 101,

    // Success
    OK = 200,
    Created = 201,
    Accepted = 202,
    NoContent = 204,
    PartialContent = 206,

    // Redirection
    MovedPermanently = 301,
    Found = 302,
    SeeOther = 303,
    NotModified = 304,
    TemporaryRedirect = 307,
    PermanentRedirect = 308,

    // Client errors
    BadRequest = 400,
    Unauthorized = 401,
    Forbidden = 403,
    NotFound = 404,
    MethodNotAllowed = 405,
    Conflict = 409,
    Gone = 410,
    PreconditionFailed = 412,
    PayloadTooLarge = 413,
    TooManyRequests = 429,

    // Server errors
    InternalServerError = 500,
    NotImplemented = 501,
    BadGateway = 502,
    ServiceUnavailable = 503,
    GatewayTimeout = 504
};

bool is_success_status(int status);
bool is_redirect_status(int status);
bool is_client_error_status(int status);
bool is_server_error_status(int status);
bool is_retryable_status(int status);

// HTTP headers (case-insensitive)
class HttpHeaders {
public:
    void set(const std::string& name, const std::string& value);
    void add(const std::string& name, const std::string& value);
    void remove(const std::string& name);

    std::optional<std::string> get(const std::string& name) const;
    std::vector<std::string> get_all(const std::string& name) const;
    bool has(const std::string& name) const;

    // Iteration
    using HeaderPair = std::pair<std::string, std::string>;
    std::vector<HeaderPair> all() const;

    // Common headers
    void set_content_type(const std::string& content_type);
    void set_content_length(size_t length);
    void set_authorization(const std::string& scheme, const std::string& credentials);
    void set_bearer_token(const std::string& token);
    void set_basic_auth(const std::string& username, const std::string& password);

    std::optional<std::string> content_type() const;
    std::optional<size_t> content_length() const;

private:
    // Headers stored as lowercase name -> values
    std::map<std::string, std::vector<std::string>> headers_;

    static std::string normalize_name(const std::string& name);
};

// Progress callback for transfers
struct HttpProgress {
    size_t download_total = 0;
    size_t download_now = 0;
    size_t upload_total = 0;
    size_t upload_now = 0;
    double download_speed = 0;  // bytes/sec
    double upload_speed = 0;    // bytes/sec
};

using HttpProgressCallback = std::function<bool(const HttpProgress&)>;  // Return false to cancel

// HTTP request
struct HttpRequest {
    HttpMethod method = HttpMethod::GET;
    std::string url;
    HttpHeaders headers;
    std::vector<uint8_t> body;

    // Timeouts
    std::chrono::milliseconds connect_timeout{30000};
    std::chrono::milliseconds total_timeout{300000};  // 5 minutes default

    // Progress
    HttpProgressCallback progress_callback;

    // SSL options
    bool verify_ssl = true;
    std::string ca_bundle_path;  // Empty = system default
    std::string client_cert_path;
    std::string client_key_path;

    // Retry options (handled by HttpClient)
    int max_retries = 3;
    std::chrono::milliseconds initial_retry_delay{1000};
    double retry_backoff_multiplier = 2.0;

    // Range request (for resumable downloads)
    std::optional<std::pair<size_t, size_t>> byte_range;  // start, end (inclusive)

    // Convenience constructors
    static HttpRequest get(const std::string& url);
    static HttpRequest post(const std::string& url, const std::vector<uint8_t>& body);
    static HttpRequest post(const std::string& url, const std::string& body);
    static HttpRequest put(const std::string& url, const std::vector<uint8_t>& body);
    static HttpRequest del(const std::string& url);

    // Set JSON body
    void set_json_body(const std::string& json);
};

// HTTP response
struct HttpResponse {
    int status_code = 0;
    std::string status_message;
    HttpHeaders headers;
    std::vector<uint8_t> body;

    // Timing info
    std::chrono::milliseconds total_time{0};
    std::chrono::milliseconds connect_time{0};

    // Convenience methods
    bool ok() const { return is_success_status(status_code); }
    std::string body_string() const;

    // Error info (for failed requests)
    std::string error;
    bool is_network_error = false;  // True if error was network-level, not HTTP status
};

// MEDIUM FIX: Certificate pinning configuration
// Allows pinning specific certificates or public keys for high-security connections
struct CertificatePinConfig {
    // Mode of pinning
    enum class Mode {
        None,               // No pinning (default)
        PublicKey,          // Pin to public key hash (SPKI)
        Certificate,        // Pin to full certificate hash
        Backup              // Primary + backup pins (for rotation)
    };

    Mode mode = Mode::None;

    // Pinned hashes (SHA-256 of public key or certificate, base64 encoded)
    // For public key pinning, this is the hash of the Subject Public Key Info (SPKI)
    std::vector<std::string> pinned_hashes;

    // Whether to allow leaf certificate (default: also check intermediates)
    bool include_intermediates = true;

    // Behavior on pin failure
    bool fail_on_mismatch = true;     // If false, just log and continue
    bool report_mismatches = true;    // Report mismatches to monitoring

    // Pin expiration (for managed rotation)
    std::optional<std::chrono::system_clock::time_point> pin_expiry;

    // Convenience factory methods
    static CertificatePinConfig pin_public_key(const std::string& spki_hash) {
        CertificatePinConfig cfg;
        cfg.mode = Mode::PublicKey;
        cfg.pinned_hashes.push_back(spki_hash);
        return cfg;
    }

    static CertificatePinConfig pin_public_keys(const std::vector<std::string>& spki_hashes) {
        CertificatePinConfig cfg;
        cfg.mode = Mode::Backup;
        cfg.pinned_hashes = spki_hashes;
        return cfg;
    }

    // Helper to generate SPKI hash from certificate file
    // Returns base64-encoded SHA-256 hash of the public key
    // static std::optional<std::string> compute_spki_hash(const std::string& cert_path);
};

// HTTP client configuration
// N-C9: Pool size and timeouts can be overridden via environment variables:
//   MERIDIAN_CONNECTION_POOL_SIZE - Connections per host (default: 10)
//   MERIDIAN_REQUEST_TIMEOUT - Request timeout in seconds (default: 30)
//   MERIDIAN_MAX_CONCURRENT_REQUESTS - Max concurrent requests (default: 100)
//   MERIDIAN_MAX_PENDING_REQUESTS - Max pending requests (default: 10000)
struct HttpClientConfig {
    // Connection pooling (can be overridden by MERIDIAN_CONNECTION_POOL_SIZE)
    size_t max_connections_per_host = 32;
    size_t max_total_connections = 256;
    std::chrono::seconds connection_idle_timeout{300};  // 5 min — keep connections alive between bursts

    // TCP keep-alive to prevent idle connections from being dropped by firewalls/LBs
    bool tcp_keepalive = true;
    std::chrono::seconds tcp_keepalive_idle{60};      // Start probes after 60s idle
    std::chrono::seconds tcp_keepalive_interval{15};  // Probe every 15s

    // Default timeouts (can be overridden by MERIDIAN_REQUEST_TIMEOUT)
    std::chrono::milliseconds default_connect_timeout{30000};
    std::chrono::milliseconds default_total_timeout{300000};

    // Response size limits (0 = unlimited)
    size_t max_response_size = 100 * 1024 * 1024;  // 100MB default

    // SSL
    bool verify_ssl_by_default = true;
    std::string default_ca_bundle;

    // MEDIUM FIX: Certificate pinning configuration
    // Allows pinning certificates for specific hosts
    std::map<std::string, CertificatePinConfig> certificate_pins;  // host -> pin config

    // User agent
    std::string user_agent = "meridian/1.0";

    // Proxy
    std::string proxy_url;  // Empty = no proxy
    std::string proxy_username;
    std::string proxy_password;

    // DNS
    std::chrono::seconds dns_cache_timeout{60};

    // Connection health check
    std::chrono::seconds health_check_interval{30};

    // Verbose logging (for debugging)
    bool verbose = false;
};

// Async operation handle
class HttpAsyncOperation {
public:
    virtual ~HttpAsyncOperation() = default;

    // Check if complete
    virtual bool is_complete() const = 0;

    // Wait for completion
    virtual HttpResponse wait() = 0;

    // Cancel the operation
    virtual void cancel() = 0;
};

// HTTP client with connection pooling and retry logic
class HttpClient {
public:
    explicit HttpClient(const HttpClientConfig& config = {});
    ~HttpClient();

    // Non-copyable, movable
    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;
    HttpClient(HttpClient&&) noexcept;
    HttpClient& operator=(HttpClient&&) noexcept;

    // Synchronous request
    HttpResponse execute(const HttpRequest& request);

    // Synchronous request with automatic retry
    HttpResponse execute_with_retry(const HttpRequest& request);

    // Asynchronous request
    std::unique_ptr<HttpAsyncOperation> execute_async(const HttpRequest& request);

    // Convenience methods
    HttpResponse get(const std::string& url, const HttpHeaders& headers = {});
    HttpResponse post(const std::string& url, const std::vector<uint8_t>& body,
                      const HttpHeaders& headers = {});
    HttpResponse post_json(const std::string& url, const std::string& json,
                           const HttpHeaders& headers = {});
    HttpResponse put(const std::string& url, const std::vector<uint8_t>& body,
                     const HttpHeaders& headers = {});
    HttpResponse del(const std::string& url, const HttpHeaders& headers = {});

    // Download to file
    HttpResponse download(const std::string& url, const std::string& path,
                          HttpProgressCallback progress = nullptr);

    // Upload from file
    HttpResponse upload(const std::string& url, const std::string& path,
                        HttpProgressCallback progress = nullptr);

    // Get current config
    const HttpClientConfig& config() const;

    // Connection pool stats
    struct PoolStats {
        size_t active_connections = 0;
        size_t idle_connections = 0;
        size_t total_requests = 0;
        size_t failed_requests = 0;
    };
    PoolStats pool_stats() const;

    // H7.2 FIX: Batch/parallel request execution using CURL multi handle
    // Execute multiple requests in parallel, returns responses in same order as requests
    std::vector<HttpResponse> execute_batch(const std::vector<HttpRequest>& requests);

    // H7.2 FIX: Async batch execution - returns immediately, results available via futures
    struct BatchOperation {
        std::vector<std::future<HttpResponse>> futures;

        // Wait for all to complete
        std::vector<HttpResponse> wait_all();

        // Check if all complete
        bool all_complete() const;
    };
    BatchOperation execute_batch_async(const std::vector<HttpRequest>& requests);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// AWS SigV4 signing helper (used for S3)
class AwsSigV4Signer {
public:
    AwsSigV4Signer(const std::string& access_key_id,
                   const std::string& secret_access_key,
                   const std::string& region,
                   const std::string& service);

    // Sign a request
    void sign(HttpRequest& request) const;

    // Sign with session token (for STS credentials)
    void sign_with_token(HttpRequest& request, const std::string& session_token) const;

private:
    std::string access_key_id_;
    std::string secret_access_key_;
    std::string region_;
    std::string service_;

    std::string get_canonical_request(const HttpRequest& request,
                                       const std::string& signed_headers,
                                       const std::string& payload_hash) const;
    std::string get_string_to_sign(const std::string& datetime,
                                    const std::string& date,
                                    const std::string& canonical_request) const;
    std::string calculate_signature(const std::string& date,
                                     const std::string& string_to_sign) const;
};

// URL parsing helper
struct ParsedUrl {
    std::string scheme;   // http, https
    std::string host;
    int port = 0;         // 0 = default for scheme
    std::string path;
    std::string query;
    std::string fragment;
    std::string userinfo; // user:password

    std::string to_string() const;
    int effective_port() const;

    static std::optional<ParsedUrl> parse(const std::string& url);
};

// URL encoding/decoding
std::string url_encode(const std::string& str);
std::string url_decode(const std::string& str);

// Base64 encoding (for Basic auth)
std::string base64_encode(const std::vector<uint8_t>& data);
std::string base64_encode(const std::string& str);
std::vector<uint8_t> base64_decode(const std::string& encoded);

} // namespace meridian::net
