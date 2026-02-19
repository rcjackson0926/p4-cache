#pragma once

#include <cstddef>
#include <cstdint>

namespace meridian::constants {

// Server defaults
constexpr uint16_t DEFAULT_SERVER_PORT = 9500;
constexpr uint16_t DEFAULT_GRPC_PORT = 9443;
constexpr const char* DEFAULT_LISTEN_ADDRESS = "0.0.0.0";
constexpr const char* DEFAULT_DATA_DIR = "/var/lib/meridian";

// Session defaults
constexpr int DEFAULT_SESSION_TTL_SECONDS = 86400;  // 24 hours
constexpr int DEFAULT_REDIS_PORT = 6379;

// Query limits (petabyte-scale defaults)
constexpr size_t DEFAULT_QUERY_LIMIT = 100000;
constexpr size_t DEFAULT_COMMIT_QUERY_LIMIT = 100000;
constexpr size_t DEFAULT_REPO_QUERY_LIMIT = 100000;

// LDAP defaults
constexpr int DEFAULT_LDAP_PAGE_SIZE = 1000;
constexpr int DEFAULT_LDAP_TIMEOUT_SECONDS = 30;
constexpr int DEFAULT_LDAP_SEARCH_TIMEOUT_SECONDS = 60;

// Storage defaults
constexpr size_t DEFAULT_MULTIPART_THRESHOLD = 5 * 1024 * 1024;        // 5MB
constexpr size_t DEFAULT_MULTIPART_CHUNK_SIZE = 8 * 1024 * 1024;       // 8MB
constexpr size_t DEFAULT_UPLOAD_CONCURRENCY = 8;
constexpr size_t DEFAULT_LARGE_FILE_THRESHOLD = 100 * 1024 * 1024;     // 100MB
constexpr size_t DEFAULT_STREAM_BUFFER_SIZE = 8 * 1024 * 1024;         // 8MB
constexpr size_t DEFAULT_BINARY_CHECK_SIZE = 8192;                      // 8KB

// Rate limiting defaults
constexpr int DEFAULT_RATE_LIMIT = 1000;
constexpr int DEFAULT_WEBHOOK_RATE_LIMIT = 1000;
constexpr int DEFAULT_ROLLING_WINDOW_DAYS = 30;
constexpr size_t DEFAULT_MAX_REQUESTS_TRACKED = 100000;
constexpr int DEFAULT_VIOLATION_DEMOTION_THRESHOLD = 10;
constexpr int DEFAULT_VIOLATION_MULTIPLIER_THRESHOLD = 5;
constexpr int DEFAULT_PROMOTION_MIN_REQUESTS = 1000;

// Rate limit tiers
constexpr int DEFAULT_TIER_ANONYMOUS = 60;
constexpr int DEFAULT_TIER_AUTHENTICATED = 1000;
constexpr int DEFAULT_TIER_PREMIUM = 5000;

// VFS defaults (petabyte-scale)
constexpr size_t DEFAULT_VFS_DIRECTORY_ENTRY_LIMIT = 1000000;
constexpr int DEFAULT_VFS_THREAD_COUNT = 4;
constexpr int DEFAULT_VFS_WEBHOOK_TIMEOUT_SECONDS = 30;
constexpr size_t DEFAULT_VFS_CACHE_SIZE_MB = 1024;

// Cluster defaults
constexpr int DEFAULT_CLUSTER_RAFT_PORT = 9501;
constexpr int DEFAULT_CLUSTER_GRPC_PORT = 9502;
constexpr int DEFAULT_CLUSTER_TTL_SECONDS = 30;
constexpr int DEFAULT_ELECTION_TIMEOUT_MS = 1000;
constexpr int DEFAULT_HEARTBEAT_INTERVAL_MS = 100;
constexpr int DEFAULT_SNAPSHOT_THRESHOLD = 10000;

// HTTP request defaults
constexpr int DEFAULT_HTTP_REQUEST_TIMEOUT_SECONDS = 30;

// Webhook defaults
constexpr int DEFAULT_WEBHOOK_TIMEOUT_SECONDS = 30;
constexpr int DEFAULT_WEBHOOK_MAX_RETRIES = 3;
constexpr int DEFAULT_WEBHOOK_RETRY_DELAY_SECONDS = 60;
constexpr size_t DEFAULT_WEBHOOK_MAX_PAYLOAD_SIZE = 25 * 1024 * 1024;  // 25MB

// CI defaults
constexpr int DEFAULT_CI_MAX_CONCURRENT_JOBS = 4;
constexpr int DEFAULT_CI_JOB_TIMEOUT_SECONDS = 3600;
constexpr size_t DEFAULT_CI_ARTIFACT_MAX_SIZE_MB = 100;

// Backup defaults
constexpr int DEFAULT_BACKUP_RETENTION_COUNT = 7;

// Analytics defaults
constexpr int DEFAULT_ANALYTICS_RETENTION_DAYS = 90;

// Audit defaults
constexpr size_t DEFAULT_AUDIT_QUERY_LIMIT = 100000;

// Diff algorithm (petabyte-scale)
constexpr size_t HISTOGRAM_MAX_OCCURRENCES = 256;

// Index format
constexpr size_t INDEX_ENTRY_FIXED_SIZE = 62;

// Default identities
constexpr const char* DEFAULT_USER_NAME = "Unknown";
constexpr const char* DEFAULT_USER_EMAIL = "unknown@localhost";

} // namespace meridian::constants
