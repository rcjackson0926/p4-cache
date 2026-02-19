# Configuration Reference

`p4-cache` can be configured via CLI flags, a JSON config file, or a combination of both. CLI flags take precedence over JSON values.

## CLI Flags

### Required

| Flag | Description |
|------|-------------|
| `--depot-path <path>` | Path to the Perforce depot directory on NVMe. Must exist and be a directory. |
| `--primary-type <type>` | Primary storage backend type: `s3`, `azure`, `gcs`, or `nfs`. |

### Mode

| Flag | Description |
|------|-------------|
| `--read-only` | Read-only replica mode. Disables upload workers and fanotify write monitoring. Files are only fetched and cached locally with LRU eviction. |

### Primary Backend (`--primary-*`)

These flags configure the primary storage backend. All uploads go here, and it is checked first on restores.

#### S3 / S3-compatible (MinIO, Ceph, etc.)

| Flag | Param key | Description |
|------|-----------|-------------|
| `--primary-type s3` | `type` | Select S3 backend |
| `--primary-endpoint <url>` | `endpoint` | S3 endpoint URL (e.g., `https://s3.amazonaws.com`) |
| `--primary-bucket <name>` | `bucket` | **Required.** S3 bucket name |
| `--primary-region <region>` | `region` | AWS region (default: `us-east-1`) |
| `--primary-access-key <key>` | `access_key` | Access key ID. Also reads `AWS_ACCESS_KEY_ID` env |
| `--primary-secret-key <key>` | `secret_key` | Secret access key. Also reads `AWS_SECRET_ACCESS_KEY` env |
| `--primary-prefix <prefix>` | `path_prefix` | Key prefix in bucket (default: depot directory basename) |
| `--primary-ca-cert <path>` | `ca_cert_path` | CA certificate file for SSL |
| `--primary-no-verify-ssl` | `verify_ssl=false` | Disable SSL certificate verification |
| `--primary-sse` | `server_side_encryption=true` | Enable server-side encryption |

#### Azure Blob Storage

| Flag | Param key | Description |
|------|-----------|-------------|
| `--primary-type azure` | `type` | Select Azure backend |
| `--primary-endpoint <url>` | `endpoint` | Azure Blob endpoint URL |
| `--primary-container <name>` | `container` | **Required.** Blob container name |
| `--primary-account-name <name>` | `account_name` | Storage account name |
| `--primary-account-key <key>` | `account_key` | Storage account key |
| `--primary-sas-token <token>` | `sas_token` | Shared Access Signature token |
| `--primary-prefix <prefix>` | `path_prefix` | Blob name prefix (default: depot directory basename) |

#### Google Cloud Storage

| Flag | Param key | Description |
|------|-----------|-------------|
| `--primary-type gcs` | `type` | Select GCS backend |
| `--primary-endpoint <url>` | `endpoint` | GCS endpoint URL (for emulators) |
| `--primary-bucket <name>` | `bucket` | **Required.** GCS bucket name |
| `--primary-project-id <id>` | `project_id` | GCP project ID |
| `--primary-credentials-file <path>` | `credentials_file` | Path to service account JSON |
| `--primary-prefix <prefix>` | `path_prefix` | Object name prefix (default: depot directory basename) |

#### NFS / Local Filesystem

| Flag | Param key | Description |
|------|-----------|-------------|
| `--primary-type nfs` | `type` | Select local filesystem backend |
| `--primary-path <path>` | `path` | **Required.** Path to storage directory. Must exist. |

No prefix is applied for NFS backends.

### Secondary Backend (`--secondary-*`)

The secondary backend is an optional read-only fallback. It uses the exact same flags as the primary backend, but with the `--secondary-` prefix instead of `--primary-`.

The secondary is **never written to** — uploads always go to the primary. When a restore from the primary fails, the daemon automatically tries the secondary before returning a failure.

Use cases:
- Disaster recovery: secondary points to a separate region or provider
- Migration: secondary points to the old storage location during a backend migration
- Cost optimization: secondary points to a cheaper cold-storage tier

### Cache Settings

| Flag | Default | Description |
|------|---------|-------------|
| `--max-cache-gb <N>` | 100 | Maximum NVMe cache size in gigabytes |
| `--max-cache-bytes <N>` | — | Maximum NVMe cache size in bytes (overrides `--max-cache-gb`) |
| `--low-watermark-gb <N>` | 80 | Eviction starts when cache exceeds this threshold |
| `--low-watermark-bytes <N>` | — | Eviction threshold in bytes (overrides `--low-watermark-gb`) |

Eviction target is automatically computed as 90% of the low watermark. When eviction triggers, it continues until the cache drops below the target.

### Worker Threads

| Flag | Default | Description |
|------|---------|-------------|
| `--upload-threads <N>` | 8 | Number of upload worker threads (ignored in read-only mode) |
| `--restore-threads <N>` | 16 | Number of restore worker threads |

Upload concurrency (concurrent PUT operations per upload cycle) defaults to 16 and can be set via JSON config (`upload_concurrency`).

### Daemon Settings

| Flag | Default | Description |
|------|---------|-------------|
| `--daemon` | off | Fork to background as a daemon (double-fork) |
| `--verbose` | off | Enable verbose logging (individual file uploads, evictions, restores) |
| `--pid-file <path>` | — | Write PID to file (removed on clean shutdown) |
| `--log-file <path>` | — | Redirect stdout and stderr to file (append mode) |
| `--stats-interval <secs>` | 60 | How often to log aggregate stats. Set to 0 to disable. |
| `--metrics-file <path>` | — | Write Prometheus metrics to this `.prom` file (for node_exporter textfile collector) |
| `--metrics-interval <secs>` | 15 | How often to write the `.prom` file |
| `--config <path>` | — | Load a JSON configuration file (see below) |

## JSON Configuration

Pass `--config <path>` to load a JSON file. JSON values are overlaid onto defaults; CLI flags override JSON values.

### Schema

```json
{
  "depot_path": "/mnt/nvme/depot",
  "read_only": false,

  "primary": {
    "type": "s3",
    "endpoint": "https://s3.amazonaws.com",
    "bucket": "my-p4-depot",
    "region": "us-east-1",
    "access_key": "AKIA...",
    "secret_key": "...",
    "path_prefix": "depot",
    "verify_ssl": "true",
    "server_side_encryption": "false"
  },

  "secondary": {
    "type": "nfs",
    "path": "/mnt/nfs/depot-archive"
  },

  "max_cache_gb": 500,
  "low_watermark_gb": 400,
  "upload_threads": 8,
  "upload_concurrency": 16,
  "restore_threads": 16,
  "verbose": false,
  "stats_interval": 60,
  "metrics_file": "/var/lib/node_exporter/textfile/p4cache.prom",
  "metrics_interval": 15
}
```

### JSON Field Reference

| Field | Type | Description |
|-------|------|-------------|
| `depot_path` | string | Depot directory path |
| `read_only` | bool | Read-only replica mode |
| `primary` | object | Primary backend config (see below) |
| `secondary` | object | Optional secondary backend config |
| `max_cache_gb` | integer | Max cache size in GB |
| `low_watermark_gb` | integer | Eviction threshold in GB |
| `upload_threads` | integer | Upload worker thread count |
| `upload_concurrency` | integer | Concurrent PUT operations |
| `restore_threads` | integer | Restore worker thread count |
| `verbose` | bool | Verbose logging |
| `stats_interval` | integer | Stats log interval in seconds |
| `metrics_file` | string | Path to `.prom` file for Prometheus textfile collector |
| `metrics_interval` | integer | Metrics write interval in seconds (default 15) |

### Backend Object Fields

Within the `"primary"` or `"secondary"` JSON objects, `"type"` is parsed as the backend type and all other keys are stored as backend parameters:

```json
{
  "type": "s3",
  "bucket": "my-bucket",
  "region": "us-east-1"
}
```

is equivalent to:

```
BackendConfig { type: "s3", params: { "bucket": "my-bucket", "region": "us-east-1" } }
```

Any key can be specified in the backend object — the params map is passed directly to `StorageBackendFactory::create()`.

## Environment Variables

| Variable | Applies to | Description |
|----------|------------|-------------|
| `AWS_ACCESS_KEY_ID` | `--primary-type s3` | S3 access key (used when `--primary-access-key` is not set) |
| `AWS_SECRET_ACCESS_KEY` | `--primary-type s3` | S3 secret key (used when `--primary-secret-key` is not set) |
| `P4CACHE_DEPOT` | `libp4shim.so` | Depot path for the LD_PRELOAD shim |
| `P4CACHE_SOCK` | `libp4shim.so` | Unix socket path (default: `<depot>/.p4cache/shim.sock`) |

## Defaults

| Setting | Default value |
|---------|---------------|
| `state_dir` | `<depot_path>/.p4cache/` |
| `path_prefix` | `<depot_path basename>` (for cloud backends; empty for NFS) |
| `max_cache_bytes` | 100 GB |
| `eviction_low_watermark` | 80 GB |
| `eviction_target` | 90% of `eviction_low_watermark` (72 GB at default) |
| `upload_threads` | 8 |
| `upload_concurrency` | 16 |
| `upload_batch_size` | 64 |
| `upload_poll_interval` | 100 ms |
| `restore_threads` | 16 |
| `stats_interval_secs` | 60 |
| `metrics_interval_secs` | 15 |

## Validation Rules

The daemon validates configuration at startup and exits with an error if any rule fails:

- `depot_path` must be set, exist, and be a directory
- `primary.type` must be set to `s3`, `azure`, `gcs`, or `nfs`
- S3 primary requires `bucket`
- Azure primary requires `container`
- GCS primary requires `bucket`
- NFS primary requires `path` (must exist)
- If a secondary is configured, the same type-specific rules apply
- `max_cache_bytes` must be > 0
- `eviction_low_watermark` must be <= `max_cache_bytes`
