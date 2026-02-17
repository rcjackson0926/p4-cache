# P4 Cache Guide

This guide documents `p4-cache` behavior and configuration.

## 1. Purpose

`p4-cache` provides an NVMe-backed local cache with cloud/NFS object persistence for Perforce-style depot paths. It supports S3, Azure Blob Storage, Google Cloud Storage, and NFS backends, with optional dual-backend (primary + secondary fallback) configuration.

## 2. Required Arguments

```bash
p4-cache \
  --depot-path <path> \
  --primary-type <s3|azure|gcs|nfs>
```

## 3. Backend Configuration

### S3

```bash
p4-cache \
  --depot-path /p4/depot \
  --primary-type s3 \
  --primary-endpoint https://s3.amazonaws.com \
  --primary-bucket my-depot \
  --primary-region us-east-1
```

S3 credentials: `--primary-access-key` / `--primary-secret-key` or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables.

### Azure Blob Storage

```bash
p4-cache \
  --depot-path /p4/depot \
  --primary-type azure \
  --primary-account-name myaccount \
  --primary-account-key mykey \
  --primary-container my-depot
```

### Google Cloud Storage

```bash
p4-cache \
  --depot-path /p4/depot \
  --primary-type gcs \
  --primary-bucket my-depot \
  --primary-project-id my-project \
  --primary-credentials-file /path/to/creds.json
```

### NFS (local/mounted filesystem)

```bash
p4-cache \
  --depot-path /p4/depot \
  --primary-type nfs \
  --primary-path /mnt/nfs/depot-archive
```

### Dual Backend (primary + secondary fallback)

```bash
p4-cache \
  --depot-path /p4/depot \
  --primary-type s3 \
  --primary-bucket my-depot \
  --primary-endpoint https://s3.amazonaws.com \
  --secondary-type nfs \
  --secondary-path /mnt/nfs/depot-archive
```

The secondary backend is read-only: restores try primary first, then fall back to secondary.

## 4. Common Options

- `--read-only`: replica/read-only mode (no uploads)
- `--max-cache-gb <N>`
- `--low-watermark-gb <N>`
- `--upload-threads <N>` / `--restore-threads <N>`
- `--daemon`
- `--verbose`
- `--pid-file <path>`
- `--log-file <path>`
- `--stats-interval <secs>`
- `--primary-prefix <prefix>`: key prefix (default: depot dir name)
- `--primary-no-verify-ssl`: skip SSL verification
- `--primary-sse`: enable server-side encryption
- `--primary-ca-cert <path>`: CA certificate for SSL

## 5. JSON Configuration

```json
{
  "depot_path": "/p4/depot",
  "primary": {
    "type": "s3",
    "endpoint": "https://s3.amazonaws.com",
    "bucket": "my-depot",
    "region": "us-east-1"
  },
  "secondary": {
    "type": "nfs",
    "path": "/mnt/nfs/depot-archive"
  },
  "max_cache_gb": 500,
  "low_watermark_gb": 400
}
```

Load with `--config <path>`.

## 6. LD_PRELOAD Shim

```bash
LD_PRELOAD=/usr/local/lib/libp4shim.so P4CACHE_DEPOT=/mnt/nvme/depot \
  p4d -r /mnt/nvme/depot -p 1666 -d
```

Environment variables:
- `P4CACHE_DEPOT` — depot path prefix to intercept
- `P4CACHE_SOCK` — Unix socket path to daemon (default: `<depot>/.p4cache/shim.sock`)

## 7. Full Example

```bash
p4-cache \
  --depot-path /p4/depot \
  --primary-type s3 \
  --primary-endpoint https://s3.example.com \
  --primary-bucket p4-archive \
  --max-cache-gb 500 \
  --low-watermark-gb 400 \
  --daemon \
  --pid-file /var/run/p4-cache.pid \
  --log-file /var/log/p4-cache.log
```

## 8. Operational Recommendations

- Keep cache and watermark sized to real workload.
- Use read-only mode for replica roles.
- Watch logs and stats interval output for churn and restore/upload backlogs.
- Use secondary backend for disaster recovery or migration scenarios.
