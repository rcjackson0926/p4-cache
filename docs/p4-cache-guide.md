# P4 Cache Operations Guide

Quick-reference for running `p4-cache`. For full details see:
- [Configuration Reference](configuration.md) — all CLI flags, JSON fields, environment variables
- [Architecture](architecture.md) — design, threading, data flow, LMDB schema
- [Deployment Guide](deployment.md) — systemd, permissions, monitoring, troubleshooting

## Required Arguments

```bash
p4-cache \
  --depot-path <path> \
  --primary-type <s3|azure|gcs|nfs>
```

## Backend Examples

### S3

```bash
p4-cache \
  --depot-path /p4/depot \
  --primary-type s3 \
  --primary-endpoint https://s3.amazonaws.com \
  --primary-bucket my-depot \
  --primary-region us-east-1
```

S3 credentials: `--primary-access-key` / `--primary-secret-key` or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars.

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

### NFS / Local Filesystem

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

Uploads go to primary only. Restores try primary first, then secondary.

## Common Options

| Flag | Description |
|------|-------------|
| `--read-only` | Replica mode (no uploads, no fanotify) |
| `--max-cache-gb <N>` | Max cache size (default: 100 GB) |
| `--low-watermark-gb <N>` | Eviction threshold (default: 80 GB) |
| `--upload-threads <N>` | Upload workers (default: 8) |
| `--restore-threads <N>` | Restore workers (default: 16) |
| `--daemon` | Fork to background |
| `--verbose` | Log individual file operations |
| `--pid-file <path>` | PID file for daemon mode |
| `--log-file <path>` | Log file (append mode) |
| `--stats-interval <secs>` | Stats logging interval (default: 60) |
| `--metrics-file <path>` | Prometheus `.prom` file for node_exporter |
| `--metrics-interval <secs>` | Metrics write interval (default: 15) |
| `--config <path>` | JSON config file |
| `--primary-prefix <prefix>` | Storage key prefix (default: depot dir name) |
| `--primary-no-verify-ssl` | Disable SSL verification |
| `--primary-sse` | Enable server-side encryption |
| `--primary-ca-cert <path>` | CA certificate for SSL |

## JSON Configuration

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

## LD_PRELOAD Shim

Start P4d with the shim to enable cold-file restore interception:

```bash
LD_PRELOAD=/usr/local/lib/libp4shim.so P4CACHE_DEPOT=/mnt/nvme/depot \
  p4d -r /mnt/nvme/depot -p 1666 -d
```

| Env var | Description |
|---------|-------------|
| `P4CACHE_DEPOT` | Depot path to intercept |
| `P4CACHE_SOCK` | Socket path (default: `<depot>/.p4cache/shim.sock`) |

## Full Production Example

```bash
# Grant fanotify capability
sudo setcap cap_sys_admin+ep /usr/local/bin/p4-cache

# Start daemon
p4-cache \
  --depot-path /mnt/nvme/depot \
  --primary-type s3 \
  --primary-endpoint https://s3.example.com \
  --primary-bucket p4-archive \
  --max-cache-gb 500 \
  --low-watermark-gb 400 \
  --daemon \
  --pid-file /var/run/p4-cache.pid \
  --log-file /var/log/p4-cache.log

# Start P4d with shim
LD_PRELOAD=/usr/local/lib/libp4shim.so P4CACHE_DEPOT=/mnt/nvme/depot \
  p4d -r /mnt/nvme/depot -p ssl:1666 -d
```

## Quick Checks

```bash
# Is daemon running?
kill -0 $(cat /var/run/p4-cache.pid) && echo OK

# Test socket connectivity
echo "FETCH test" | socat - UNIX-CONNECT:/mnt/nvme/depot/.p4cache/shim.sock

# Check manifest stats (use mdb_stat from lmdb-utils)
mdb_stat -a /mnt/nvme/depot/.p4cache/manifest.lmdb
```

## Prometheus Metrics

Enable Prometheus metrics via the textfile collector (no HTTP server needed):

```bash
p4-cache \
  --depot-path /p4/depot \
  --primary-type s3 --primary-bucket my-depot \
  --metrics-file /var/lib/node_exporter/textfile/p4cache.prom \
  --metrics-interval 15
```

Configure node_exporter to pick up the file:
```bash
node_exporter --collector.textfile.directory=/var/lib/node_exporter/textfile/
```

Verify the output:
```bash
cat /var/lib/node_exporter/textfile/p4cache.prom
```

Exported metrics include counters (uploads, restores, evictions, shim requests), gauges (file states, cache size, queue depths), and histograms (operation durations). All metrics carry `depot` and `mode` labels.

## Operational Recommendations

- Size cache and watermarks to your working set, not total depot size.
- Use `--read-only` for replica servers (no `CAP_SYS_ADMIN` needed).
- Watch the periodic stats log for growing dirty counts (upload backlog) or restore failures.
- Use a secondary backend during storage migrations to avoid downtime.
