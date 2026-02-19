# Deployment Guide

This guide covers production deployment of `p4-cache` including system setup, service configuration, monitoring, replica servers, and troubleshooting.

## Prerequisites

- Linux kernel 2.6.37+ (fanotify support)
- NVMe storage mounted at the depot path
- Remote storage accessible (S3/Azure/GCS credentials or NFS mount)
- `p4-cache` and `libp4shim.so` built and installed
- `liblmdb-dev` installed (for the LMDB manifest)

## Installation

```bash
# Build
cd build && cmake .. && make -j8

# Install binaries
sudo install -m 755 build/p4-cache /usr/local/bin/
sudo install -m 755 build/libp4shim.so /usr/local/lib/

# Grant fanotify capability (avoids running as root)
sudo setcap cap_sys_admin+ep /usr/local/bin/p4-cache
```

## Permissions

### CAP_SYS_ADMIN for fanotify

The daemon requires `CAP_SYS_ADMIN` to use fanotify for detecting file writes. There are two options:

**Option A: setcap (recommended)**
```bash
sudo setcap cap_sys_admin+ep /usr/local/bin/p4-cache
```

**Option B: systemd AmbientCapabilities**
```ini
[Service]
AmbientCapabilities=CAP_SYS_ADMIN
```

In read-only mode, fanotify is not used and `CAP_SYS_ADMIN` is not required.

### File Permissions

- The daemon needs read access to all depot files (for uploads) and write access to the state directory (`<depot>/.p4cache/`)
- The shim socket (`<depot>/.p4cache/shim.sock`) is created with mode 0666 so P4d (potentially running as a different user) can connect
- The daemon process should run as the same user as P4d, or a user with appropriate access to the depot directory

## systemd Service

### Primary Server (read-write)

Create `/etc/systemd/system/p4-cache.service`:

```ini
[Unit]
Description=P4 Cache Daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
User=perforce
Group=perforce

ExecStart=/usr/local/bin/p4-cache \
    --config /etc/p4-cache/config.json \
    --daemon \
    --pid-file /var/run/p4-cache.pid \
    --log-file /var/log/p4-cache.log
PIDFile=/var/run/p4-cache.pid

Restart=on-failure
RestartSec=5

AmbientCapabilities=CAP_SYS_ADMIN

# Resource limits
LimitNOFILE=65536
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

### Configuration File

Create `/etc/p4-cache/config.json`:

```json
{
  "depot_path": "/mnt/nvme/depot",
  "primary": {
    "type": "s3",
    "endpoint": "https://s3.amazonaws.com",
    "bucket": "my-p4-depot",
    "region": "us-east-1"
  },
  "max_cache_gb": 500,
  "low_watermark_gb": 400,
  "upload_threads": 8,
  "upload_concurrency": 16,
  "restore_threads": 16,
  "stats_interval": 60,
  "metrics_file": "/var/lib/node_exporter/textfile/p4cache.prom",
  "metrics_interval": 15
}
```

S3 credentials via environment (add to systemd `EnvironmentFile`):

```bash
# /etc/p4-cache/env
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

```ini
# Add to [Service] section
EnvironmentFile=/etc/p4-cache/env
```

### Enable and Start

```bash
sudo systemctl daemon-reload
sudo systemctl enable p4-cache
sudo systemctl start p4-cache
sudo systemctl status p4-cache
```

## P4d Integration

### Starting P4d with the Shim

Add the LD_PRELOAD shim to your P4d startup:

```bash
LD_PRELOAD=/usr/local/lib/libp4shim.so \
P4CACHE_DEPOT=/mnt/nvme/depot \
  p4d -r /mnt/nvme/depot -p ssl:1666 -d
```

For systemd-managed P4d, add to the service file:

```ini
[Service]
Environment="LD_PRELOAD=/usr/local/lib/libp4shim.so"
Environment="P4CACHE_DEPOT=/mnt/nvme/depot"
```

### Startup Order

1. Start `p4-cache` first (creates the shim socket)
2. Start P4d with `LD_PRELOAD`

If P4d starts before the daemon, the shim will fail to connect to the socket and fall back to normal behavior (ENOENT for missing files). Once the daemon starts, subsequent shim requests will succeed.

## Replica Server Setup

Replicas use read-only mode. No fanotify, no uploads, no `CAP_SYS_ADMIN` required.

```json
{
  "depot_path": "/mnt/nvme/depot",
  "read_only": true,
  "primary": {
    "type": "s3",
    "bucket": "my-p4-depot",
    "region": "us-east-1"
  },
  "max_cache_gb": 200,
  "low_watermark_gb": 160,
  "restore_threads": 16
}
```

```bash
p4-cache --config /etc/p4-cache/config.json --daemon \
  --pid-file /var/run/p4-cache.pid --log-file /var/log/p4-cache.log
```

## Dual Backend Configuration

### Migration Example

When migrating from one storage provider to another, use the old location as secondary:

```json
{
  "depot_path": "/mnt/nvme/depot",
  "primary": {
    "type": "s3",
    "bucket": "new-depot-bucket",
    "region": "us-west-2"
  },
  "secondary": {
    "type": "s3",
    "bucket": "old-depot-bucket",
    "region": "us-east-1"
  }
}
```

New uploads go to the primary. Restores try primary first, then fall back to secondary for files that haven't been re-uploaded yet.

### NFS Fallback Example

Use a local NFS mount as a low-latency fallback:

```json
{
  "depot_path": "/mnt/nvme/depot",
  "primary": {
    "type": "s3",
    "bucket": "my-depot",
    "region": "us-east-1"
  },
  "secondary": {
    "type": "nfs",
    "path": "/mnt/nfs/depot-archive"
  }
}
```

## Monitoring

### Log Output

The daemon logs stats at the configured interval (default 60s):

```
[stats] cache=245.3/500.0 GB | files: 1523400 total, 12 dirty, 3 uploading,
1200000 clean | uploads: 890234 ok, 12 fail | restores: 45021 ok,
3 fail (120 secondary) | evictions: 323385 | shim: 44901 fetch, 230 miss
```

Note: stats are maintained as in-memory atomic counters (no DB query needed). The "evicted" count is not shown because evicted files are deleted from the manifest entirely.

### Prometheus Metrics

p4-cache can export metrics to a `.prom` textfile for node_exporter's textfile collector. No HTTP server is required.

**Enable metrics** by adding `--metrics-file` to the CLI or `metrics_file` to the JSON config:

```bash
p4-cache --config /etc/p4-cache/config.json \
  --metrics-file /var/lib/node_exporter/textfile/p4cache.prom \
  --metrics-interval 15
```

Or in the JSON config:
```json
{
  "metrics_file": "/var/lib/node_exporter/textfile/p4cache.prom",
  "metrics_interval": 15
}
```

**Configure node_exporter:**
```bash
node_exporter --collector.textfile.directory=/var/lib/node_exporter/textfile/
```

**Verify output:**
```bash
cat /var/lib/node_exporter/textfile/p4cache.prom
# promtool check metrics < /var/lib/node_exporter/textfile/p4cache.prom
```

**Exported metrics:**

| Type | Metrics |
|------|---------|
| Counters | `p4cache_uploads_total{result}`, `p4cache_upload_bytes_total`, `p4cache_restores_total{result}`, `p4cache_restore_bytes_total`, `p4cache_restores_secondary_total`, `p4cache_evictions_total`, `p4cache_eviction_bytes_total`, `p4cache_shim_requests_total{result}`, `p4cache_watcher_events_total{type}` |
| Gauges | `p4cache_files_dirty`, `p4cache_files_uploading`, `p4cache_files_clean`, `p4cache_files_total`, `p4cache_cache_bytes`, `p4cache_cache_max_bytes`, `p4cache_upload_queue_pending`, `p4cache_restore_queue_pending` |
| Histograms | `p4cache_upload_duration_seconds`, `p4cache_restore_duration_seconds`, `p4cache_eviction_batch_duration_seconds`, `p4cache_shim_request_duration_seconds` |

All metrics carry constant labels: `depot` (depot path) and `mode` (`readwrite` or `readonly`).

The file is written atomically (temp + rename) so readers never see partial content.

### Key Metrics to Watch

| Metric | Meaning | Action if abnormal |
|--------|---------|-------------------|
| `dirty` count | Files waiting for upload | If consistently high, increase `upload_concurrency` or check network |
| `uploading` count | Files currently being uploaded | If stuck at a constant value, check backend connectivity |
| `uploads fail` | Failed upload count | Check storage credentials and connectivity |
| `restores fail` | Failed restore count | Check storage backend health |
| `secondary` | Restores served from secondary | High count during migration is normal; in steady state should be near 0 |
| `cache` GB | Current NVMe usage | If at max, eviction may cause restore churn |
| `shim miss` | Shim requests for files not in storage | High count suggests P4d is requesting non-existent files |

### Health Checks

Check if the daemon is running:
```bash
# Via PID file
kill -0 $(cat /var/run/p4-cache.pid) 2>/dev/null && echo "running" || echo "stopped"

# Via socket existence
test -S /mnt/nvme/depot/.p4cache/shim.sock && echo "socket exists" || echo "no socket"
```

Test shim connectivity:
```bash
echo "FETCH test/nonexistent" | socat - UNIX-CONNECT:/mnt/nvme/depot/.p4cache/shim.sock
# Expected: "NOTFOUND" (daemon is responsive)
```

### LMDB Manifest Inspection

The manifest is an LMDB environment at `<depot>/.p4cache/manifest/`. Use the `mdb_stat` and `mdb_dump` tools from `lmdb-utils`:

```bash
# Database statistics (entry counts, page usage, depth)
mdb_stat -a /mnt/nvme/depot/.p4cache/manifest/

# Dump all keys in the files database (hex format)
mdb_dump -s files /mnt/nvme/depot/.p4cache/manifest/

# Environment info (map size, page size, max readers)
mdb_stat -e /mnt/nvme/depot/.p4cache/manifest/
```

## Scaling

### Billion-File Depots

p4-cache is designed for depots approaching 1 PB with billions of files. Key design decisions for this scale:

- **LMDB manifest**: Memory-mapped B+ tree handles billions of keys natively. No WAL checkpointing or compaction overhead.
- **Delete on eviction**: Evicted files are unlinked (not truncated to 0-byte stubs), avoiding inode exhaustion on ext4 and preventing unbounded manifest growth.
- **3 file states only**: dirty, uploading, clean. No "evicted" state means the manifest only contains files currently on NVMe (bounded by cache size, not depot size).
- **In-memory stats**: Atomic counters updated on state transitions. No `GROUP BY` queries over billions of rows.
- **Optional startup scan**: Use `--skip-startup-scan` for 30-60 TB caches where scanning billions of files takes hours. Files written while the daemon was down will be caught by fanotify when next modified, or restored from storage by the shim when next read.

### Sizing Guidelines

The NVMe cache should be large enough to hold your working set — the files actively being read and written during normal operations.

| Scenario | Recommended cache size |
|----------|----------------------|
| Small depot (< 100 GB) | 1.5x total depot size |
| Medium depot, active development | 30-50% of total depot size |
| Large depot (PB-scale), CI workloads | 30-60 TB NVMe, `--skip-startup-scan` |

### Sizing the Watermarks

- **`max_cache_gb`**: absolute maximum; the daemon will not let the cache exceed this
- **`low_watermark_gb`**: eviction starts when cache exceeds this; set to ~80% of max
- **Eviction target**: automatically set to 90% of low watermark; eviction stops here

Example for a 1 TB NVMe with 500 GB allocated to cache:
```
--max-cache-gb 500 --low-watermark-gb 400
```
Eviction triggers at 400 GB, runs until 360 GB.

## Troubleshooting

### Daemon won't start: "fanotify_init failed (requires CAP_SYS_ADMIN)"

The daemon needs `CAP_SYS_ADMIN` for fanotify. Fix:
```bash
sudo setcap cap_sys_admin+ep /usr/local/bin/p4-cache
```
Or add `AmbientCapabilities=CAP_SYS_ADMIN` to the systemd service.

In read-only mode, fanotify is not used and this error won't occur.

### Uploads stuck / "dirty" count growing

1. Check storage connectivity: can you manually upload to the bucket?
2. Check credentials: are `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` set?
3. Check logs for "Upload failed" errors
4. Increase `upload_concurrency` if uploads succeed but are slow

### Restores failing / P4d getting ENOENT

1. Check if the daemon is running and the shim socket exists
2. Check P4d was started with `LD_PRELOAD` and `P4CACHE_DEPOT`
3. Check logs for "Restore failed" errors
4. Verify the file exists in the storage backend manually

### High eviction churn

If the stats show files being evicted and immediately restored:
- The cache is too small for the workload
- Increase `--max-cache-gb` and `--low-watermark-gb`
- Or identify which files are being accessed repeatedly and ensure they stay warm

## Backup and Recovery

### Backing Up the Manifest

The LMDB manifest can be copied while the daemon is running. LMDB uses copy-on-write, so readers see a consistent snapshot. To make a safe backup:

```bash
# Use mdb_copy for a consistent snapshot
mdb_copy /mnt/nvme/depot/.p4cache/manifest/ /backup/manifest/

# Or with compaction (smaller output)
mdb_copy -c /mnt/nvme/depot/.p4cache/manifest/ /backup/manifest/
```

### Resetting the Cache

To start fresh, stop the daemon and delete the state directory:

```bash
sudo systemctl stop p4-cache
rm -rf /mnt/nvme/depot/.p4cache
sudo systemctl start p4-cache
```

On restart, the daemon will:
1. Create a new empty manifest
2. Scan the depot for untracked files and register them as dirty (unless `--skip-startup-scan`)
3. Begin uploading them to the primary backend

### Crash Recovery

On startup, the daemon automatically resets any entries in `uploading` state back to `dirty`. This ensures no files are lost if the daemon crashes mid-upload. LMDB is crash-safe by default (no WAL replay needed).

### Failover to a Standby Server

If the primary server has a hardware problem (e.g., memory failure) but the NVMe drive is still readable, you can copy the cache to a standby server to avoid re-fetching the entire depot from backend storage.

**Procedure:**

1. Stop p4-cache and P4d on the failing server (if still running):
   ```bash
   sudo systemctl stop p4d
   sudo systemctl stop p4-cache
   ```

2. Copy the depot directory (including `.p4cache/`) to the standby:
   ```bash
   rsync -aHAX /mnt/nvme/depot/ standby:/mnt/nvme/depot/
   ```

3. Start p4-cache and P4d on the standby:
   ```bash
   sudo systemctl start p4-cache
   sudo systemctl start p4d
   ```

On startup, p4-cache runs crash recovery (resets `uploading` entries to `dirty`) and resumes normal operation. Clean files are recognized as already uploaded. Evicted files are already gone from disk and manifest — they'll be fetched from storage on demand.

**Important: copy the LMDB lock file.** The manifest directory contains `data.mdb` and `lock.mdb`. Both must be copied together. If the daemon was not stopped cleanly, LMDB's crash recovery will handle any incomplete transactions on startup.

**If you skip the manifest entirely** and only copy the depot files, p4-cache will start with an empty manifest. `scan_untracked_files()` will mark every file on disk as dirty and re-upload it to the backend. For a large cache this means a full re-upload, but nothing breaks. Use `--skip-startup-scan` to defer this if the scan would take too long.

### Data Durability and the Dirty Window

p4-cache is a **write-back cache**: P4d writes are acknowledged at NVMe speed and uploaded to the backend asynchronously. Files in `dirty` or `uploading` state exist only on the NVMe drive and have not yet reached durable storage.

**If the NVMe drive is lost**, dirty files are gone. The backend only has files that completed upload (reached `clean` state). This affects:

- **Replicas** running in read-only mode against the same backend will get `NOTFOUND` for any file that was dirty on the primary when it died. The replica cannot serve those files.
- **Perforce replication** (`p4 pull -u`) is a separate channel that transfers file content directly between P4d instances over the Perforce protocol, not through the storage backend. If a replica already pulled the file content via `p4 pull` before the primary failed, it has its own copy and is unaffected.

**To minimize the dirty window:**

- Tune `upload_poll_interval` (default 100ms) and `upload_batch_size` (default 64) for faster drain
- Increase `upload_concurrency` for more parallel uploads
- Monitor the `dirty` count in stats output — a consistently high count means uploads aren't keeping up with writes
