# Deployment Guide

This guide covers production deployment of `p4-cache` including system setup, service configuration, monitoring, replica servers, and troubleshooting.

## Prerequisites

- Linux kernel 2.6.37+ (fanotify support)
- NVMe storage mounted at the depot path
- Remote storage accessible (S3/Azure/GCS credentials or NFS mount)
- `p4-cache` and `libp4shim.so` built and installed

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
  "stats_interval": 60
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
1200000 clean, 323385 evicted | uploads: 890234 ok, 12 fail | restores: 45021 ok,
3 fail (120 secondary) | evictions: 323385 | shim: 44901 fetch, 230 miss
```

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

### SQLite Manifest Inspection

```bash
# File counts by state
sqlite3 /mnt/nvme/depot/.p4cache/manifest.db \
  "SELECT state, COUNT(*), SUM(size) FROM cache_entries GROUP BY state;"

# Oldest dirty files (upload backlog)
sqlite3 /mnt/nvme/depot/.p4cache/manifest.db \
  "SELECT path, datetime(created_at, 'unixepoch') FROM cache_entries
   WHERE state='dirty' ORDER BY created_at LIMIT 10;"

# Largest clean files (eviction candidates)
sqlite3 /mnt/nvme/depot/.p4cache/manifest.db \
  "SELECT path, size/1048576 AS mb FROM cache_entries
   WHERE state='clean' ORDER BY size DESC LIMIT 10;"
```

## Cache Sizing Guidelines

### Sizing the Cache

The NVMe cache should be large enough to hold your working set — the files actively being read and written during normal operations. Typical sizing:

| Scenario | Recommended cache size |
|----------|----------------------|
| Small depot (< 100 GB) | 1.5x total depot size |
| Medium depot, active development | 30-50% of total depot size |
| Large depot, CI workloads | 10-20% of total depot size, focused on recent changelists |

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

### SQLite "database is locked" errors

The daemon uses WAL mode and retries SQLITE_BUSY up to 10 times with backoff. If you see lock errors:
- Don't run external tools that lock the manifest while the daemon is running
- If inspecting the manifest, use read-only access: `sqlite3 -readonly manifest.db`

## Backup and Recovery

### Backing Up the Manifest

The manifest can be backed up while the daemon is running (WAL mode supports concurrent reads):

```bash
sqlite3 /mnt/nvme/depot/.p4cache/manifest.db ".backup /backup/manifest.db"
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
2. Scan the depot for untracked files and register them as dirty
3. Begin uploading them to the primary backend

### Crash Recovery

On startup, the daemon automatically resets any entries in `uploading` state back to `dirty`. This ensures no files are lost if the daemon crashes mid-upload.

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

On startup, p4-cache runs crash recovery (resets `uploading` entries to `dirty`) and resumes normal operation. Clean files are recognized as already uploaded. Evicted 0-byte stubs restore on demand from the backend.

**Important: copy the WAL file.** If p4-cache wasn't stopped cleanly before copying, the SQLite WAL file (`manifest.db-wal`) contains recent transactions that haven't been folded into the main database. You must copy `manifest.db`, `manifest.db-wal`, and `manifest.db-shm` together. If only `manifest.db` is copied, recent state changes are lost — those files will be re-scanned and re-uploaded, which is wasteful but not destructive.

**If you can't stop p4-cache before copying**, don't rsync the manifest while the daemon is writing to it. Instead, take a consistent snapshot:
```bash
sqlite3 /mnt/nvme/depot/.p4cache/manifest.db ".backup /tmp/manifest-snapshot.db"
```
Then rsync the depot files separately and place the snapshot at `<depot>/.p4cache/manifest.db` on the standby.

**If you skip the manifest entirely** and only copy the depot files, p4-cache will start with an empty manifest. `scan_untracked_files()` will mark every file on disk as dirty and re-upload it to the backend. For a large cache this means a full re-upload, but nothing breaks.

### Data Durability and the Dirty Window

p4-cache is a **write-back cache**: P4d writes are acknowledged at NVMe speed and uploaded to the backend asynchronously. Files in `dirty` or `uploading` state exist only on the NVMe drive and have not yet reached durable storage.

**If the NVMe drive is lost**, dirty files are gone. The backend only has files that completed upload (reached `clean` state). This affects:

- **Replicas** running in read-only mode against the same backend will get `NOTFOUND` for any file that was dirty on the primary when it died. The replica cannot serve those files.
- **Perforce replication** (`p4 pull -u`) is a separate channel that transfers file content directly between P4d instances over the Perforce protocol, not through the storage backend. If a replica already pulled the file content via `p4 pull` before the primary failed, it has its own copy and is unaffected.

**To minimize the dirty window:**

- Tune `upload_poll_interval` (default 100ms) and `upload_batch_size` (default 64) for faster drain
- Increase `upload_concurrency` for more parallel uploads
- Monitor the `dirty` count in stats output — a consistently high count means uploads aren't keeping up with writes
