# p4-cache

A transparent NVMe caching daemon for Perforce depot servers. `p4-cache` sits between Helix Core Server (P4d) and remote storage, letting P4d read and write at full NVMe speed while the daemon handles background uploads, LRU eviction, and on-demand restores.

## How It Works

```
P4d  ──reads/writes──>  NVMe depot  <──watches──  p4-cache daemon  ──uploads──>  Remote Storage
                             ^                          |                        (S3/Azure/GCS/NFS)
                             |                          |
                        libp4shim.so               LMDB manifest
                      (restores cold files)        (.p4cache/manifest.lmdb/)
```

1. **P4d writes a file** to the NVMe depot at full disk speed.
2. **`p4-cache` detects the write** via Linux fanotify (`FAN_CLOSE_WRITE`), marks it dirty in the manifest, and queues it for upload.
3. **Background upload workers** push the file to the primary storage backend.
4. **When the NVMe cache fills up**, the eviction thread deletes the oldest clean files.
5. **When P4d reads an evicted file**, the `libp4shim.so` LD_PRELOAD shim intercepts the ENOENT, asks the daemon to restore it from remote storage, then retries the open.

## Supported Backends

| Backend | Type flag | Required params |
|---------|-----------|-----------------|
| Amazon S3 / MinIO | `s3` | `bucket` |
| Azure Blob Storage | `azure` | `container` |
| Google Cloud Storage | `gcs` | `bucket` |
| NFS / local filesystem | `nfs` | `path` |

Each deployment can have a **primary** backend (read-write) and an optional **secondary** backend (read-only fallback for restores).

## Quick Start

### Build

```bash
cd build && cmake .. && make -j8
```

This produces:
- `build/p4-cache` — the cache daemon
- `build/libp4shim.so` — the LD_PRELOAD shim library

### Run

```bash
# Grant fanotify capability (one-time)
sudo setcap cap_sys_admin+ep build/p4-cache

# Start the daemon
./build/p4-cache \
  --depot-path /mnt/nvme/depot \
  --primary-type s3 \
  --primary-endpoint https://s3.amazonaws.com \
  --primary-bucket my-p4-depot \
  --max-cache-gb 500

# Start P4d with the shim
LD_PRELOAD=./build/libp4shim.so P4CACHE_DEPOT=/mnt/nvme/depot \
  p4d -r /mnt/nvme/depot -p 1666
```

### Run as a Daemon

```bash
p4-cache \
  --depot-path /mnt/nvme/depot \
  --primary-type s3 \
  --primary-bucket my-p4-depot \
  --max-cache-gb 500 \
  --daemon \
  --pid-file /var/run/p4-cache.pid \
  --log-file /var/log/p4-cache.log
```

## Documentation

| Document | Description |
|----------|-------------|
| [Operations Guide](docs/p4-cache-guide.md) | Quick reference for CLI flags, JSON config, and backend examples |
| [Configuration Reference](docs/configuration.md) | Complete reference for all CLI flags, JSON fields, environment variables, and defaults |
| [Architecture](docs/architecture.md) | Internal design, component interactions, data flow, threading model, and LMDB schema |
| [Deployment Guide](docs/deployment.md) | Production deployment with systemd, permissions, monitoring, replica setup, and troubleshooting |

## Requirements

- Linux (fanotify requires `CAP_SYS_ADMIN`)
- C++20 compiler (GCC 11+ or Clang 14+)
- CMake 3.20+
- LMDB, OpenSSL, zlib, libcurl, nlohmann/json
