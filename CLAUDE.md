# p4-cache

## Build

```bash
cd build && cmake .. && make -j8
```

Requires the meridian tree at `../meridian` (for `StorageBackend` and `ThreadPool` headers + `libmeridian_core.a`). Build meridian first: `cd ../meridian/build && cmake .. && make -j8`.

## Executables

| Binary | Purpose |
|--------|---------|
| `p4-cache` | NVMe + multi-backend cache daemon for Perforce depots (links meridian_core) |
| `libp4shim.so` | LD_PRELOAD shim for P4d cold/evicted file interception |

## P4 Cache (NVMe Depot Acceleration)

The `p4-cache` daemon provides a transparent NVMe caching layer between P4d and remote storage (S3, Azure Blob, GCS, or NFS). P4d reads and writes directly to a local NVMe mount at full disk speed. The daemon uploads new files to the primary backend in the background, evicts least-recently-accessed files to 0-byte stubs when the cache fills, and restores evicted/cold files from storage on demand via an LD_PRELOAD shim. An optional secondary backend provides read-only fallback for restores.

**Binaries:** `p4-cache` (daemon), `libp4shim.so` (LD_PRELOAD library for P4d)

```bash
# Primary server (read-write, S3 backend)
p4-cache \
  --depot-path /mnt/nvme/depot \
  --primary-type s3 \
  --primary-endpoint https://s3.amazonaws.com \
  --primary-bucket my-p4-depot \
  --max-cache-gb 500 \
  --daemon --pid-file /var/run/p4-cache.pid --log-file /var/log/p4-cache.log

# Start P4d with shim for cold/evicted file support
LD_PRELOAD=/usr/local/lib/libp4shim.so P4CACHE_DEPOT=/mnt/nvme/depot \
  p4d -r /mnt/nvme/depot -p 1666 -d

# Replica server (read-only, no uploads)
p4-cache --depot-path /mnt/nvme/depot --primary-type s3 --primary-bucket ... --read-only --daemon
```

**Key design decisions:**
- Does NOT use `FAN_OPEN_PERM` — this blocks ALL opens on the entire mount, stalling the filesystem. Read interception is handled entirely by the LD_PRELOAD shim via `fstat()` after open.
- Read-only mode skips fanotify entirely (no `FAN_CLOSE_WRITE` needed).
- SQLite WAL manifest tracks file states (dirty -> uploading -> clean -> evicted).
- Requires `CAP_SYS_ADMIN` for fanotify: `sudo setcap cap_sys_admin+ep p4-cache`

**Source files:**
- Config: `include/meridian/proxy/cache_config.hpp`, `src/cache_config.cpp`
- Engine: `include/meridian/proxy/depot_cache.hpp`, `src/depot_cache.cpp`
- Watcher: `include/meridian/proxy/depot_watcher.hpp`, `src/depot_watcher.cpp`
- Shim: `src/shim.cpp`
- Daemon: `src/main.cpp`

**Documentation:**
- `docs/p4-cache-guide.md` — operations quick-reference
- `docs/configuration.md` — complete configuration reference
- `docs/architecture.md` — internal design and data flow
- `docs/deployment.md` — production deployment and troubleshooting
