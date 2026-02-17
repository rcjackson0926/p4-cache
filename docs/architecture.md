# Architecture

This document describes the internal design of `p4-cache`.

## Overview

`p4-cache` is a userspace caching daemon that makes Perforce depot access transparent across NVMe and remote object storage. It consists of two binaries:

- **`p4-cache`** — the daemon process that manages the cache lifecycle
- **`libp4shim.so`** — an LD_PRELOAD library injected into P4d for cold-file interception

The daemon and shim communicate over a Unix domain socket (`<depot>/.p4cache/shim.sock`).

## Components

### 1. CacheConfig (`cache_config.hpp` / `cache_config.cpp`)

Parses configuration from CLI flags, JSON files, and environment variables. Produces a validated `CacheConfig` struct consumed by all other components.

Key types:
- **`BackendConfig`** — type string (`"s3"`, `"azure"`, `"gcs"`, `"nfs"`) plus a `map<string,string>` of parameters. Validated per-type (e.g., S3 requires `bucket`, NFS requires `path`).
- **`CacheConfig`** — top-level config with `primary` and optional `secondary` `BackendConfig`, plus depot path, cache limits, thread counts, and daemon settings.

### 2. DepotCache (`depot_cache.hpp` / `depot_cache.cpp`)

The core engine. Owns:
- SQLite manifest database
- Primary and secondary `StorageBackend` instances
- Upload worker thread + upload thread pool
- Eviction worker thread
- Restore thread pool
- Shim Unix socket server thread
- Stats reporter thread

### 3. DepotWatcher (`depot_watcher.hpp` / `depot_watcher.cpp`)

Uses Linux `fanotify` to monitor the depot mount for `FAN_CLOSE_WRITE` events. When a file is written, it calls `DepotCache::on_file_written()` to mark it dirty.

In read-only mode, fanotify is not initialized — the watcher thread exists only for clean shutdown signaling via a self-pipe.

### 4. Shim (`shim.cpp`)

An `LD_PRELOAD` library that hooks `open()` and `openat()` in P4d's process space. It handles two scenarios:

1. **ENOENT interception** — when `open()` returns `ENOENT` for a path under the depot, the shim asks the daemon to fetch the file from remote storage, then retries.
2. **Evicted stub detection** — when `open()` succeeds but returns a 0-byte regular file, the shim checks via `fstat()` and asks the daemon to restore it, then re-opens.

The shim maintains a per-thread negative cache (up to 10,000 entries) to avoid repeated lookups for paths not in storage.

### 5. main.cpp

Entry point. Handles daemonization (double-fork), log redirection, signal handling (SIGINT/SIGTERM), and orchestrates startup/shutdown of `DepotCache` and `DepotWatcher`.

## Data Flow

### Write Path (read-write mode)

```
P4d writes file
       |
       v
fanotify FAN_CLOSE_WRITE event
       |
       v
DepotWatcher::event_loop() ──> DepotCache::on_file_written()
       |                               |
       |                     1. Get file size
       |                     2. INSERT OR REPLACE into manifest (state='dirty')
       |                     3. Update cache_bytes_ counter
       |                     4. Notify upload_cv_
       |                     5. Check eviction threshold
       v
upload_worker_loop()
       |
  1. SELECT dirty batch from manifest
  2. UPDATE state='uploading'
  3. Submit to upload_pool_ (concurrent PUTs)
  4. primary_->put(key, data)
  5. UPDATE state='clean' or state='dirty' (on failure)
```

### Read Path — Warm (file on NVMe)

```
P4d opens file ──> open() succeeds ──> fstat() shows size > 0 ──> normal read
```

No daemon involvement. Full NVMe speed.

### Read Path — Evicted Stub

```
P4d opens file ──> open() succeeds ──> fstat() shows size == 0
       |
       v
libp4shim.so: check_and_restore_stub()
       |
       v
Unix socket: "FETCH <relative-path>\n"
       |
       v
DepotCache::fetch_for_shim()
  1. Check if file already exists (race check)
  2. Look up storage_key in manifest
  3. primary_->get(key) — if fails and secondary exists:
  4. secondary_->get(key)
  5. Write file to NVMe
  6. INSERT into manifest (state='clean')
  7. Return "OK <size>"
       |
       v
libp4shim.so: close(fd), re-open() ──> normal read
```

### Read Path — Cold (not on NVMe, not in manifest)

```
P4d opens file ──> open() returns ENOENT
       |
       v
libp4shim.so: should_intercept() ──> check negative cache
       |
       v
Unix socket: "FETCH <relative-path>\n"
       |
       v
DepotCache::fetch_for_shim()
  (same as evicted stub flow above)
       |
  success: shim retries open()
  failure: shim adds to negative cache, returns ENOENT
```

### Eviction Path

```
eviction_worker_loop()
  triggered when cache_bytes_ > eviction_low_watermark
       |
  1. SELECT clean files ORDER BY last_access ASC LIMIT 100
  2. truncate(file, 0)  — creates 0-byte stub
  3. UPDATE state='evicted', size=0
  4. Decrement cache_bytes_
       |
  continues until cache_bytes_ <= eviction_target (90% of low watermark)
```

## Threading Model

| Thread | Mode | Purpose |
|--------|------|---------|
| Main thread | both | Signal handling loop (`sleep_for(200ms)`) |
| Upload coordinator | read-write only | Polls dirty batch every 100ms, dispatches to upload pool |
| Upload pool (N threads) | read-write only | Concurrent PUT operations (`upload_concurrency`, default 16) |
| Eviction worker | both | Wakes on threshold or every 10s, truncates clean files |
| Shim server | both | Accepts Unix socket connections, dispatches to restore pool |
| Restore pool (N threads) | both | Concurrent GET + file write operations (`restore_threads`, default 16) |
| Stats reporter | both | Logs stats every `stats_interval_secs` (default 60) |
| Watcher event loop | read-write only | Reads fanotify events, calls `on_file_written()` |

### Concurrency Design

- **SQLite access** is serialized by `db_mutex_`. WAL mode allows concurrent readers, but all prepared statement usage is mutex-protected.
- **In-flight fetch deduplication**: `pending_fetches_` map (protected by `pending_mutex_`) ensures multiple threads requesting the same file share a single `shared_future<bool>`.
- **Cache size tracking**: `cache_bytes_` is an `atomic<uint64_t>`, updated lock-free.
- **Stats**: protected by `stats_mutex_` (separate from `db_mutex_` to avoid lock contention).

## SQLite Manifest Schema

The manifest lives at `<depot>/.p4cache/manifest.db` and uses WAL journaling.

```sql
CREATE TABLE cache_entries (
    path TEXT PRIMARY KEY,         -- relative path within depot
    size INTEGER NOT NULL,         -- file size in bytes (0 when evicted)
    storage_key TEXT NOT NULL,     -- key used for storage backend operations
    state TEXT NOT NULL DEFAULT 'dirty',  -- dirty|uploading|clean|evicted
    last_access INTEGER NOT NULL,  -- epoch seconds, for LRU eviction
    created_at INTEGER NOT NULL,   -- epoch seconds, for upload ordering
    etag TEXT                      -- reserved for future content verification
) WITHOUT ROWID;

-- Eviction: find oldest clean files
CREATE INDEX idx_evict ON cache_entries(state, last_access) WHERE state = 'clean';

-- Upload: find oldest dirty files
CREATE INDEX idx_drain ON cache_entries(state, created_at) WHERE state = 'dirty';
```

### File State Machine

```
                 on_file_written()
    ┌────────────────────────────────────┐
    │                                    │
    v                                    │
  dirty ──upload──> uploading ──success──> clean ──evict──> evicted
    ^                  │                                      │
    │                  │ failure                               │
    └──────────────────┘                                      │
    ^                                                         │
    │                        restore_file() / fetch_for_shim()│
    └─────────────────────────────────────────────────────────┘
                        (restored as 'clean')
```

## Storage Backend Integration

The daemon uses meridian's `StorageBackendFactory::create(type, params)` to instantiate backends. The `BackendConfig.type` maps directly to the factory type, except `"nfs"` which maps to `"local"`.

All backend interaction uses the abstract `StorageBackend` interface:
- `put(key, data, options)` — upload to primary
- `get(key, options)` — restore from primary, fallback to secondary

The `storage_key` is the file's relative path within the depot. The backend's `path_prefix` parameter (defaulting to the depot directory basename) provides namespacing within the bucket/container.

## Design Decisions

### Why Not FAN_OPEN_PERM?

`FAN_OPEN_PERM` with `FAN_MARK_MOUNT` intercepts ALL `open()` calls on the mount — not just depot files. When the daemon is busy restoring a file, every other process trying to open any file on that mount blocks. This stalls the entire filesystem.

Instead, evicted file detection is handled by the LD_PRELOAD shim, which:
1. Only runs inside P4d's process
2. Checks `fstat()` after a successful `open()` (non-blocking)
3. Only contacts the daemon when it finds a 0-byte stub

### Why SQLite WAL?

- Crash-safe: interrupted uploads are recovered on restart (uploading -> dirty)
- Concurrent readers: the stats reporter and shim server can read while the upload coordinator writes
- Single-file state: easy to back up, inspect (`sqlite3 manifest.db`), or delete to reset

### Why a Separate Shim Library?

Running the interception logic in-process with P4d via `LD_PRELOAD` avoids the kernel overhead of `FAN_OPEN_PERM`. The shim is a thin layer (~300 lines) with no dependencies beyond libc and pthreads. It communicates with the daemon over a Unix socket, keeping the complex storage logic out of P4d's address space.
