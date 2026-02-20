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
- LMDB manifest database (3 named databases)
- LMDB access log database (separate environment, see below)
- Primary and secondary `StorageBackend` instances
- Upload worker thread + upload thread pool
- Eviction worker thread
- Restore thread pool
- Shim Unix socket server thread
- Stats reporter thread
- Access log receiver and writer threads

### 3. DepotWatcher (`depot_watcher.hpp` / `depot_watcher.cpp`)

Uses Linux `fanotify` to monitor the depot mount for `FAN_CLOSE_WRITE` events. When a file is written, it calls `DepotCache::on_file_written()` to mark it dirty.

In read-only mode, fanotify is not initialized — the watcher thread exists only for clean shutdown signaling via a self-pipe.

### 4. Shim (`shim.cpp`)

An `LD_PRELOAD` library that hooks `open()` and `openat()` in P4d's process space. It handles two scenarios:

- **ENOENT interception** — when `open()` returns `ENOENT` for a path under the depot, the shim asks the daemon to fetch the file from remote storage, then retries.
- **Access recording** — when `open()` succeeds for a depot file, the shim records the path in a per-thread ring buffer. When the buffer fills (~32 KB / ~400 paths), it sends a datagram to the daemon's access log socket (fire-and-forget, ~7ns per record on the hot path).

Since evicted files are deleted (not truncated to 0-byte stubs), the hot path for `open()` is just the raw syscall plus access recording — no `fstat()` needed.

The shim maintains a per-thread negative cache (up to 10,000 entries) to avoid repeated lookups for paths not in storage.

### 5. MetricsExporter (`metrics.hpp` / `metrics.cpp`)

Optional Prometheus metrics exporter. When `--metrics-file` is set, creates a `prometheus::Registry` with counters, gauges, and histograms for all cache operations. A background writer thread periodically serializes the registry to a `.prom` textfile using atomic temp+rename, suitable for node_exporter's textfile collector.

Key classes:
- **`MetricsExporter`** — owns the registry, all metric families, and the writer thread. Receives pointers to `DepotCache` and `DepotWatcher` for periodic gauge snapshots.
- **`ScopedTimer`** — RAII class that observes a histogram with elapsed duration on destruction. Used to instrument upload, restore, eviction, and shim request durations.

### 6. Access Tool (`access_tool.cpp`)

Standalone `p4-cache-access` binary for querying the access log database. Opens the LMDB access log read-only. Only links against LMDB (no meridian, no curl, no prometheus).

Subcommands: `stat`, `count`, `get <path>`, `prefix <prefix>`, `stale --before <time>`, `export`.

### 7. main.cpp

Entry point. Handles daemonization (double-fork), log redirection, signal handling (SIGINT/SIGTERM), and orchestrates startup/shutdown of `DepotCache`, `DepotWatcher`, and `MetricsExporter`.

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
       |                     2. Put into LMDB files + dirty_queue (state=dirty)
       |                     3. Update cache_bytes_ counter
       |                     4. Notify upload_cv_
       |                     5. Check eviction threshold
       v
upload_worker_loop()
       |
  1. Cursor iterate dirty_queue (oldest-first), collect batch
  2. Update files entries: state=uploading, delete from dirty_queue
  3. Submit to upload_pool_ (concurrent PUTs)
  4. primary_->put(key, data)
  5. Update files: state=clean + add to evict_order (success)
     or state=dirty + add to dirty_queue (failure)
```

### Read Path — Warm (file on NVMe)

```
P4d opens file ──> open() succeeds ──> normal read
                         |
                         v
              libp4shim.so: record_access()
                 memcpy path into thread-local buffer (~7ns)
                         |
                         v  (when buffer fills ~32KB)
              flush_access_buffer(): sendto() datagram to daemon
                         |
                         v
              DepotCache::access_receiver_loop()
                 parse paths, accumulate in batch map
                         |
                         v  (every 5s or 10K entries)
              DepotCache::access_writer_loop()
                 sort batch, mdb_put() in single txn
                         |
                         v
              access LMDB: key=depot-relative path, value=8B epoch timestamp
```

Access recording is fire-and-forget and never blocks the read path. No fstat() syscall overhead.

### Read Path — Evicted / Cold (file not on NVMe)

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
  1. Check if file already exists (race check)
  2. Look up storage_key in manifest
  3. primary_->get(key) — if fails and secondary exists:
  4. secondary_->get(key)
  5. Write file to NVMe
  6. Put into LMDB files (state=clean) + evict_order
  7. Return "OK <size>"
       |
       v
libp4shim.so: retry open() ──> normal read
```

### Eviction Path

```
eviction_worker_loop()
  triggered when cache_bytes_ > eviction_low_watermark
       |
  Phase 0: Cursor iterate evict_order (oldest-first), collect batch of 100
  Phase 1: unlink(file) + rmdir empty parent dirs
  Phase 2: Delete from LMDB files + evict_order (in single write txn)
           Decrement cache_bytes_ and count_clean_
       |
  continues until cache_bytes_ <= eviction_target (90% of low watermark)
```

Eviction deletes files entirely — no 0-byte stubs, no "evicted" state. This avoids unbounded inode consumption and manifest growth.

## Threading Model

| Thread | Mode | Purpose |
|--------|------|---------|
| Main thread | both | Signal handling loop (`sleep_for(200ms)`) |
| Upload coordinator | read-write only | Polls dirty batch every 100ms, dispatches to upload pool |
| Upload pool (N threads) | read-write only | Concurrent PUT operations (`upload_concurrency`, default 16) |
| Eviction worker | both | Wakes on threshold or every 10s, unlinks clean files |
| Shim server | both | Accepts Unix socket connections, dispatches to restore pool |
| Restore pool (N threads) | both | Concurrent GET + file write operations (`restore_threads`, default 16) |
| Stats reporter | both | Logs stats every `stats_interval_secs` (default 60) |
| Metrics writer | both (if configured) | Writes `.prom` file every `metrics_interval_secs` (default 15) |
| Access receiver | both (if enabled) | Reads datagram socket, parses paths, accumulates batch |
| Access writer | both (if enabled) | Periodic sorted LMDB batch flush (every 5s or 10K entries) |
| Watcher event loop | read-write only | Reads fanotify events, calls `on_file_written()` |

### Concurrency Design

- **LMDB write transactions** are serialized by `db_mutex_`. LMDB natively supports concurrent readers, so read-only transactions do not need the mutex.
- **In-flight fetch deduplication**: `pending_fetches_` map (protected by `pending_mutex_`) ensures multiple threads requesting the same file share a single `shared_future<bool>`.
- **Cache size tracking**: `cache_bytes_` is an `atomic<uint64_t>`, updated lock-free.
- **Stats counters**: `count_dirty_`, `count_uploading_`, `count_clean_` are `atomic<uint64_t>`, updated on every state transition. No DB query needed for stats.
- **Stats (operational)**: protected by `stats_mutex_` (separate from `db_mutex_` to avoid lock contention).

## LMDB Manifest

The manifest lives at `<depot>/.p4cache/manifest/` (a directory containing `data.mdb` and `lock.mdb`).

### Databases

| Database | Key | Value | Purpose |
|----------|-----|-------|---------|
| `files` | path (string) | FileEntry (packed) | Main record for every cached file |
| `dirty_queue` | created_at (8B BE) + path | (empty) | Upload batching: oldest-first iteration |
| `evict_order` | last_access (8B BE) + path | (empty) | LRU eviction: oldest-first iteration |

### FileEntry Value Format

```
[8B size][8B last_access][8B created_at][1B state][storage_key bytes...]
```

State enum: 0=dirty, 1=uploading, 2=clean. No "evicted" state.

### File State Machine

```
                 on_file_written()
    ┌────────────────────────────────────┐
    │                                    │
    v                                    │
  dirty ──upload──> uploading ──success──> clean ──evict──> [deleted]
    ^                  │
    │                  │ failure
    └──────────────────┘
                                   fetch_for_shim()
    [not on disk] ─────────────────────────────> clean
```

### State Transitions (all in single LMDB write transactions)

| Transition | files DB | dirty_queue | evict_order |
|-----------|----------|-------------|-------------|
| new file → dirty | put | put | — |
| dirty → uploading | update | del | — |
| uploading → clean | update | — | put |
| uploading → dirty (failure) | update | put | — |
| clean → deleted (eviction) | del | — | del |
| fetch_for_shim → clean | put | — | put |

## LMDB Access Log

The access log is a separate LMDB environment at `<depot>/.p4cache/access/`, independent from the manifest. It permanently records when each depot file was last read through the cache.

### Why a Separate Environment?

- **Independent write lock**: High-frequency access writes don't contend with upload/eviction transactions on the manifest
- **Relaxed durability**: Uses `MDB_WRITEMAP | MDB_MAPASYNC | MDB_NOSYNC` for maximum write throughput. Losing the last 60s of access data on crash is acceptable.
- **Different scale**: Access DB may grow to 250 GB+ for billion-file depots while manifest stays bounded by cache size

### Schema

Single unnamed database (no `mdb_env_set_maxdbs` call needed):

| Key | Value |
|-----|-------|
| depot-relative path (string) | 8-byte little-endian epoch timestamp |

### Write Optimization

1. **Pre-sorted batch inserts**: Paths are sorted lexicographically before `mdb_put()`, giving sequential B-tree leaf access with ~85-90% page fill factor
2. **In-memory deduplication**: `unordered_map<string, uint64_t>` collapses repeated opens of the same file into one write (keeps latest timestamp)
3. **Periodic sync**: `mdb_env_sync()` every 60s bounds worst-case data loss
4. **Fire-and-forget IPC**: Shim sends datagrams with `MSG_DONTWAIT`; if the socket buffer is full, events are silently dropped

### Crash Safety

| Failure | Impact |
|---------|--------|
| Daemon crash | Lose up to 60s of access events (last sync interval) + in-memory batch |
| OS crash / power loss | Same; LMDB rolls back to last `mdb_env_sync()` point |
| Socket buffer full | Individual datagrams silently dropped |
| Disk full | `mdb_txn_commit()` returns `MDB_MAP_FULL`; daemon logs error, drops batch, continues |

Access log failure never affects cache correctness (uploads, evictions, restores).

### Query Tool

The `p4-cache-access` binary opens the access LMDB read-only for analysis:

```bash
p4-cache-access --db .p4cache/access stat       # Entry count, tree depth, DB size
p4-cache-access --db .p4cache/access get <path>  # Last access time for one file
p4-cache-access --db .p4cache/access prefix dir/ # Files under a directory
p4-cache-access --db .p4cache/access stale --before 30d  # Files not accessed in 30 days
p4-cache-access --db .p4cache/access export --format csv  # Full dump
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

Instead, evicted file interception is handled by the LD_PRELOAD shim, which only runs inside P4d's process and uses the ENOENT path to trigger fetches.

### Why Delete on Eviction (Not 0-byte Stubs)?

At billion-file scale, 0-byte stubs exhaust ext4's inode table (default ~64M inodes per TB). Deleting evicted files avoids this limit entirely. It also eliminates the "evicted" state from the manifest, preventing unbounded row growth.

### Why LMDB (Not SQLite)?

- **Billions of keys**: LMDB is a memory-mapped B+ tree that handles billions of keys natively. SQLite's B-tree becomes unusable at 300-700 GB manifest sizes.
- **No maintenance overhead**: No WAL checkpointing, no background compaction, no VACUUM. Crash-safe by default.
- **Access patterns**: Point lookups (`mdb_get`) and cursor iteration (`mdb_cursor_get`) are exactly the patterns we need.
- **Single-writer concurrency**: Matches the existing `db_mutex_` serialization. Multiple readers run concurrently without locks.

### Why a Separate Shim Library?

Running the interception logic in-process with P4d via `LD_PRELOAD` avoids the kernel overhead of `FAN_OPEN_PERM`. The shim is a thin layer (~200 lines) with no dependencies beyond libc and pthreads. It communicates with the daemon over a Unix socket, keeping the complex storage logic out of P4d's address space.
