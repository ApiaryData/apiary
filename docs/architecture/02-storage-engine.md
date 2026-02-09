# Storage Engine

## Overview

Apiary's storage engine provides ACID transactions over Parquet files in object storage. It draws from two proven designs: Delta Lake's transaction log model (append-only ledger with conditional writes for serialisation) and the bee-inspired principles of leafcutter cell sizing and mason bee isolation.

All committed state lives in object storage — data files, ledger entries, registry metadata, and coordination state. Local disk is used only for caching, spilling, and write buffering. If every compute node disappears, the bucket contains everything needed to resume operations.

---

## The StorageBackend Trait

Every storage operation in Apiary goes through a single trait:

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Write an object. Overwrites if it exists.
    async fn put(&self, key: &str, data: Bytes) -> Result<()>;
    
    /// Read an object. Returns Err if not found.
    async fn get(&self, key: &str) -> Result<Bytes>;
    
    /// List objects with a given prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    
    /// Delete an object.
    async fn delete(&self, key: &str) -> Result<()>;
    
    /// Conditional write: succeeds only if the key does not already exist.
    /// Returns Ok(true) if written, Ok(false) if key already existed.
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> Result<bool>;
    
    /// Check if an object exists.
    async fn exists(&self, key: &str) -> Result<bool>;
}
```

### LocalBackend

For solo mode and local development. Backed by the local filesystem.

```rust
pub struct LocalBackend {
    base_dir: PathBuf,
}
```

- `put` → write file to `{base_dir}/{key}`
- `get` → read file from `{base_dir}/{key}`
- `list` → directory listing with prefix matching
- `put_if_not_exists` → `open` with `O_CREAT | O_EXCL` flags (atomic on local filesystem)

### S3Backend

For multi-node and cloud deployments. Any S3-compatible endpoint: AWS S3, GCS (via S3 compatibility), MinIO, Ceph.

```rust
pub struct S3Backend {
    client: Arc<dyn ObjectStore>,  // from the `object_store` crate
    bucket: String,
}
```

- `put` → `PutObject`
- `get` → `GetObject`
- `list` → `ListObjectsV2`
- `put_if_not_exists` → `PutObject` with `If-None-Match: *` (S3 conditional put, available since 2024)

The `object_store` crate from the Arrow ecosystem provides a unified interface across S3, GCS, Azure, and local filesystem, with built-in retry logic and connection pooling.

### Configuration

```python
# Solo mode: local filesystem (default)
ap = Apiary("production")

# S3
ap = Apiary("production", storage="s3://my-bucket/apiary?region=eu-west-1")

# MinIO
ap = Apiary("production", storage="s3://apiary@minio.local:9000")

# GCS
ap = Apiary("production", storage="gs://my-bucket/apiary")
```

When no storage argument is provided, Apiary uses `LocalBackend` with `~/.apiary/data/` as the base directory.

---

## Bucket Layout

```
{storage_root}/
├── _registry/
│   ├── state_000001.json        # registry version 1
│   ├── state_000002.json        # registry version 2 (latest)
│   └── _checkpoint/
│       └── checkpoint_000100.json
├── _heartbeats/
│   ├── node_abc123.json         # node heartbeat files
│   └── node_def456.json
├── _queries/
│   └── {query_id}/              # distributed query coordination
│       ├── manifest.json
│       └── partial_{node_id}.arrow
└── {hive}/
    └── {box}/
        └── {frame}/
            ├── _ledger/
            │   ├── 000000.json
            │   ├── 000001.json
            │   └── _checkpoint/
            │       └── checkpoint_000100.json
            └── {partition_col}={value}/
                ├── cell_{uuid}.parquet
                └── cell_{uuid}.parquet
```

---

## The Transaction Ledger

Each frame has a ledger — an ordered sequence of JSON files that describes every mutation to the frame's state. The ledger is the source of truth for which cells are active, what the schema is, and the frame's version history.

### Ledger Entries

```rust
pub struct LedgerEntry {
    pub version: u64,
    pub timestamp: DateTime<Utc>,
    pub node_id: NodeId,
    pub action: LedgerAction,
}

pub enum LedgerAction {
    CreateFrame {
        schema: FrameSchema,
        partition_by: Vec<String>,
    },
    AddCells {
        cells: Vec<CellMetadata>,
    },
    RewriteCells {
        removed: Vec<CellId>,
        added: Vec<CellMetadata>,
    },
}

pub struct CellMetadata {
    pub id: CellId,
    pub path: String,                          // relative path within frame directory
    pub format: String,                        // "parquet" for v1
    pub partition_values: HashMap<String, String>,
    pub rows: u64,
    pub bytes: u64,
    pub stats: HashMap<String, ColumnStats>,
}

pub struct ColumnStats {
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
    pub null_count: u64,
    pub distinct_count: Option<u64>,
}
```

### Commit Protocol: Optimistic Concurrency via Conditional Writes

The ledger is the serialisation point for all writes to a frame. Two concurrent writers to the same frame are serialised by the storage backend's conditional write:

```
Writer A:                              Writer B:
  Read current version: 41               Read current version: 41
  Write cells to storage                 Write cells to storage
  put_if_not_exists(                     put_if_not_exists(
    _ledger/000042.json, ...)              _ledger/000042.json, ...)
  → SUCCESS (first to write)             → FAILED (key already exists)
                                         Re-read version: 42
                                         put_if_not_exists(
                                           _ledger/000043.json, ...)
                                         → SUCCESS
```

No consensus protocol. No leader election. The storage layer provides the atomic compare-and-swap operation. One writer wins, the other retries. This is the same mechanism Delta Lake uses for concurrent writes.

### Reading the Ledger

To determine the current state of a frame, a node replays the ledger entries from the latest checkpoint:

1. Check for a checkpoint file in `_ledger/_checkpoint/`
2. If found, load it (contains the full set of active cells at that version)
3. Replay any entries after the checkpoint version
4. The result is the current set of active cells and the current schema

### Checkpointing

After every 100 versions, the committing node writes a checkpoint:

```json
{
  "version": 200,
  "schema": { "fields": [...] },
  "active_cells": [
    {"id": "cell_a1b2c3", "path": "region=north/cell_a1b2c3.parquet", ...},
    {"id": "cell_e5f6g7h8", "path": "region=south/cell_e5f6g7h8.parquet", ...}
  ]
}
```

Checkpoints accelerate ledger loading — instead of replaying hundreds of entries, a node loads the latest checkpoint and replays only the entries after it.

---

## Cell Writing

### The Write Path

```
Python SDK: frame.write(table)
  │
  ├── 1. Schema validation
  │     Check incoming data against frame schema.
  │     Implicit widening (int32 → float64). Extra columns dropped with warning.
  │     Missing nullable columns filled with null. Missing non-nullable → error.
  │     Null partition values → error.
  │
  ├── 2. Partition the data
  │     Split RecordBatches by partition column values.
  │     Each unique combination of partition values becomes a group.
  │
  ├── 3. Write cells (leafcutter sizing)
  │     For each partition group:
  │       If data size < target_cell_size → write one cell
  │       If data size > target_cell_size → split into multiple cells
  │     Write Parquet files to object storage.
  │     LZ4 compression (fast, low CPU, appropriate for Pi).
  │     Calculate cell-level statistics during write.
  │
  ├── 4. Commit ledger entry
  │     Create LedgerEntry with AddCells action.
  │     Attempt put_if_not_exists for next version number.
  │     If conflict → retry from step 4 with incremented version.
  │     Cell files are already written — only the ledger entry retries.
  │
  └── 5. Return write result
        Version number, cells written, duration, colony temperature.
```

### Leafcutter Cell Sizing

Inspired by leafcutter bees (Megachile), which cut nest materials to precisely fit their chambers, cell sizes are calculated to match the scanning node's memory budget:

```rust
pub struct CellSizingPolicy {
    pub target_cell_size: u64,      // memory_per_bee / 4 (heuristic)
    pub max_cell_size: u64,         // target * 2
    pub min_cell_size: u64,         // 16MB floor (S3 per-request overhead)
}
```

**Why the floor matters:** Object storage has per-request overhead (20-200ms per GET). Very small cells (< 1MB) create excessive request overhead. The minimum cell size ensures that the I/O cost of fetching a cell is amortised over enough data to be worthwhile. On a Pi with 1GB per bee, target is 256MB. On a cloud node with 4GB per bee, target is 1GB.

### Schema Validation Rules

| Condition | Behavior |
|---|---|
| Write type safely castable to frame type (e.g., int32 → float64) | Implicit cast |
| Write type not safely castable (e.g., float64 → int32) | Error |
| Extra columns in write data | Dropped with warning |
| Missing nullable column | Filled with null |
| Missing non-nullable column | Error |
| Null value in partition column | Error |

### Write Backpressure

When colony temperature exceeds 0.85 (Hot), writes are throttled:

```python
# Default: block until temperature drops or timeout
result = frame.write(table)  # blocks up to 30s if colony is hot

# Configurable:
result = frame.write(table, backpressure="block")   # default
result = frame.write(table, backpressure="error")   # fail immediately
result = frame.write(table, backpressure="ignore")   # write regardless
```

The write result includes metadata:

```python
result.version         # ledger version
result.cells_written   # number of cells created
result.duration        # total time including backpressure wait
result.temperature     # colony temperature at write time
```

---

## Cell Reading and Partition Pruning

### Cell-Level Statistics

Every cell carries min/max statistics per column in its ledger entry. The query planner uses these to skip cells that cannot match the query filter:

```sql
SELECT * FROM sensors.temperature WHERE temp > 40.0
```

```
Planner checks cell stats:
  cell_001: temp max = 38.5  → SKIP
  cell_002: temp max = 42.1  → SCAN
  cell_003: temp max = 35.0  → SKIP

Result: 1 of 3 cells scanned
```

### Partition Pruning

Partition column values are encoded in the storage path. The planner eliminates entire partitions before examining individual cells:

```sql
SELECT * FROM sensors.temperature WHERE region = 'north'
```

```
Partitions:
  region=north/  → INCLUDE
  region=south/  → EXCLUDE
  region=east/   → EXCLUDE
```

### Projection Pushdown

Parquet is columnar. When a query selects only specific columns, only those column chunks are read from storage. Combined with partition pruning and cell statistics, this minimises the data pulled from object storage.

---

## Local Cell Cache

Each node maintains a local cache of recently accessed cells. The cache is a simple LRU (Least Recently Used) eviction policy over the local data directory:

```rust
pub struct CellCache {
    cache_dir: PathBuf,
    max_size: u64,           // total cache size limit (e.g., 2GB on a Pi)
    current_size: AtomicU64,
    entries: RwLock<HashMap<String, CacheEntry>>,
}

pub struct CacheEntry {
    pub storage_key: String,
    pub local_path: PathBuf,
    pub size: u64,
    pub last_accessed: Instant,
}
```

**Read path with cache:**

```
Query needs cell_a1b2c3.parquet:
  1. Check local cache → HIT → read from local disk (fast)
  2. Check local cache → MISS → fetch from object storage → write to cache → read
```

**Eviction:** When the cache exceeds its size limit, the least recently accessed entries are evicted. Eviction is non-blocking — it runs in a background task.

**Cache-aware planning:** In multi-node mode, each node's heartbeat reports which cells it has cached. The query planner preferentially assigns cells to nodes that already have them cached, reducing S3 fetches.

---

## Compaction

Over time, a frame accumulates many small cells from individual writes. Compaction merges them into fewer, larger cells to improve query performance (fewer S3 requests) and reduce ledger size.

### Compaction Policy

```rust
pub struct CompactionPolicy {
    pub min_cell_count: usize,         // compact when partition has > N cells (default: 10)
    pub small_cell_threshold: u64,     // cells smaller than this are "small" (default: target / 4)
    pub max_cell_age_uncompacted: Duration,  // compact after this age (default: 1 hour)
}
```

### Compaction Process

1. Identify a partition meeting the compaction criteria
2. Read all small cells in the partition from object storage
3. Merge into new, larger cells (respecting leafcutter sizing)
4. Write new cells to object storage (new UUIDs — never overwrite)
5. Commit a `RewriteCells` ledger entry via conditional put
6. If another write committed to this partition between steps 1 and 5, the conditional put fails — retry with fresh state
7. On successful commit, old cells are eligible for cleanup

### Garbage Collection

After a `RewriteCells` commit, old cell files remain in object storage until explicitly cleaned. The `apiary vacuum` command (or an automatic background task) deletes cell files that are no longer referenced by any ledger entry newer than the latest checkpoint.

---

## The Registry

The registry is the namespace catalog: which hives, boxes, and frames exist, and their metadata. It is stored as versioned JSON files in object storage, using the same conditional-put mechanism as the ledger for serialisation.

### Registry State

```json
{
  "version": 5,
  "hives": {
    "analytics": {
      "boxes": {
        "sensors": {
          "frames": {
            "temperature": {
              "schema": {"fields": [...]},
              "partition_by": ["region"],
              "created_at": "2025-02-01T00:00:00Z",
              "max_partitions": 10000,
              "properties": {}
            }
          }
        }
      }
    }
  }
}
```

### DDL Commit Protocol

```
CREATE FRAME analytics.sensors.humidity (...):
  1. Read current registry version (e.g., state_000005.json)
  2. Validate: frame does not already exist
  3. Add the frame to the in-memory registry
  4. Write new version: put_if_not_exists(state_000006.json, ...)
  5. If conflict → re-read latest version, check if frame was already created
     (idempotency check), retry if needed
```

### Partition Cardinality Limit

Each frame has a configurable maximum partition count (default: 10,000). The write path checks the partition count before committing. If a write would exceed the limit, it fails with a clear error suggesting a lower-cardinality partition column.

---

## Overwrite Semantics

v1 is append-only for regular writes. There is no row-level `DELETE` or `UPDATE`. The workaround for data correction is a full overwrite:

```python
# Read, filter, rewrite
good_data = frame.read(filter={"region": "north"})
frame.overwrite(good_data)
```

`frame.overwrite()` commits a `RewriteCells` entry that removes all existing cells and adds new ones. This is a full rewrite — not efficient for large frames — but it is correct and ACID-safe.

v2 adds proper DELETE/UPDATE support using copy-on-write rewrites.

If a user attempts `DELETE FROM` in SQL, the error message explains:

```
Error: DELETE is not supported in Apiary v1.
  Use frame.overwrite() in the Python SDK to replace data.
```

---

## Design Rationale

### Why Object Storage, Not Local-First?

Object storage as the canonical layer eliminates the three hardest distributed storage problems:

1. **Cell location tracking.** Every node can read every cell. No need to track which node holds which file, no gossip state for cell locations, no replication protocol.

2. **Write path residency.** Any node can write to any frame. The conditional put on the ledger provides serialisation. No consensus protocol needed.

3. **Durability.** S3 provides 99.999999999% durability. Apiary does not need to implement its own replication.

The tradeoff is latency (S3 round trips add 20-200ms per operation) and an internet/network dependency. The local cell cache mitigates read latency for hot data. Write latency is acceptable for batch workloads (v1 target).

### Why Conditional Puts, Not Raft?

Raft consensus requires: a leader election protocol, a persistent log on every node, a state machine that applies committed entries, a TCP transport between consensus participants, and handling of membership changes. It is the most complex component in a distributed system.

Conditional puts require: a single HTTP request with a header. The storage layer (S3, GCS, or the local filesystem) provides the atomic operation. Apiary trusts the storage layer's consistency guarantees — which have been battle-tested at planetary scale.

The tradeoff is commit latency. Raft can commit in a single network round trip (~1ms on a LAN). S3 conditional puts take 50-200ms. For v1 batch workloads, this difference is negligible. For v2 streaming writes, a write-ahead log on local disk (flushed to S3 asynchronously) will bridge the gap.
