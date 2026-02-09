# Apiary v1 Development Prompts

## How to Use These Prompts

Each prompt corresponds to a step in the v1 build order. They are designed to be given to an AI coding agent (Claude Code, Cursor, Copilot Workspace, etc.) as a self-contained task. The prompts are sequential — each builds on the outputs of the previous steps.

**Before starting:** Ensure the agent has access to the architecture documentation:
- `01-architecture-overview.md`
- `02-storage-engine.md`
- `03-swarm-coordination.md`
- `04-query-execution.md`
- `05-behavioral-model.md`
- `06-roadmap.md`

**Repository convention:** The project is called `apiary`. The Rust workspace lives at the repository root. The Python SDK is a PyO3-based crate that builds a Python wheel.

---

## Prompt 1: Rust Skeleton + PyO3 Bridge + StorageBackend

### Context

You are building **Apiary**, a distributed data processing framework inspired by bee colony intelligence. It runs on Raspberry Pis and small compute, using Rust for the runtime and Python for the user-facing SDK. All data lives in object storage (S3, MinIO, or local filesystem). Nodes are stateless compute that coordinate through the storage layer.

Read `01-architecture-overview.md` for the project vision and `02-storage-engine.md` for the StorageBackend design.

### Task

Create the Rust workspace, the StorageBackend trait with LocalBackend and S3Backend implementations, core types, node configuration with system detection, and the PyO3-based Python SDK.

### Requirements

**Rust workspace structure:**

```
apiary/
├── Cargo.toml                  # workspace root
├── crates/
│   ├── apiary-core/            # core types, traits, config, errors
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── types.rs        # HiveId, BoxId, FrameId, CellId, BeeId, NodeId
│   │       ├── config.rs       # NodeConfig with system detection
│   │       ├── error.rs        # ApiaryError types
│   │       └── storage.rs      # StorageBackend trait
│   ├── apiary-storage/         # storage backend implementations
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── local.rs        # LocalBackend (filesystem)
│   │       └── s3.rs           # S3Backend (any S3-compatible)
│   ├── apiary-runtime/         # node runtime, bee pool (placeholder)
│   │   └── src/
│   │       ├── lib.rs
│   │       └── node.rs         # ApiaryNode
│   ├── apiary-python/          # PyO3 Python bindings
│   │   └── src/
│   │       └── lib.rs
│   └── apiary-cli/             # CLI binary (placeholder)
│       └── src/
│           └── main.rs
├── python/
│   └── apiary/
│       ├── __init__.py
│       └── py.typed
├── pyproject.toml              # maturin build config
└── README.md
```

**Core types (`types.rs`):**

Typed identifiers: `HiveId`, `BoxId`, `FrameId`, `CellId`, `BeeId`, `NodeId`. Each is a newtype wrapper around a String. All implement `Clone`, `Debug`, `PartialEq`, `Eq`, `Hash`, `Serialize`, `Deserialize`, `Display`.

**StorageBackend trait (`storage.rs`):**

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, key: &str, data: Bytes) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Bytes>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> Result<bool>;
    async fn exists(&self, key: &str) -> Result<bool>;
}
```

**LocalBackend (`local.rs`):**

Backed by filesystem. `put` writes a file. `get` reads a file. `list` does directory traversal with prefix matching. `put_if_not_exists` uses `OpenOptions` with `create_new(true)` for atomicity. Automatically creates parent directories on `put`.

**S3Backend (`s3.rs`):**

Uses the `object_store` crate from the Arrow ecosystem. Supports any S3-compatible endpoint (AWS, MinIO, GCS via S3 compat). `put_if_not_exists` uses conditional put with `If-None-Match: *` header. Parse storage URI to configure: `s3://bucket/prefix?region=eu-west-1`.

**Node configuration (`config.rs`):**

```rust
pub struct NodeConfig {
    pub node_id: NodeId,
    pub storage_uri: String,             // "local://~/.apiary/data" or "s3://bucket/prefix"
    pub cache_dir: PathBuf,              // local cache directory
    pub cores: usize,                    // auto-detected
    pub memory_bytes: u64,               // auto-detected
    pub memory_per_bee: u64,             // memory_bytes / cores
    pub target_cell_size: u64,           // memory_per_bee / 4
    pub max_cell_size: u64,              // target * 2
    pub min_cell_size: u64,              // 16MB floor
    pub heartbeat_interval: Duration,    // default 5s
    pub dead_threshold: Duration,        // default 30s
}
```

`NodeConfig::detect(storage_uri)` auto-detects cores and memory from the system.

**ApiaryNode (`node.rs`):**

```rust
pub struct ApiaryNode {
    pub config: NodeConfig,
    pub storage: Arc<dyn StorageBackend>,
}
```

- `ApiaryNode::start(config) -> Result<Self>` — initialises storage backend, logs capacity
- `ApiaryNode::shutdown(&self)` — clean shutdown

**PyO3 bindings:**

```python
from apiary import Apiary

ap = Apiary("production")                                    # local filesystem
ap = Apiary("production", storage="s3://bucket/prefix")      # S3
ap.start()
print(ap.status())  # {'node_id': '...', 'cores': 4, 'memory_gb': 4.0, 'bees': 4, 'storage': 'local'}
ap.shutdown()
```

**Dependencies:** `serde`, `serde_json`, `uuid`, `thiserror`, `pyo3`, `tokio`, `tracing`, `tracing-subscriber`, `bytes`, `object_store` (with S3 feature), `async-trait`.

### Acceptance Criteria

1. `cargo build --workspace` succeeds
2. `maturin develop` builds a Python wheel, `from apiary import Apiary` works
3. `Apiary("test").start()` detects cores and memory correctly
4. LocalBackend: put, get, list, delete, put_if_not_exists all work
5. LocalBackend: `put_if_not_exists` returns `false` when key exists (atomic)
6. S3Backend: compiles and can be configured (full S3 testing is integration-level)
7. All core types defined with required traits
8. `cargo clippy` and `cargo test` pass

---

## Prompt 2: Registry + Namespace

### Context

You are continuing development of **Apiary**. Step 1 established the workspace, StorageBackend, and core types. Now build the registry — the namespace catalog that manages Hive → Box → Frame.

Read `01-architecture-overview.md` for the namespace design and `02-storage-engine.md` for how the registry is persisted to object storage.

The registry is persisted as versioned JSON files in the storage backend. DDL operations use conditional writes for serialisation — the same mechanism as ledger commits.

### Task

Implement the registry and expose namespace operations through the Python SDK.

### Requirements

**Registry data model:**

```rust
pub struct Registry {
    version: u64,
    hives: HashMap<String, HiveEntry>,
    storage: Arc<dyn StorageBackend>,
}

pub struct HiveEntry {
    pub id: HiveId,
    pub name: String,
    pub boxes: HashMap<String, BoxEntry>,
    pub created_at: DateTime<Utc>,
}

pub struct BoxEntry {
    pub id: BoxId,
    pub name: String,
    pub frames: HashMap<String, FrameEntry>,
    pub created_at: DateTime<Utc>,
}

pub struct FrameEntry {
    pub id: FrameId,
    pub name: String,
    pub schema: FrameSchema,
    pub partition_by: Vec<String>,
    pub max_partitions: usize,       // default 10,000
    pub created_at: DateTime<Utc>,
    pub properties: HashMap<String, String>,
}
```

**Persistence via conditional writes:**

The registry is stored at `_registry/state_{version:06}.json`. DDL operations:

1. Load the latest registry state from storage
2. Apply the mutation in memory
3. Write the new version with `put_if_not_exists`
4. If conflict (another node committed first): reload and retry

Implement `Registry::load(storage) -> Result<Self>` which lists `_registry/state_*.json`, reads the highest version, and deserialises. Implement `Registry::commit(&mut self) -> Result<()>` which writes the next version with a conditional put.

**Namespace context and resolution:**

```rust
pub struct NamespaceContext {
    pub default_hive: Option<String>,
    pub default_box: Option<String>,
}
```

`resolve(path: &str) -> Result<(HiveId, BoxId, FrameId)>` resolves dotted paths using defaults. `"temperature"` uses both defaults. `"sensors.temperature"` uses default hive.

**Default hive and box:** Created automatically on first registry initialisation.

**Python SDK:**

```python
hive = ap.create_hive("analytics")
box = hive.create_box("sensors")
frame = box.create_frame("temperature",
    schema={"timestamp": "datetime", "region": "string", "temp": "float64"},
    partition_by=["region"])

ap.use_hive("analytics")
ap.use_box("sensors")

ap.list_hives()
hive.list_boxes()
box.list_frames()
box.describe_frame("temperature")

# Convenience with defaults
ap.create_frame("quick_data", schema={"a": "int64", "b": "string"})
```

Schema type strings: `"int32"`, `"int64"`, `"float32"`, `"float64"`, `"string"`, `"bool"`, `"datetime"`, `"date"`. Map to Arrow DataType internally.

### Acceptance Criteria

1. Full namespace hierarchy: create hive → create box → create frame → resolve path
2. Defaults work: single-name resolution uses default hive and box
3. Duplicate names produce clear errors
4. Registry persists to storage backend and loads on restart
5. Concurrent DDL: two nodes creating different frames — both succeed via retry
6. Concurrent DDL: two nodes creating the same frame — one succeeds, other detects duplicate
7. Python SDK namespace operations work end-to-end
8. `cargo clippy` and `cargo test` pass

---

## Prompt 3: Ledger + Cell Storage

### Context

You are continuing development of **Apiary**. Steps 1-2 established the workspace, StorageBackend, and registry. Now build the transaction ledger and cell writing/reading — the ACID storage engine.

Read `02-storage-engine.md` for the complete design: ledger format, commit protocol, cell writing, schema validation, and leafcutter sizing.

### Task

Implement the ledger, cell writer, cell reader, and the batch write path in `apiary-storage`. Expose write and read operations through the Python SDK.

### Requirements

**Ledger:**

```rust
pub struct Ledger {
    frame_path: String,          // hive/box/frame
    storage: Arc<dyn StorageBackend>,
    current_version: u64,
    active_cells: Vec<CellMetadata>,
    schema: FrameSchema,
}
```

- `Ledger::create(storage, frame_path, schema, partition_by, node_id)` — writes version 0 (CreateFrame)
- `Ledger::open(storage, frame_path)` — loads from latest checkpoint + replay
- `Ledger::commit(action, node_id) -> Result<u64>` — conditional put for next version, retries on conflict
- `Ledger::active_cells() -> &[CellMetadata]` — current cell set from replay
- `Ledger::prune_cells(filters) -> Vec<&CellMetadata>` — partition + stat pruning

**Cell writing:**

```rust
pub struct CellWriter {
    storage: Arc<dyn StorageBackend>,
    frame_path: String,
    schema: FrameSchema,
    partition_by: Vec<String>,
    sizing: CellSizingPolicy,
}
```

- Partition incoming data by partition columns
- Write Parquet cells to storage backend with LZ4 compression
- Respect leafcutter sizing: split if data exceeds `target_cell_size`, enforce `min_cell_size` floor
- Calculate cell-level statistics (min/max per column, null count, row count, byte size)
- Return `Vec<CellMetadata>` for ledger commit

**Schema validation** per Document 2: implicit widening, extra columns dropped with warning, missing nullable columns filled with null, null partition values rejected.

**Cell reading:**

Use the `parquet` crate (from `arrow-rs`) to read cells. Support projection pushdown (read only requested columns) and basic filter pushdown.

For cells in S3, use the `object_store` crate's async reader. For cells in local storage, use direct file I/O.

**End-to-end write path:**

```
frame.write(table) →
  1. Schema validation
  2. Partition data
  3. Write cells to storage (via CellWriter)
  4. Commit ledger entry (via Ledger::commit with conditional put)
  5. Return WriteResult { version, cells_written, duration }
```

**frame.overwrite(table):** Commits a `RewriteCells` entry that removes all existing cells and adds new ones.

**Checkpointing:** After every 100 versions, write a checkpoint with the full active cell set.

**Python SDK:**

```python
import pyarrow as pa

table = pa.table({
    "timestamp": [...],
    "region": ["north", "south", "north"],
    "temp": [20.5, 22.1, 19.8]
})

result = frame.write(table)
print(result.version, result.cells_written)

data = frame.read()                              # all data
data = frame.read(filter={"region": "north"})    # partition pruning
frame.overwrite(filtered_data)                    # full rewrite
```

Accept `pyarrow.Table`, `pyarrow.RecordBatch`, or `pandas.DataFrame` (convert via pyarrow).

### Acceptance Criteria

1. Create frame → write data → read it back (round-trip through Parquet in storage backend)
2. Partitioning creates separate cell files per partition value
3. Cell statistics calculated and stored in ledger entries
4. Partition pruning works: read with filter only scans matching partitions
5. Cell stat pruning: cells whose min/max cannot match the filter are skipped
6. Leafcutter sizing: large writes split into multiple cells at target size
7. Schema validation: type widening works, invalid types rejected, null partition values rejected
8. Concurrent writes: two processes writing to same frame — both succeed via conditional put retry
9. Checkpointing works after 100+ writes
10. `frame.overwrite()` replaces all cells atomically
11. Python SDK write/read works end-to-end with pyarrow Tables
12. `cargo clippy` and `cargo test` pass

---

## Prompt 4: DataFusion Integration

### Context

You are continuing development of **Apiary**. Steps 1-3 established the workspace, registry, and ACID storage. Now integrate Apache DataFusion for SQL queries.

Read `04-query-execution.md` for the integration approach: register cells as DataFusion ListingTables, handle custom SQL commands, and support EXPLAIN.

**After this step, solo mode is functionally complete.**

### Task

Integrate DataFusion by resolving frame names to cell URIs, registering them as ListingTables, and handling custom SQL commands (USE, SHOW, DESCRIBE).

### Requirements

**Create `apiary-query` crate:**

```rust
pub struct ApiaryQueryContext {
    session: SessionContext,
    storage: Arc<dyn StorageBackend>,
    registry: Arc<RwLock<Registry>>,
    namespace_context: NamespaceContext,
}
```

**Query execution flow:**

1. Parse the SQL to detect custom commands (USE, SHOW, DESCRIBE)
2. If standard SQL: extract table references, resolve via registry, read ledger, prune cells, register as ListingTable, execute via DataFusion
3. Return results as Arrow RecordBatches

**Object store registration:** Register the appropriate `ObjectStore` with DataFusion's `SessionContext` based on the StorageBackend configuration. DataFusion's built-in `object_store` integration handles S3/local/GCS transparently.

**Cell pruning before registration:** Only register cells that survive partition pruning and cell stat pruning. This means DataFusion never sees irrelevant files.

**Custom commands:**

```sql
USE HIVE analytics;
USE BOX sensors;
SHOW HIVES;
SHOW BOXES IN analytics;
SHOW FRAMES IN analytics.sensors;
DESCRIBE analytics.sensors.temperature;
```

Intercept these with string matching before DataFusion, query the registry, return results as Arrow tables.

**EXPLAIN:** For solo mode, DataFusion's built-in EXPLAIN is sufficient. Add pruning information (cells pruned, partitions eliminated) to the output.

**Python SDK:**

```python
result = ap.sql("SELECT region, avg(temp) FROM analytics.sensors.temperature GROUP BY region")
print(result)  # pyarrow Table

ap.sql("USE HIVE analytics")
ap.sql("USE BOX sensors")
result = ap.sql("SELECT * FROM temperature WHERE region = 'north'")
df = result.to_pandas()
```

**Unsupported DML:** If the user attempts DELETE/UPDATE, return a clear error explaining the limitation.

### Acceptance Criteria

1. `ap.sql("SELECT * FROM hive.box.frame")` works end-to-end
2. Partition pruning: `WHERE region = 'north'` only registers north cells with DataFusion
3. Cell stat pruning: `WHERE temp > 40` skips cells whose max < 40
4. Projection pushdown: `SELECT temp FROM ...` only reads the temp column
5. Aggregations: `GROUP BY` produces correct results
6. USE HIVE / USE BOX set context correctly
7. SHOW and DESCRIBE return correct results
8. DELETE/UPDATE produce clear error messages
9. **Solo mode is complete:** create → write → query on a single node
10. `cargo clippy` and `cargo test` pass

---

## Prompt 5: Mason Bee Isolation + BeePool

### Context

You are continuing development of **Apiary**. Steps 1-4 produced a functional solo-mode platform. Before adding multi-node execution, implement mason bee isolation — sealed execution chambers with hard memory budgets per bee.

Read `05-behavioral-model.md`, Behavior 1 (Sealed Chambers — Mason Bees).

### Task

Implement the mason bee chamber system and BeePool in `apiary-runtime`.

### Requirements

**Bee and Chamber types:**

```rust
pub struct Bee {
    pub id: BeeId,
    pub state: BeeState,
    pub chamber: MasonChamber,
}

pub enum BeeState {
    Idle,
    Busy(TaskId),
}

pub struct MasonChamber {
    pub bee_id: BeeId,
    pub memory_budget: u64,
    pub memory_used: AtomicU64,
    pub scratch_dir: PathBuf,
    pub task_timeout: Duration,
}
```

**BeePool:**

```rust
pub struct BeePool {
    bees: Vec<Bee>,
    task_queue: VecDeque<QueuedTask>,
}
```

- `BeePool::new(config) -> Self` — creates N bees (N = cores), each with `memory_per_bee` budget
- `BeePool::submit(task) -> JoinHandle<TaskResult>` — find idle bee, mark busy, execute in chamber. If all busy, queue.
- `BeePool::status() -> Vec<BeeStatus>` — state and memory per bee

**Memory enforcement:** Cooperative — tasks call `chamber.request_memory(bytes)` before large allocations. Exceeding budget terminates the task.

**Task timeout:** Tasks exceeding `task_timeout` are terminated.

**Scratch isolation:** Each bee has `{cache_dir}/scratch/bee_{n}/`, cleared after each task.

**Integration:** Update `ap.sql()` to execute through the BeePool.

**Python SDK:**

```python
ap.bee_status()
# [{'bee_id': 'bee-0', 'state': 'idle', 'memory_used': 0, 'memory_budget': 1073741824}, ...]
```

### Acceptance Criteria

1. BeePool creates correct number of bees
2. Tasks execute with enforced memory budgets
3. Memory exceeded → task terminated, other bees unaffected
4. Task timeout terminates long-running tasks
5. Scratch directories isolated and cleaned
6. Multiple concurrent SQL queries run on separate bees
7. When all bees busy, tasks queue
8. `cargo clippy` and `cargo test` pass

---

## Prompt 6: Heartbeat Table + World View

### Context

You are continuing development of **Apiary**. Steps 1-5 produced a solo-mode platform with bee isolation. Now add multi-node awareness through the heartbeat table.

Read `03-swarm-coordination.md` for the heartbeat design.

### Task

Implement heartbeat writing, world view building, and node lifecycle in `apiary-runtime`.

### Requirements

**Heartbeat writer:** Background task writing `_heartbeats/node_{id}.json` every `heartbeat_interval`.

Heartbeat contents: node_id, timestamp, capacity (cores, memory, memory_per_bee, target_cell_size), load (bees_busy, bees_idle, memory_pressure, queue_depth, colony_temperature), cache summary (frames with cell counts and sizes).

**World view builder:** Background task polling `_heartbeats/` prefix every `poll_interval`.

```rust
pub struct WorldView {
    pub nodes: HashMap<NodeId, NodeStatus>,
    pub updated_at: DateTime<Utc>,
}

pub enum NodeState { Alive, Suspect, Dead }
```

Node is Alive if heartbeat < `dead_threshold` old. Suspect at 50-100% of threshold. Dead beyond threshold.

**Node lifecycle:**
- Join: write first heartbeat. Other nodes discover on next poll.
- Graceful departure: delete heartbeat file, stop.
- Crash: heartbeat goes stale, marked dead after threshold.

**Cleanup:** Background task deletes heartbeat files for nodes dead > 1 hour.

**Python SDK:**

```python
ap.swarm_status()
# {
#   'nodes': [
#     {'node_id': '...', 'state': 'alive', 'bees': 4, 'idle_bees': 3},
#     ...
#   ],
#   'total_bees': 8,
#   'total_idle_bees': 7
# }
```

### Acceptance Criteria

1. Node writes heartbeat to storage backend every interval
2. World view builder discovers other nodes' heartbeats
3. Stale heartbeat → node marked Suspect then Dead
4. Graceful departure → heartbeat deleted, node removed from world view
5. Solo mode still works (node sees only its own heartbeat)
6. `ap.swarm_status()` returns accurate information
7. `cargo clippy` and `cargo test` pass

---

## Prompt 7: Distributed Query Execution

### Context

You are continuing development of **Apiary**. Steps 1-6 produced a multi-node-aware platform. Now add distributed query execution — splitting work across the swarm with partial results exchanged via object storage.

Read `04-query-execution.md` for the full design: cache-aware planning, SQL fragment generation, the execution protocol, and EXPLAIN/EXPLAIN ANALYZE.

### Task

Implement the distributed planner, SQL fragment generator, execution coordinator, and result merger.

### Requirements

**Cache-aware load balancer:**

```rust
pub fn plan_query(
    cells: Vec<CellInfo>,
    nodes: Vec<NodeInfo>,
    local_node: NodeId,
) -> QueryPlan
```

Assign cells to nodes: prefer cache locality, then idle capacity. Respect leafcutter memory budgets. If all cells fit one bee, run locally.

**SQL fragment generation:**

Decompose aggregation queries into partial + merge phases. Simple scans just split cells. See the decomposition table in Document 4.

```rust
pub struct DistributedPlan {
    pub query_id: String,
    pub tasks: Vec<PlannedTask>,
    pub merge_sql: String,      // how to combine partial results
}

pub struct PlannedTask {
    pub task_id: String,
    pub node_id: NodeId,
    pub cells: Vec<String>,      // storage keys
    pub sql_fragment: String,
}
```

**Execution protocol:**

1. Coordinator writes `_queries/{query_id}/manifest.json`
2. Coordinator executes its own tasks, writes partial result
3. Workers poll `_queries/` for manifests, execute assigned tasks, write partial results as Arrow IPC
4. Coordinator polls for all partial results, merges, returns

**Task polling:** Each node runs a background poller checking `_queries/` for manifests with tasks assigned to it. Default interval: 500ms.

**Task timeout and retry:** If a partial result does not appear within timeout, check world view. If node is dead, reassign. If alive, extend or abort. Use abandonment tracker (Behavior 3).

**EXPLAIN:**

```sql
EXPLAIN SELECT region, avg(temp) FROM temperature GROUP BY region;
-- Shows: pruning results, node assignments, cache hits, SQL fragments
```

**EXPLAIN ANALYZE:**

```sql
EXPLAIN ANALYZE SELECT region, avg(temp) FROM temperature GROUP BY region;
-- Executes query, shows: per-node timing, S3 fetches, cache hits, memory, coordination overhead
```

**Cleanup:** Coordinator deletes `_queries/{query_id}/` after completion. Background task cleans orphaned queries > 1 hour old.

**Python SDK:** No API changes — `ap.sql()` transparently distributes when world view has multiple alive nodes.

### Acceptance Criteria

1. Query distributes across multiple nodes when beneficial
2. Partial aggregation (SUM/COUNT) produces correct results
3. Cache locality: cells assigned to nodes that have them cached
4. Leafcutter sizing: task assignments respect per-node memory budgets
5. Task timeout and retry: failed tasks reassigned to another node
6. `EXPLAIN` shows plan with pruning, assignments, and reasoning
7. `EXPLAIN ANALYZE` shows execution timing and S3 fetches
8. Single-node fallback: small queries run locally without distribution overhead
9. Orphaned query cleanup works
10. `cargo clippy` and `cargo test` pass

---

## Prompt 8: Local Cell Cache

### Context

You are continuing development of **Apiary**. Steps 1-7 produced a distributed query engine. Now add the local cell cache to reduce S3 fetches for frequently accessed data.

Read `02-storage-engine.md` for the cache design and `03-swarm-coordination.md` for how cache contents are reported in heartbeats.

### Task

Implement the LRU cell cache, integrate it with the read path and the planner, and report cache contents in heartbeats.

### Requirements

**CellCache:**

```rust
pub struct CellCache {
    cache_dir: PathBuf,
    max_size: u64,
    current_size: AtomicU64,
    entries: RwLock<HashMap<String, CacheEntry>>,
}
```

- `get_or_fetch(storage_key) -> PathBuf` — return local path. If not cached, fetch from storage backend, store locally, return path.
- LRU eviction when cache exceeds `max_size`
- Background eviction (non-blocking)
- Thread-safe for concurrent bee access

**Integration with read path:** Cell reading checks cache first. Cache hit → read from local disk. Miss → fetch from storage, cache, read.

**Integration with planner:** The cache-aware load balancer uses cache contents from heartbeats to prefer nodes with cached cells.

**Integration with heartbeats:** Report cached frame summaries (frame path, cell count, total bytes) in the heartbeat file.

**Write buffering:** Cells written locally are automatically in the cache (they were just created). The cache entry is created during the write path without a separate fetch.

**Cache sizing:** Default: 50% of available disk space minus 1GB (reserve for OS and scratch). Configurable via `--cache-size` flag.

### Acceptance Criteria

1. First read of a cell fetches from storage, second read serves from cache
2. LRU eviction frees space when cache exceeds limit
3. Concurrent reads of different cells work correctly
4. Heartbeat reports cache contents
5. Planner prefers nodes with cached cells
6. Freshly written cells are immediately in cache
7. `cargo clippy` and `cargo test` pass

---

## Prompt 9: Colony Temperature + Compaction + Abandonment

### Context

You are continuing development of **Apiary**. Steps 1-8 produced a cached, distributed query engine. Now add the behavioral model: colony temperature, compaction, and task abandonment.

Read `05-behavioral-model.md` for the four v1 behaviors.

### Task

Implement colony temperature measurement and regulation, automatic compaction, and task abandonment with diagnostics.

### Requirements

**Colony temperature:**

```rust
pub struct ColonyThermometer { setpoint: f64 }
```

Measure from bee utilisation, memory pressure, queue depth. Regulate: Cold → run compaction, Hot → backpressure writes, Critical → reject work.

**Write backpressure:** When temperature > 0.85, `frame.write()` blocks (up to 30s) or returns error based on `backpressure` parameter. Write result includes `temperature` field.

**Compaction:**

```rust
pub struct CompactionPolicy {
    pub min_cell_count: usize,      // default 10
    pub small_cell_threshold: u64,  // target_cell_size / 4
    pub max_uncompacted_age: Duration, // default 1 hour
}
```

Background task checks each frame's partitions against the policy. When criteria are met, read small cells, merge into larger cells (respecting leafcutter sizing), commit `RewriteCells` via conditional put. Conflict (concurrent write) → retry.

Compaction runs when colony temperature is Cold or Ideal. Paused when Warm or hotter.

**Manual compaction:** `apiary vacuum <frame>` forces a compaction pass.

**Abandonment:**

```rust
pub struct AbandonmentTracker {
    trial_counts: HashMap<TaskId, u32>,
    trial_limit: u32, // default 3
}
```

Record failures per task. After `trial_limit`, abandon with diagnostic error including full attempt history.

**Python SDK:**

```python
ap.colony_status()
# {'temperature': 0.42, 'regulation': 'ideal', 'bees': {'busy': 2, 'idle': 2}}

result = frame.write(table)
result.temperature  # 0.42
```

### Acceptance Criteria

1. Colony temperature accurately reflects node load
2. Heavy query load → temperature rises
3. Temperature > 0.85 → writes blocked (backpressure)
4. Temperature < 0.3 → compaction runs automatically
5. Compaction merges small cells, reduces cell count
6. Compaction handles concurrent write conflicts via retry
7. Tasks failing 3 times → abandoned with diagnostic error
8. `ap.colony_status()` returns accurate state
9. `cargo clippy` and `cargo test` pass

---

## Prompt 10: Testing and Hardening

### Context

You are continuing development of **Apiary**. Steps 1-9 produced the complete v1 feature set. Now harden the system through comprehensive testing, benchmarks, and documentation.

Read the success criteria from `06-roadmap.md`.

### Task

Create integration tests, chaos tests, benchmarks, and project documentation.

### Requirements

**Integration tests:**

```rust
// tests/solo_mode.rs — start → create → write → query → verify
// tests/multi_node.rs — 3 nodes with shared storage → distributed query
// tests/node_failure.rs — node dies during query → task retried
// tests/mason_isolation.rs — memory exceeded → other bees unaffected
// tests/concurrent_writes.rs — two writers → both succeed via conditional retry
// tests/compaction.rs — many small writes → compaction reduces cells → query faster
// tests/backpressure.rs — heavy load → temperature rises → writes throttled
```

**Chaos testing:** Harness that starts N nodes (same process, different configs), randomly kills nodes during queries, verifies results are correct.

**Benchmarks (criterion):**

- Write throughput: rows/second at 1K, 10K, 100K, 1M rows
- Query latency: scan, filtered scan, aggregation at 100K, 1M, 10M rows
- Cache impact: cold vs warm query latency
- Distributed speedup: 1 node vs 3 nodes on same data

**Documentation:**

```
docs/
├── getting-started.md       # install, start, first query in 5 minutes
├── concepts.md              # namespace, bees, biological model
├── python-sdk.md            # API reference
├── sql-reference.md         # supported syntax
└── architecture-summary.md  # high-level for contributors
```

**README.md:** Brief description, quick start (3 commands), feature highlights, architecture diagram.

**Error message review:** All errors should be clear, specific, and actionable.

### Acceptance Criteria

1. All integration tests pass
2. Chaos tests demonstrate recovery from node kills during queries
3. Mason isolation test proves no cross-bee contamination
4. Benchmarks establish baselines
5. Documentation covers getting started, concepts, SDK, SQL
6. README provides compelling 5-minute introduction
7. `cargo clippy` with no warnings, `cargo test` passes, `cargo bench` runs

---

## Prompt Sequencing

```
Prompt 1 (Skeleton + StorageBackend)
  └→ Prompt 2 (Registry)
      └→ Prompt 3 (Ledger + Cells)
          └→ Prompt 4 (DataFusion) ← SOLO MODE COMPLETE
              └→ Prompt 5 (Mason Chambers)
                  └→ Prompt 6 (Heartbeats + World View) ← MULTI-NODE AWARE
                      └→ Prompt 7 (Distributed Queries)
                          └→ Prompt 8 (Cell Cache)
                              └→ Prompt 9 (Behavioral Model)
                                  └→ Prompt 10 (Testing)
```

**Milestones:**

After Prompt 4: Single node, full SQL, zero distribution.
After Prompt 7: Multiple nodes, distributed queries via shared storage.
After Prompt 9: Self-regulating swarm with backpressure and compaction.
After Prompt 10: v1 release candidate.
