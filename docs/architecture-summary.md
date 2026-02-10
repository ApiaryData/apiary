# Architecture Summary

This document provides a high-level overview of Apiary's architecture for contributors.

## Crate Structure

```
crates/
├── apiary-core/       Core types, traits, and identifiers
├── apiary-storage/    StorageBackend trait + LocalBackend + S3Backend
├── apiary-runtime/    Node runtime, bee pool, heartbeat, world view
├── apiary-query/      DataFusion integration, SQL parsing, distributed planner
├── apiary-python/     PyO3 bindings exposing the Python SDK
└── apiary-cli/        Command-line interface
```

## Storage Engine

### Backends

The `StorageBackend` trait abstracts all I/O. Two implementations ship in v1:

- **LocalBackend** — filesystem storage for solo mode and development.
- **S3Backend** — S3-compatible object storage (AWS S3, MinIO, GCS via S3 API).

Key operations: `put`, `get`, `list`, `delete`, and **`put_if_not_exists`** (conditional write for concurrency control).

### Data Format

- **Cells** are Parquet files with LZ4 compression.
- **Ledger entries** are JSON files recording each transaction (append, overwrite).
- **Registry** is JSON metadata for hives, boxes, and frames.

### Transaction Model

Apiary uses an **optimistic concurrency** model inspired by Delta Lake:

1. Writer reads the current ledger version.
2. Writer creates a new ledger entry (e.g., `000042.json`).
3. Writer uses `put_if_not_exists` to commit. If the version already exists (another writer won the race), the write is retried with the updated state.

No distributed locks, no Raft, no two-phase commit.

### Ledger Checkpointing

Over time, the ledger accumulates entries. Periodic **checkpointing** compacts the ledger into a single snapshot, speeding up reads.

## Coordination

### Heartbeats

Each node writes a heartbeat file to `_heartbeats/{node_id}.json` every few seconds containing:

- Node ID, address, core count, memory
- Current bee utilization
- Cached cell list
- Timestamp

### World View

Each node periodically reads all heartbeat files to build a **world view** — a snapshot of every node in the swarm. Nodes are classified as:

| State     | Condition |
|-----------|-----------|
| **Alive** | Heartbeat age < threshold (e.g., 30s) |
| **Suspect** | Heartbeat age between threshold and dead limit |
| **Dead**  | Heartbeat age > dead limit; heartbeat file cleaned up |

### Graceful Departure

On `shutdown()`, a node deletes its heartbeat file so peers immediately see it as gone.

### No Node-to-Node Communication (v1)

All coordination is storage-mediated. Nodes never talk directly to each other. This eliminates networking complexity and works through firewalls, NATs, and across cloud regions.

## Query Execution

### Single-Node Path

1. SQL is parsed by DataFusion.
2. Custom commands (`USE`, `SHOW`, `DESCRIBE`) are intercepted before DataFusion.
3. A custom `TableProvider` registers frames and resolves 3-part names.
4. Cell pruning uses partition metadata and WHERE predicates to skip irrelevant Parquet files.
5. DataFusion applies projection pushdown (only reads needed columns).
6. Bees execute the plan inside sealed chambers.
7. Results are serialized as Arrow IPC bytes and returned.

### Distributed Path

For queries spanning many cells across a multi-node swarm:

1. **Plan** — The coordinator examines the world view, checks which nodes cache which cells, and assigns cells to workers based on cache locality and capacity.
2. **Manifest** — The coordinator writes a query manifest to `_queries/{query_id}/manifest.json`.
3. **Execute** — Workers poll for manifests, find their assigned tasks, execute them in bee chambers, and write partial results to `_queries/{query_id}/results/`.
4. **Merge** — The coordinator reads partial results, merges them (applying final aggregation, sorting, limits), and returns the combined result.

Small queries (few cells, single node) skip distribution and execute locally for lower latency.

## Behavioral Model

The biological model provides runtime algorithms, not just naming conventions.

### Mason Bee Isolation

Each bee gets a **sealed chamber** with:

- A fixed memory budget (node memory ÷ number of cores, minus overhead).
- An isolated scratch directory for spill files.
- Task timeout enforcement.

If a task exceeds its budget, it fails immediately. Other bees are unaffected.

### Leafcutter Cell Sizing

Writes are split into cells sized to fit within a single bee's memory budget. This guarantees that every cell in the system is processable by any bee, even on the smallest hardware.

### Colony Temperature

A composite metric (0.0–1.0) combining memory pressure, queue depth, error rate, and storage latency. It drives operational decisions:

- **Cold** (< 0.2) — system is underutilized.
- **Ideal** (0.2–0.5) — normal operation.
- **Warm** (0.5–0.7) — approaching capacity.
- **Hot** (0.7–0.9) — apply backpressure, defer non-critical work.
- **Critical** (> 0.9) — shed load, reject new tasks.

### Abandonment Tracker

Failed tasks are retried up to a configurable trial limit. After exhausting retries, the task is abandoned and the error surfaces to the caller. This prevents poison-pill tasks from consuming resources.

## Technology Choices

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Language  | Rust | Memory safety, ARM64 cross-compilation, zero-cost abstractions |
| Python bridge | PyO3 + maturin | Zero-copy Arrow interop, native Python wheels |
| SQL engine | Apache DataFusion | Rust-native, Arrow-native, extensible query engine |
| Storage format | Apache Parquet | Columnar, compressed, widely supported |
| In-memory format | Apache Arrow | Zero-copy, columnar, cross-language |
| Object storage | S3 API (via `object_store` crate) | Universal, battle-tested |
| Async runtime | Tokio | Standard Rust async runtime |

## Contributing

1. Read the detailed architecture docs in `docs/architecture/`.
2. Build with `cargo build --workspace && maturin develop`.
3. Run tests with `cargo test --workspace`.
4. Run acceptance tests with `python test_step*_acceptance.py`.
