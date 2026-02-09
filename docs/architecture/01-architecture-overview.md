# Architecture Overview

## What Is Apiary?

Apiary is a distributed data processing framework inspired by bee colony intelligence. It is designed to run on small, inexpensive hardware — Raspberry Pis, NUCs, old laptops — while scaling seamlessly to cloud compute. Users write data via Python, query via SQL, and the swarm distributes work across available nodes with no central controller.

Apiary is a **lakehouse for small compute**. Data lives in object storage (S3, GCS, MinIO, or the local filesystem). Compute nodes are stateless workers that read from and write to that storage. Nodes coordinate through the storage layer itself — no consensus protocol, no gossip network, no direct node-to-node communication. A node joins the swarm by connecting to the same storage bucket. A node leaves by stopping its heartbeat. The storage layer is the single source of truth for data, metadata, and coordination.

The biological model is not a metaphor layered over conventional design. It is the scheduling and resource management framework. Each CPU core is a bee. Bees operate in isolated chambers with hard memory budgets. Colony temperature regulates admission and backpressure. Cell sizes are precision-cut to fit each worker's capacity. The swarm self-regulates without a coordinator because the regulation is embedded in every node's local behavior.

---

## Core Principles

**Object storage is canonical.** All committed data, all metadata, and all coordination state lives in object storage. Nodes are stateless compute with local caches. If every node disappears, the data and the catalog remain in the bucket, intact and queryable by any new node that connects.

**No node-to-node communication.** Nodes do not talk to each other. They talk to object storage. This eliminates an entire class of distributed systems problems: NAT traversal, mesh networking, gossip protocol tuning, consensus leader election, split brain, network partitions between nodes. The only network dependency is connectivity to the storage endpoint.

**Conditional writes for serialisation.** When two nodes try to commit simultaneously, object storage's conditional put semantics (`If-None-Match`, `O_CREAT|O_EXCL`) provide the serialisation point. One write succeeds, the other retries. This replaces Raft consensus with a mechanism that is simpler, battle-tested at planetary scale, and requires zero coordination infrastructure.

**Biology is the runtime, not the metaphor.** The bee-inspired behavioral model governs concrete resource management decisions: memory budgets per core, cell sizing per node, task retry thresholds, and system-wide backpressure. These behaviors are algorithmically grounded in swarm intelligence research and solve real distributed systems problems.

**Solo mode is the starting point.** A single node with local filesystem storage is a fully functional data platform. No distributed coordination, no object storage dependency, no configuration. Install and use. Multi-node operation adds scale, not complexity — nodes discover each other through the shared storage layer.

---

## The Three-Layer Interface

```
┌─────────────────────────────────────────────┐
│  SQL (DataFusion)                           │
│  SELECT avg(temp) FROM sensors.temperature  │
├─────────────────────────────────────────────┤
│  Python SDK (PyO3)                          │
│  frame.write(table) / ap.sql("...")         │
├─────────────────────────────────────────────┤
│  Rust Runtime                               │
│  Storage backend, bee pool, planner, cache  │
└─────────────────────────────────────────────┘
```

**SQL** is the query language. Apache DataFusion provides parsing, logical planning, optimisation, and execution. Apiary resolves frame names to cell files in object storage and lets DataFusion's built-in Parquet and object store support handle the rest.

**Python** is the programming interface. A PyO3-based SDK provides frame creation, data writing, SQL execution, and swarm status. It accepts PyArrow Tables, RecordBatches, and Pandas DataFrames.

**Rust** is the runtime. Compiled, memory-safe, cross-platform. A single binary runs on ARM64 (Pi) and x86_64 (cloud). The runtime manages the storage backend, bee pool, local cache, query planning, and behavioral model.

---

## Namespace Hierarchy

```
Hive (database)
└── Box (schema)
    └── Frame (table)
        └── Cells (Parquet files in object storage)
```

Three user-facing levels, matching standard SQL conventions: `hive.box.frame` maps to `database.schema.table`.

**Hive** — the top-level grouping. A logical database. `analytics`, `staging`, `raw`.

**Box** — a namespace within a hive. A logical schema. `sensors`, `billing`, `users`.

**Frame** — a queryable dataset. A logical table. Contains a schema, optional partition columns, and a ledger that tracks which cells (Parquet files) hold the data.

Default hive and box (`default.default`) are created automatically so a user can immediately create frames without namespace ceremony.

```sql
-- Fully qualified
SELECT * FROM analytics.sensors.temperature;

-- With defaults set
USE HIVE analytics;
USE BOX sensors;
SELECT * FROM temperature;
```

---

## Physical Compute Model: 1 Core = 1 Bee

Every virtual core on a node is a **bee** — an independent unit of compute with its own memory budget and isolated execution context (a mason bee chamber).

```
Raspberry Pi 4 (4 cores, 4GB RAM):
  4 bees, each with ~1GB memory budget

Cloud VM (32 cores, 128GB RAM):
  32 bees, each with ~4GB memory budget
```

Bees do not share mutable state. A bee that exceeds its memory budget or time limit is terminated without affecting other bees on the same node. This isolation is critical on Pis where a single runaway task could exhaust the entire system.

---

## Three-Tier Storage Model

```
Tier 1 — Memory
  Arrow RecordBatches in a bee's mason chamber.
  Active computation. Governed by the bee's memory budget.

Tier 2 — Local Disk (cache + spill)
  Recently accessed cells pulled from object storage (LRU cache).
  Spill files when a bee exceeds its memory budget during a large operation.
  Write buffer for cells before they are flushed to object storage.

Tier 3 — Object Storage (canonical)
  S3, GCS, Azure Blob, MinIO, or local filesystem via StorageBackend trait.
  All committed data. All metadata. All coordination state.
  The single source of truth.
```

---

## Subsystems

### Storage Backend

A `StorageBackend` trait with two implementations: `LocalBackend` (filesystem) and `S3Backend` (any S3-compatible endpoint). The trait provides: `put`, `get`, `list`, `delete`, and `put_if_not_exists` (conditional write). All Apiary operations — ledger commits, cell writes, registry persistence, heartbeats — go through this trait.

In solo mode, `LocalBackend` uses the local filesystem. In multi-node mode, all nodes connect to the same `S3Backend`. The application code is identical in both cases.

See: **Document 2 — Storage Engine**

### Ledger and ACID Transactions

Each frame has a transaction ledger — a sequence of JSON files in object storage that describes the frame's state. Writes append a new ledger entry using conditional puts for atomicity. Concurrent writes are serialised by the storage layer: one succeeds, the other retries with a fresh version number. This is the same model as Delta Lake's `_delta_log`.

See: **Document 2 — Storage Engine**

### Swarm Coordination

Nodes discover each other through a heartbeat table in object storage. Each node writes a heartbeat file every N seconds. Nodes read each other's heartbeats to build a world view: who is alive, their capacity, their load, their cache contents. No gossip protocol, no direct node-to-node communication.

See: **Document 3 — Swarm Coordination**

### Query Planning and Execution

A SQL query is resolved to a set of cells in object storage. A cache-aware load balancer assigns cells to nodes based on cache locality, capacity, and current load. Each node independently scans its assigned cells from object storage (or local cache), computes a partial result, writes it to object storage, and the coordinator merges the partials into the final result.

See: **Document 4 — Query Execution**

### Behavioral Model

Four behaviors govern the runtime in v1:

- **Mason bee isolation** — sealed execution chambers with hard memory budgets per bee
- **Leafcutter bee sizing** — cells and tasks are precisely sized to fit each node's capacity
- **Abandonment** — tasks that repeatedly fail are abandoned and retried elsewhere
- **Colony temperature** — a composite health metric that drives write backpressure and maintenance scheduling

The full twenty-behavior model (waggle dance, pheromone signalling, three-tier workers, guard bees, etc.) activates progressively in v2 and v3 as the system gains direct node-to-node communication and a more sophisticated planner.

See: **Document 5 — Behavioral Model**

---

## Technology Stack

| Component | Technology | Why |
|---|---|---|
| Runtime language | Rust | Memory safety, zero-cost abstractions, cross-compilation to ARM64 |
| Python bridge | PyO3 + maturin | Zero-copy Arrow interop, native Python wheel |
| SQL engine | Apache DataFusion | Rust-native, Arrow-native, extensible catalog |
| Storage format | Apache Parquet | Columnar, compressed, universal ecosystem support |
| In-memory format | Apache Arrow | Zero-copy, columnar, cross-language |
| Object storage | S3 API (via `object_store` crate) | Universal, battle-tested, works with AWS/GCS/Azure/MinIO |
| Local storage | Filesystem | Simple, fast, no dependencies |
| Async runtime | Tokio | Standard Rust async, needed for S3 operations |
| Serialisation | serde + JSON | Ledger entries, registry state, heartbeats |

---

## The Beekeeping Metaphor: A Complete Mapping

### Core Concepts

| Bee Concept | Apiary Component | Role |
|---|---|---|
| Hive | Database / Catalog | Top-level logical grouping |
| Box | Schema | Domain namespace within a hive |
| Frame | Table | The queryable data unit |
| Cell | Parquet file | Physical storage unit in object storage |
| Bee | Virtual core | Unit of compute (1 vcore = 1 bee) |
| Swarm | Compute mesh | All nodes and their bees |
| Meadow | Object storage | Where all data lives (S3, filesystem) |
| Queen | — | No queen; coordination through shared storage |
| Beekeeper | Emergent behavior | No single coordinator |

### Behavioral Model

| Species | Behavior | Apiary Component | Role |
|---|---|---|---|
| Mason Bees (Osmia) | Sealed Chambers | Isolated execution | Memory-budgeted, blast-radius containment per bee |
| Leafcutter Bees (Megachile) | Precise Cutting | Cell sizing / task partitioning | Work sized to fit each worker's capacity |
| Honeybee | Abandonment | Task retry / failure handling | Self-healing after repeated failures |
| Honeybee | Colony Temperature | System homeostasis | Write backpressure and maintenance scheduling |

### Future Behaviors (v2+)

| Species | Behavior | Apiary Component | When |
|---|---|---|---|
| Honeybee | Waggle Dance | Quality-weighted task distribution | v2 (requires node-to-node communication) |
| Honeybee | Three-Tier Workers | Employed / Onlooker / Scout roles | v2 |
| Honeybee | Pheromone Signalling | Backpressure and rate limiting | v2 (via gossip protocol) |
| Honeybee | Nectar Ripening | Streaming data maturation | v2 |
| Honeybee | Guard Bees | Distributed authentication | v2 |
| Honeybee | Swarming | Auto-scaling by colony division | v2 |
| Honeybee | Propolis | Continuous security auditing | v2 |
| Honeybee | Winter Cluster | Degraded mode conservation | v2 |
| Honeybee | Orientation Flights | New node ramp-up | v2 |
| Honeybee | Trophallaxis | Data transfer with metadata | v2 |
| Stingless Bees | Tiered Storage Pots | Cell classification | v2 |
| Bumblebees | Buzz Pollination | Forced data extraction | v2 |
| Cuckoo Bees | Brood Parasitism | Adversarial testing | v2 |
| Carpenter Bees | Tunnel Boring | Embedded indexes | v2 |
| Orchid Bees | Chemical Library | Metadata enrichment | v2 |
| Sweat Bees | Variable Sociality | Adaptive coordination | v2 (gossip + direct comms) |

---

## What Makes Apiary Different

1. **Small-compute-first design.** Not a cloud system squeezed onto a Pi, but a system designed for resource-constrained hardware that scales to the cloud.

2. **Object storage as coordination layer.** No consensus protocol, no gossip network, no node-to-node communication. Nodes coordinate through the same storage that holds the data. This eliminates the hardest distributed systems problems.

3. **Biology-driven resource management.** Memory budgets, cell sizing, backpressure, and failure recovery are governed by bee-inspired behavioral patterns — not metaphors, but algorithms.

4. **Zero-configuration multi-node.** A second node joins the swarm by connecting to the same storage bucket. No seed nodes, no tokens, no mesh network setup. The storage layer is the discovery mechanism.

5. **Lakehouse on a budget.** Delta Lake-style ACID transactions over Parquet in object storage, queryable via SQL, writable via Python. The same model that powers Databricks, running on hardware that costs less than a month of cloud compute.

6. **Solo to swarm without code changes.** A single Pi with local filesystem storage is a fully functional data platform. Switching to S3 and adding nodes is a configuration change, not an architecture change.
