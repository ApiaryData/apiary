# Roadmap & MVP Scope

## Overview

Apiary's architecture is comprehensive, but it ships incrementally. The guiding principle: **prove the core loop first, then expand.** v1 must demonstrate that a swarm of nodes can ingest data into object storage, maintain ACID transactions, and answer SQL queries with distributed execution. Everything else builds on that proof.

The simplified v1 architecture — object storage as the coordination layer, no consensus protocol, no gossip, no direct node-to-node communication — means the core loop can be proven with dramatically less code than a traditional distributed system.

---

## v1 — Prove the Core

### Goal

A working distributed data platform. A user installs a binary, writes data via Python, queries via SQL, and sees work distributed across multiple nodes. Data lives in object storage (S3, MinIO, or local filesystem). Nodes are stateless compute that coordinate through the storage layer.

### What Ships

**Runtime:**
- Rust runtime compiled for ARM64 and x86_64
- Single binary installation
- PyO3 bridge to Python SDK
- 1 core = 1 bee compute model
- StorageBackend trait with LocalBackend and S3Backend

**Namespace:**
- Hive → Box → Frame hierarchy
- Default hive and box for simple deployments
- Context setting (USE HIVE, USE BOX)

**Storage (Object Storage + Ledger):**
- StorageBackend trait: put, get, list, delete, put_if_not_exists
- LocalBackend (filesystem) and S3Backend (any S3-compatible endpoint)
- Transaction ledger (append-only JSON in storage backend)
- Optimistic concurrency via conditional writes (no Raft)
- Parquet cells with LZ4 compression
- Cell-level statistics for partition pruning
- Leafcutter cell sizing based on node capacity
- Ledger checkpointing
- Basic compaction (merge small cells, manual + automatic)
- Local cell cache with LRU eviction

**SQL Layer:**
- DataFusion integration via ListingTable registration
- Single-node and distributed scan execution
- Filter pushdown, projection pruning
- Partition elimination via cell stats
- Custom commands: USE, SHOW, DESCRIBE
- EXPLAIN and EXPLAIN ANALYZE with swarm diagnostics

**Multi-Node Coordination:**
- Heartbeat table in object storage
- World view from heartbeat polling
- Node discovery via shared storage (no seed nodes, no tokens)
- Failure detection via stale heartbeats

**Query Planning:**
- Cache-aware load balancer (not a multi-strategy cost model)
- SQL fragment generation for distributed execution
- Partial results via object storage (Arrow IPC)
- Task timeout and retry with abandonment

**Behavioral Model (4 behaviors):**
- Mason bee sealed chambers (memory-budgeted isolated execution)
- Leafcutter bee cell sizing (target sizes based on node capacity)
- Abandonment (task retry with diagnostic reporting)
- Colony temperature (write backpressure, maintenance scheduling)

**Python SDK:**
```python
from apiary import Apiary

# Solo mode (local filesystem)
ap = Apiary("production")

# Multi-node mode (S3)
ap = Apiary("production", storage="s3://my-bucket/apiary")

ap.start()

hive = ap.create_hive("analytics")
box = hive.create_box("sensors")
frame = box.create_frame("temperature",
    schema={"timestamp": "datetime", "region": "string", "temp": "float64"},
    partition_by=["region"])

frame.write(table)
result = ap.sql("SELECT region, avg(temp) FROM analytics.sensors.temperature GROUP BY region")
```

**CLI (minimal):**
```bash
apiary start [--storage <uri>]   # start local node
apiary sql "SELECT ..."          # one-shot query
apiary shell                     # Python REPL with Apiary pre-configured
```

### What Is Deferred

- Direct node-to-node communication (gossip, Arrow Flight)
- Raft or any consensus protocol
- Full behavioral model (waggle dance, pheromones, three-tier workers, etc.)
- Streaming write path (nectar ripening)
- Data replication (S3 provides durability)
- Time travel queries
- Schema evolution
- DELETE / UPDATE DML
- Row-level security
- Embedded indexes (bloom filters, zone maps)
- Full interactive CLI (REPL, import, registry commands)
- Custom DataFusion CatalogProvider / TableProvider
- Distributed joins

### Open Design Questions for v1

1. **Task notification latency.** Nodes poll `_queries/` prefix at 500ms interval. Is this acceptable, or does v1 need a lightweight notification mechanism?

2. **Conditional put support.** S3 added `If-None-Match` in 2024. GCS and Azure have equivalents. MinIO supports it. But some S3-compatible stores may not. Do we need a fallback (advisory locking via a lock file)?

3. **Cache sizing on Pi.** A 4GB Pi with 4 bees (1GB each) has limited space for local cache. How much disk should the cache use? Should it be configurable or auto-detected from available disk space?

4. **Partial result size limits.** If a partial result is very large (e.g., a full scan with no aggregation), writing it to S3 and reading it back is expensive. Should the coordinator set a size limit and fall back to local-only execution for large results?

### Success Criteria

- Single Pi with local filesystem: create frame, write 10M+ rows, query via SQL
- 3-node cluster with shared MinIO: same operations, distributed execution
- Query with partition pruning scans only relevant cells
- Query with cell stat pruning skips cells based on min/max
- EXPLAIN shows which cells were pruned and why
- EXPLAIN ANALYZE shows per-node timing and S3 fetch counts
- Node failure during distributed query: task retried on another node
- Mason chambers prevent memory cascade (one bee's failure does not affect others)
- Colony temperature rises under load and triggers write backpressure
- Write result includes temperature and cell count
- Compaction merges small cells and reduces query scan count

---

## v2 — Direct Communication + Full Behavioral Model

### Goal

Add optional direct node-to-node communication for performance. Activate the full twenty-behavior biological model. Support streaming ingestion and advanced query patterns.

### What Ships

**Communication:**
- SWIM gossip for sub-second failure detection and pheromone propagation
- Arrow Flight for low-latency data shuffles between nodes
- Object storage coordination remains the correctness backbone
- Gossip and Flight are performance optimisations, not requirements

**Behavioral Model (complete 20 behaviors):**
- All 12 honeybee behaviors (waggle dance, three-tier workers, pheromones, etc.)
- All 8 cross-species behaviors (tiered storage, buzz pollination, adversarial testing, etc.)
- Sweat bee variable sociality: adaptive coordination intensity based on scale

**Storage:**
- Multi-format cells (Arrow IPC for hot data, Parquet for cold)
- Nectar ripening: streaming write path with local buffer and staged maturation
- Carpenter bee embedded indexes (bloom filters, zone maps)
- Orchid bee metadata enrichment (self-building data catalog)
- Stingless bee tiered cell classification (brood/honey/pollen)
- Time travel queries (VERSION AS OF, TIMESTAMP AS OF)
- Schema evolution (additive)
- DELETE / UPDATE via copy-on-write rewrite

**Query:**
- Full DataFusion CatalogProvider / TableProvider integration
- Distributed joins via Arrow Flight shuffles
- Broadcast join optimisation (pollen pot cells cached everywhere)
- Window functions distributed execution
- Speculative execution for latency-sensitive queries

**Operations:**
- Full interactive CLI with REPL, import, and registry management
- Observability: metrics, tracing, query history
- Access control: per-frame permissions, row-level security
- Guard bee authentication for node membership
- Rolling upgrades
- Winter cluster: offline operation with local write-ahead log

### Success Criteria

- Streaming sensor data ingested via nectar ripening, queryable within seconds
- Arrow Flight shuffles reduce distributed join latency by 10x vs S3
- Gossip failure detection under 3 seconds
- Full behavioral model visibly self-regulating under load
- Waggle dance quality metrics influence task routing (measurable in EXPLAIN)
- Pheromone signals steer planner away from stressed nodes
- Cuckoo bee adversarial testing detects simulated intrusion
- Colony temperature regulation visible in operational dashboards

---

## v3 — Enterprise and Federation

### Goal

Multi-apiary federation, regulatory compliance integration, and enterprise-grade operations.

### What Ships

- Multi-apiary federation (apiaries coordinate across organizational boundaries)
- Regulatory compliance integration (GDPR, HIPAA residency constraints)
- Data lineage and provenance tracking
- Materialized views and incremental maintenance
- User-defined functions (UDFs) in Python
- Advanced access control (column-level, row-level, attribute-based)
- Multi-region deployment with geo-aware data placement
- Adaptive query optimisation (learn from execution history)

---

## Phase Summary

| Capability | v1 | v2 | v3 |
|---|---|---|---|
| Rust runtime + Python SDK | ✓ | ✓ | ✓ |
| SQL via DataFusion | ✓ | ✓ | ✓ |
| Namespace (Hive/Box/Frame) | ✓ | ✓ | ✓ |
| Object storage backend | ✓ | ✓ | ✓ |
| Ledger-based ACID (conditional writes) | ✓ | ✓ | ✓ |
| Parquet cells | ✓ | ✓ | ✓ |
| Heartbeat-based coordination | ✓ | ✓ | ✓ |
| Cache-aware distributed execution | ✓ | ✓ | ✓ |
| Mason bee isolation | ✓ | ✓ | ✓ |
| Leafcutter bee sizing | ✓ | ✓ | ✓ |
| Abandonment + Colony temperature | ✓ | ✓ | ✓ |
| EXPLAIN / EXPLAIN ANALYZE | ✓ | ✓ | ✓ |
| SWIM gossip | | ✓ | ✓ |
| Arrow Flight | | ✓ | ✓ |
| Full behavioral model (20/20) | | ✓ | ✓ |
| Streaming writes (nectar ripening) | | ✓ | ✓ |
| Multi-format cells | | ✓ | ✓ |
| Embedded indexes | | ✓ | ✓ |
| Time travel | | ✓ | ✓ |
| DELETE / UPDATE | | ✓ | ✓ |
| Distributed joins | | ✓ | ✓ |
| Access control | | ✓ | ✓ |
| Full interactive CLI | | ✓ | ✓ |
| Observability | | ✓ | ✓ |
| Offline mode (winter cluster) | | ✓ | ✓ |
| Multi-apiary federation | | | ✓ |
| Regulatory compliance | | | ✓ |
| Data lineage | | | ✓ |
| Advanced access control | | | ✓ |
| Adaptive optimisation | | | ✓ |

---

## Build Order Within v1

```
1. Rust skeleton + PyO3 bridge + StorageBackend trait
   LocalBackend + S3Backend, core types, NodeConfig

2. Registry + namespace
   Hive/Box/Frame, persisted to storage backend via conditional writes

3. Ledger + cell storage
   Transaction ledger, Parquet cell writing/reading, cell stats,
   leafcutter sizing, conditional put for commit serialisation

4. DataFusion integration
   ListingTable from ledger-resolved URIs, cell stat pruning,
   USE/SHOW/DESCRIBE, schema validation
   ← SOLO MODE COMPLETE

5. Mason bee isolation + BeePool
   Memory budgets, sealed chambers, task queueing, concurrent execution

6. Heartbeat table + world view
   Heartbeat writer, world view builder, node discovery, failure detection
   ← MULTI-NODE AWARE

7. Distributed query execution
   Cache-aware load balancer, SQL fragment generation,
   partial results via S3, coordinator merge, EXPLAIN/EXPLAIN ANALYZE

8. Local cell cache
   LRU cache, cache-aware planning, write buffering

9. Colony temperature + compaction + abandonment
   Temperature measurement, write backpressure, compaction policy,
   task abandonment with diagnostics

10. Testing and hardening
    Integration tests, chaos tests, benchmarks, documentation
```

**Milestone checkpoints:**

After Step 4: Demo solo mode — single Pi, create/write/query via Python and SQL.
After Step 7: Demo distributed mode — 3 nodes, distributed queries via shared MinIO.
After Step 9: Demo self-regulation — visible temperature changes under load.
After Step 10: v1 release candidate.

---

## Community and Open Source Strategy

**v1 launch:** Open source the core runtime. The demo is a stack of Pis querying data from a shared MinIO bucket. Blog posts, conference talks, video demonstrations.

**Community hook:** The single-binary install experience. `pip install apiary-data`, write data, query via SQL. The barrier to entry is near zero. Connect to S3 and add nodes to scale.

**Commercial value (later):** Enterprise security (guard bees, access control), streaming ingestion (nectar ripening), regulatory compliance, and managed service offerings.

**Conference narrative:** "We built a lakehouse that runs on Raspberry Pis. It stores data in S3, coordinates through the bucket, and manages compute using bee swarm intelligence. Install it, write Python, query SQL. Each £35 computer runs a colony of agents that self-regulate memory, work sizing, and failure recovery. Add more Pis and they join the swarm automatically — they just need to see the same bucket."
