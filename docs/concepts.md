# Core Concepts

## Namespace Hierarchy

Apiary organizes data in a three-level hierarchy, using beekeeping terminology with traditional database aliases.

```
Hive (Database)
 └── Box (Schema)
      └── Frame (Table)
           └── Cell (Parquet file)
```

| Apiary Term | Database Term | Description |
|-------------|---------------|-------------|
| **Hive**    | Database      | Top-level grouping. Isolated namespace boundary. |
| **Box**     | Schema        | Logical grouping within a hive. |
| **Frame**   | Table         | A queryable dataset with a defined Arrow schema. |
| **Cell**    | Parquet file  | The physical storage unit. One frame contains many cells. |

A fully qualified table reference is `hive.box.frame`. SQL also supports `USE HIVE` and `USE BOX` to set context for shorter names.

Both naming conventions work in the SDK:

```python
ap.create_hive("warehouse")      # or ap.create_database("warehouse")
ap.create_box("warehouse", "raw") # or ap.create_schema("warehouse", "raw")
```

## Bees and the Swarm

| Apiary Term | System Concept | Description |
|-------------|----------------|-------------|
| **Bee**     | CPU core       | One core = one bee. Each bee is an independent worker. |
| **Swarm**   | Compute mesh   | All nodes and their bees, discovered via storage. |
| **Meadow**  | Object storage | S3, MinIO, GCS, or local filesystem where data lives. |

A Raspberry Pi 4 contributes 4 bees. An 8-core laptop contributes 8. The swarm is the union of all bees across all nodes.

Nodes have **no direct communication** in v1. All coordination happens through reading and writing files in object storage:

- **Heartbeats** — each node writes a periodic status file so others know it's alive.
- **Query manifests** — the coordinator writes task assignments; workers poll and execute.
- **Partial results** — workers write Arrow IPC result files; the coordinator merges them.

## Biological Model

The biological model is not a metaphor — it drives runtime behavior through concrete algorithms.

### Mason Bee Sealed Chambers

Each bee (core) runs inside a **sealed chamber**: a memory-isolated execution context with a fixed budget. If a bee's task exceeds its memory budget, the task fails rather than starving other bees.

```
Node memory: 4 GB
Bees: 4
Chamber budget: ~1 GB each (minus OS/runtime overhead)
```

Chambers also get isolated scratch directories for temporary spill files.

### Leafcutter Cell Sizing

Leafcutter bees cut leaf pieces to precise sizes. Similarly, Apiary sizes cells (Parquet files) to fit within a bee's memory budget. When writing data, the system:

1. Estimates the uncompressed Arrow size.
2. Splits into cells that a single bee can process without exceeding its chamber budget.
3. Writes each cell as a separate Parquet file with LZ4 compression.

This ensures every cell can be processed by any bee in the swarm, regardless of how small the hardware.

### Colony Temperature

Colony temperature is a composite health metric ranging from **0.0** (cold) to **1.0** (critical). It combines:

- **Memory pressure** — how full are the bee chambers?
- **Task queue depth** — how backed up is the work queue?
- **Error rate** — how many tasks are failing?
- **Storage latency** — how responsive is the meadow?

| Temperature | Range       | Meaning |
|-------------|-------------|---------|
| Cold        | 0.0 – 0.2  | Underutilized. Resources sitting idle. |
| Ideal       | 0.2 – 0.5  | Healthy. Normal operating range. |
| Warm        | 0.5 – 0.7  | Busy. Approaching capacity. |
| Hot         | 0.7 – 0.9  | Stressed. Backpressure should kick in. |
| Critical    | 0.9 – 1.0  | Overloaded. Shed load or risk failures. |

```python
colony = ap.colony_status()
print(f"Temperature: {colony['temperature']:.2f} ({colony['regulation']})")
```

### Abandonment Tracker

When a bee fails a task, the abandonment tracker decides whether to retry or give up:

- Each task gets a limited number of **trials**.
- Failed tasks are re-queued with a decremented trial count.
- If trials are exhausted, the task is permanently abandoned and the error is reported.

This prevents poison-pill tasks from consuming resources indefinitely.

## Storage Model

Data flows through three tiers:

```
┌─────────────────────────────────────────────┐
│  Tier 1 — Memory                            │
│  Arrow RecordBatches inside bee chambers     │
│  (active computation only)                  │
├─────────────────────────────────────────────┤
│  Tier 2 — Local Disk                        │
│  LRU cell cache + scratch/spill space       │
│  (2 GB default cache, evicted on pressure)  │
├─────────────────────────────────────────────┤
│  Tier 3 — Object Storage (Meadow)           │
│  S3 / MinIO / GCS / local filesystem        │
│  (canonical source of truth)                │
└─────────────────────────────────────────────┘
```

**Object storage is always canonical.** Local disk is a cache — losing a node loses nothing. Nodes are stateless workers that can be added or removed freely.

### Storage Layout

```
meadow/
├── _registry/
│   ├── hives.json
│   └── {hive}/
│       ├── boxes.json
│       └── {box}/
│           └── frames.json
├── _heartbeats/
│   └── {node_id}.json
├── _queries/
│   └── {query_id}/
│       ├── manifest.json
│       └── results/
└── {hive}/{box}/{frame}/
    ├── _ledger/
    │   ├── 000001.json
    │   └── checkpoint.json
    └── cells/
        └── {cell_id}.parquet
```

All metadata, coordination state, and data live in the meadow. A fresh node with access to the bucket can reconstruct the full system state.
