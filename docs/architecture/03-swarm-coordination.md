# Swarm Coordination

## Overview

Apiary nodes coordinate through object storage. There is no gossip protocol, no consensus algorithm, and no direct node-to-node communication. Nodes discover each other, share their status, and detect failures through files in the shared storage bucket.

This design eliminates an entire class of distributed systems problems: NAT traversal, mesh networking, gossip protocol tuning, consensus leader election, split brain, and network partitions between nodes. The only network requirement is that every node can reach the storage endpoint.

---

## The Heartbeat Table

Each node writes a heartbeat file to object storage at a regular interval (default: 5 seconds). The heartbeat contains the node's identity, capacity, current load, cache contents, and the timestamp.

### Heartbeat File

```
{storage_root}/_heartbeats/node_{node_id}.json
```

```json
{
  "node_id": "pi-factory-03",
  "timestamp": "2025-02-08T14:30:05Z",
  "version": 142,
  
  "capacity": {
    "cores": 4,
    "memory_total_bytes": 4294967296,
    "memory_per_bee": 1073741824,
    "target_cell_size": 268435456
  },
  
  "load": {
    "bees_total": 4,
    "bees_busy": 2,
    "bees_idle": 2,
    "memory_pressure": 0.45,
    "queue_depth": 1,
    "colony_temperature": 0.42
  },
  
  "cache": {
    "size_bytes": 1073741824,
    "entries": [
      {"frame": "analytics.sensors.temperature", "cells": 8, "bytes": 524288000},
      {"frame": "analytics.sensors.humidity", "cells": 3, "bytes": 201326592}
    ]
  }
}
```

### Writing Heartbeats

Each node runs a background task that writes its heartbeat file every `heartbeat_interval` seconds. The write uses a regular `put` (not conditional) — each heartbeat overwrites the previous one. The `version` field is incremented monotonically so readers can detect stale heartbeats.

```rust
pub struct HeartbeatWriter {
    storage: Arc<dyn StorageBackend>,
    node_id: NodeId,
    interval: Duration,          // default 5s
}

impl HeartbeatWriter {
    pub async fn run(&self) {
        loop {
            let heartbeat = self.collect_heartbeat();
            let key = format!("_heartbeats/node_{}.json", self.node_id);
            self.storage.put(&key, heartbeat.to_json()).await;
            sleep(self.interval).await;
        }
    }
}
```

### Reading the World View

Each node periodically reads the heartbeat table to build its world view:

```rust
pub struct WorldViewBuilder {
    storage: Arc<dyn StorageBackend>,
    poll_interval: Duration,      // default 5s
    dead_threshold: Duration,     // default 30s (6 missed heartbeats)
}

pub struct WorldView {
    pub nodes: HashMap<NodeId, NodeStatus>,
    pub updated_at: DateTime<Utc>,
}

pub struct NodeStatus {
    pub node_id: NodeId,
    pub heartbeat: Heartbeat,
    pub state: NodeState,
}

pub enum NodeState {
    Alive,                  // heartbeat is recent
    Suspect,                // heartbeat is stale (> dead_threshold / 2)
    Dead,                   // heartbeat is very stale (> dead_threshold)
}
```

**Building the view:**

```
1. List all files under _heartbeats/ prefix
2. Read each heartbeat file
3. For each heartbeat:
   - If timestamp is within dead_threshold → Alive
   - If timestamp is stale but within 2x dead_threshold → Suspect
   - If timestamp is very stale → Dead
4. Nodes with no heartbeat file are unknown (not in the swarm)
```

### Failure Detection

A node is considered dead when its heartbeat has not been updated for `dead_threshold` seconds (default: 30s). This is intentionally conservative — if a node is temporarily slow to write its heartbeat (S3 latency spike, GC pause), it should not be prematurely marked as dead.

The failure detection latency is `heartbeat_interval + poll_interval + dead_threshold` in the worst case — about 40 seconds with defaults. This is slower than SWIM gossip (~2 seconds) but acceptable for batch workloads. If a node dies during a query, the query coordinator detects the missing result after a task timeout and reassigns the work.

### Node Lifecycle

**Joining the swarm:**

```
1. Node starts and writes its first heartbeat to _heartbeats/node_{id}.json
2. That's it. The node is now in the swarm.
3. Other nodes discover it on their next poll of the heartbeat table.
```

No seed nodes. No join handshake. No token exchange. A node joins by writing a heartbeat and leaves by stopping.

**Graceful departure:**

```
1. Node stops accepting new tasks
2. Waits for in-progress tasks to complete
3. Deletes its heartbeat file: delete(_heartbeats/node_{id}.json)
4. Stops
```

Other nodes see the heartbeat disappear on their next poll and remove the node from their world view.

**Crash:**

```
1. Node crashes — no graceful shutdown
2. Heartbeat file remains but is never updated
3. Other nodes see the stale timestamp
4. After dead_threshold: node marked as Dead
5. In-progress tasks on the dead node time out and are reassigned
6. Stale heartbeat file is cleaned up by any node during periodic maintenance
```

---

## Solo Mode

When a node starts with `LocalBackend` and no other nodes have heartbeat files in the storage directory, it operates in solo mode. Solo mode is not a separate code path — it is the natural state of a one-node swarm.

The heartbeat writer still runs (writing to the local filesystem). The world view builder still runs (finding only its own heartbeat). All query planning runs through the same logic — it just happens that all cells are assigned to the only available node.

This means there is zero special-casing for solo mode. The system is always a swarm — sometimes a swarm of one.

---

## Distributed Task Coordination

When a query requires multiple nodes, the coordinator uses the heartbeat table for task assignment and the storage backend for result exchange.

### Query Manifest

The coordinator writes a manifest to object storage describing the work:

```
{storage_root}/_queries/{query_id}/manifest.json
```

```json
{
  "query_id": "q_a1b2c3d4",
  "coordinator": "pi-factory-03",
  "created_at": "2025-02-08T14:30:10Z",
  "timeout_seconds": 60,
  "sql": "SELECT region, avg(temp) FROM analytics.sensors.temperature GROUP BY region",
  
  "tasks": [
    {
      "task_id": "t_001",
      "assigned_node": "pi-factory-03",
      "cells": ["region=north/cell_a1b2.parquet", "region=north/cell_c3d4.parquet"],
      "sql_fragment": "SELECT region, sum(temp) as _sum, count(temp) as _count FROM __scan__ WHERE true GROUP BY region",
      "expected_columns": ["region", "_sum", "_count"]
    },
    {
      "task_id": "t_002",
      "assigned_node": "pi-factory-07",
      "cells": ["region=south/cell_e5f6.parquet"],
      "sql_fragment": "SELECT region, sum(temp) as _sum, count(temp) as _count FROM __scan__ WHERE true GROUP BY region",
      "expected_columns": ["region", "_sum", "_count"]
    }
  ]
}
```

### Task Execution

Each node polls for query manifests assigned to it (or is notified via a lightweight mechanism — see below). When a node finds a task assigned to it:

1. Read the assigned cells from object storage (or local cache)
2. Register cells as a temporary table `__scan__` in a local DataFusion context
3. Execute the SQL fragment
4. Write the partial result to: `_queries/{query_id}/partial_{node_id}.arrow`

### Result Gathering

The coordinator polls for partial result files:

```
1. After writing the manifest, start polling:
   list(_queries/{query_id}/partial_*)
2. When all expected partial files are present:
   Read each partial result
   Merge into final result (e.g., sum the _sums, sum the _counts, compute avg)
   Return to user
3. If a task times out:
   Check if the assigned node is still alive (heartbeat)
   If dead → reassign task to another node, update manifest
   If alive → extend timeout or abort query
```

### Task Notification (Optimisation)

Polling for query manifests is simple but adds latency. An optimisation for v1: nodes poll the `_queries/` prefix at a configurable interval (default: 500ms). This means a distributed query has an additional 0-500ms latency for task discovery, which is acceptable for batch workloads.

For v2, a lightweight notification mechanism (e.g., SQS, a shared Redis pub/sub, or direct HTTP pings) can reduce this to near-instant task discovery.

---

## Cleanup

### Heartbeat Cleanup

Any node can delete heartbeat files for nodes that have been dead for more than `cleanup_threshold` (default: 1 hour). This is a background maintenance task that runs periodically.

### Query Cleanup

After a query completes, the coordinator deletes the manifest and partial result files. A background task also cleans up orphaned query directories (where the coordinator died before cleanup) older than 1 hour.

### Cell Cleanup (Vacuum)

Old cell files (referenced by ledger entries before the latest checkpoint, but not referenced by any entry after) are eligible for deletion. The `apiary vacuum` command performs this cleanup. A background compaction task can also trigger it.

---

## Security

### v1: Storage-Level Access Control

Security in v1 is provided by the storage backend. Access to the bucket is access to the swarm. This is the same security model as any shared data lake.

- **AWS S3:** IAM roles, bucket policies, VPC endpoints
- **MinIO:** Access keys, bucket policies
- **Local filesystem:** File permissions

### v2+: Fine-Grained Access Control

v2 adds Apiary-level access control: per-frame permissions, row-level security, and guard bee authentication for nodes joining the swarm.

---

## Design Rationale

### Why No Gossip?

SWIM gossip provides sub-second failure detection and O(log N) state propagation. But it requires:
- UDP port accessible between all nodes
- NAT traversal or mesh networking for cross-network deployments
- A gossip protocol implementation (complex, subtle correctness requirements)
- Node-to-node network connectivity

The heartbeat table provides the same information (membership, capacity, load) with higher latency but zero node-to-node networking. For batch workloads where queries take seconds to minutes, the additional 5-30 seconds of failure detection latency is acceptable.

### Why No Direct Node-to-Node Communication?

Every direct communication channel between nodes adds complexity: connection management, authentication, retry logic, and failure handling. By routing all communication through object storage, Apiary nodes are fully independent processes that share nothing except a storage bucket.

This has a practical benefit: a node behind a corporate firewall, in a different AWS region, or on a different continent can join the swarm — as long as it can reach the S3 endpoint. No mesh VPN, no SSH tunnels, no port forwarding.

### When Will Direct Communication Be Added?

v2 introduces optional direct communication for performance-critical paths:
- SWIM gossip for sub-second failure detection
- Arrow Flight for low-latency data shuffles (joins, large aggregations)
- Direct task notifications to avoid polling latency

These are additive optimisations. The object-storage coordination layer remains the correctness backbone. Direct communication improves performance but is not required for correctness.
