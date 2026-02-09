# Query Execution

## Overview

A SQL query in Apiary follows a simple path: resolve the frame name to a set of cells in object storage, prune cells using partition values and statistics, assign cells to nodes based on cache locality and capacity, execute in parallel, and merge the results.

The query engine is Apache DataFusion — a Rust-native, Arrow-native SQL engine. Apiary's contribution is connecting DataFusion to the storage layer (resolving frame names to Parquet files) and distributing execution across the swarm when multiple nodes are available.

---

## Single-Node Query Path

On a single node (solo mode or when the planner determines distribution is not worthwhile), the query executes entirely within DataFusion:

```
1. PARSE
   DataFusion parses SQL into a LogicalPlan.

2. RESOLVE
   Apiary resolves frame names to cell files:
   - Read the frame's ledger from storage backend
   - Determine active cells
   - Apply partition pruning (eliminate partitions that cannot match WHERE)
   - Apply cell stat pruning (eliminate cells whose min/max cannot match WHERE)

3. REGISTER
   Register the surviving cells as a DataFusion ListingTable.
   DataFusion's built-in object_store integration handles S3 access.

4. EXECUTE
   DataFusion executes the query:
   - Projection pushdown (read only needed columns from Parquet)
   - Filter pushdown (push predicates into Parquet reader)
   - Standard execution: scan, filter, aggregate, sort, limit

5. RETURN
   Results as Arrow RecordBatches → pyarrow Table → user
```

### Integration with DataFusion

For v1, Apiary uses DataFusion's built-in capabilities rather than implementing custom catalog traits:

```rust
pub struct ApiaryQueryContext {
    session: SessionContext,
    storage: Arc<dyn StorageBackend>,
    registry: Registry,
    cache: Arc<CellCache>,
}

impl ApiaryQueryContext {
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        // Handle custom commands (USE, SHOW, DESCRIBE)
        if let Some(result) = self.handle_custom_command(query)? {
            return Ok(result);
        }
        
        // Extract table references from the query
        // For each referenced frame:
        //   1. Resolve via registry
        //   2. Read ledger, get active cells
        //   3. Apply pruning
        //   4. Register remaining cells as a ListingTable
        self.register_frames_for_query(query).await?;
        
        // Execute via DataFusion
        let df = self.session.sql(query).await?;
        df.collect().await
    }
}
```

DataFusion's `object_store` integration provides transparent access to S3, GCS, Azure, and local filesystem. Apiary registers the appropriate object store based on the `StorageBackend` configuration.

### Custom SQL Commands

Apiary intercepts and handles custom commands before they reach DataFusion:

```sql
USE HIVE analytics;              -- set default hive
USE BOX sensors;                 -- set default box
SHOW HIVES;                      -- list all hives
SHOW BOXES IN analytics;         -- list boxes in a hive
SHOW FRAMES IN analytics.sensors; -- list frames in a box
DESCRIBE analytics.sensors.temp;  -- show schema, partitions, cell count, total size
```

These are parsed with simple string matching (not a full SQL parser extension) and return results as Arrow tables.

---

## Distributed Query Path

When the world view shows multiple alive nodes, the query planner considers distributing the work. The decision is straightforward: if the cells to scan exceed a single bee's capacity, or if distributing would reduce wall-clock time by leveraging cached cells on other nodes, the query is distributed.

### Planning: Cache-Aware Load Balancer

The planner is a function, not a subsystem. Given cells to scan and available nodes, it assigns cells to nodes:

```rust
pub fn plan_query(
    cells: Vec<CellInfo>,
    nodes: Vec<NodeInfo>,       // from world view
    local_node: NodeId,
) -> QueryPlan {
    
    // 1. If all cells fit in one bee's memory budget and we have an idle bee, run locally
    let total_size: u64 = cells.iter().map(|c| c.bytes).sum();
    let local = nodes.iter().find(|n| n.node_id == local_node).unwrap();
    if total_size < local.memory_per_bee && local.idle_bees > 0 {
        return QueryPlan::Local { cells };
    }
    
    // 2. Assign cells to nodes using cache locality and capacity
    let mut assignments: HashMap<NodeId, Vec<CellInfo>> = HashMap::new();
    
    for cell in &cells {
        // Prefer a node that has this cell cached
        if let Some(caching_node) = find_caching_node(&cell, &nodes) {
            if has_capacity(&caching_node) {
                assignments.entry(caching_node.node_id).or_default().push(cell.clone());
                continue;
            }
        }
        
        // Otherwise, assign to the node with most idle capacity
        let best = nodes.iter()
            .filter(|n| has_capacity(n))
            .max_by_key(|n| n.idle_bees);
        if let Some(node) = best {
            assignments.entry(node.node_id).or_default().push(cell.clone());
        }
    }
    
    // 3. Check leafcutter sizing: split assignments that exceed a bee's budget
    for (node_id, cells) in &mut assignments {
        let node = nodes.iter().find(|n| n.node_id == *node_id).unwrap();
        *cells = leafcutter_split(cells, node.memory_per_bee);
    }
    
    QueryPlan::Distributed { assignments }
}
```

### SQL Fragment Generation

For distributed queries, the planner generates per-node SQL fragments rather than serialising physical plans. Each node has a full DataFusion engine and can parse SQL independently.

**Aggregation queries** are decomposed into partial and merge phases:

```sql
-- Original
SELECT region, avg(temp) FROM temperature GROUP BY region

-- Per-node fragment (partial aggregation)
SELECT region, sum(temp) as _sum, count(temp) as _count
FROM __scan__ GROUP BY region

-- Coordinator merge (final aggregation)
SELECT region, sum(_sum) / sum(_count) as avg_temp
FROM __partials__ GROUP BY region
```

**Simple scans** send the original WHERE clause to each node:

```sql
-- Original
SELECT * FROM temperature WHERE temp > 40.0 AND region = 'north'

-- Per-node fragment (same query, different cells)
SELECT * FROM __scan__ WHERE temp > 40.0
-- (region = 'north' was already handled by partition pruning)
```

**Supported decompositions in v1:**

| Query Pattern | Decomposition |
|---|---|
| Full scan | Split cells across nodes, concatenate results |
| Filtered scan | Same filter on each node, concatenate results |
| GROUP BY with SUM/COUNT/MIN/MAX | Partial aggregation per node, merge partials |
| GROUP BY with AVG | Decompose to SUM + COUNT, recompute AVG |
| ORDER BY with LIMIT | Per-node ORDER BY + LIMIT, merge + re-sort + re-limit |

**Not supported for distribution in v1:**
- Joins (execute locally on coordinator)
- Window functions (execute locally)
- Subqueries (execute locally)

When a query pattern cannot be decomposed, it runs on the coordinator node alone.

### Execution Protocol

```
Coordinator (the node that received the SQL query):

1. Plan: resolve cells, prune, assign to nodes
2. Write query manifest to:
   {storage_root}/_queries/{query_id}/manifest.json
3. Execute local tasks (cells assigned to self)
4. Write own partial result to:
   _queries/{query_id}/partial_{self_node_id}.arrow
5. Poll for other nodes' partial results:
   _queries/{query_id}/partial_{other_node_id}.arrow
6. When all partials are present:
   Read partials, merge, return final result
7. Clean up: delete _queries/{query_id}/ directory

Workers (other nodes):

1. Poll _queries/ prefix for manifests with tasks assigned to self
   (default poll interval: 500ms)
2. For each assigned task:
   a. Read assigned cells from storage (or cache)
   b. Register as temporary table __scan__
   c. Execute SQL fragment
   d. Write result to:
      _queries/{query_id}/partial_{self_node_id}.arrow
```

### Task Timeout and Retry

Each query has a timeout (default: 60 seconds). If a partial result has not appeared within the timeout:

1. Check the world view — is the assigned node still alive?
2. If the node is dead: reassign the task to another node, update the manifest
3. If the node is alive but slow: extend the timeout or abort the query

Failed tasks follow the abandonment behavior (Document 5): after a configurable number of retries (default: 3), the task is abandoned and the query fails with an error explaining which node and cells were involved.

### Result Format

Partial results are written as Arrow IPC files (`.arrow`) to object storage. Arrow IPC is used rather than Parquet because:
- Partial results are temporary (read once, then deleted)
- Arrow IPC is faster to write and read (no columnar encoding overhead)
- Arrow IPC preserves the exact schema without type inference

---

## Query Explanation

### EXPLAIN

Shows the query plan and the swarm state that informed it:

```sql
EXPLAIN SELECT region, avg(temp) FROM temperature GROUP BY region;
```

```
Query Plan:
  Type: distributed (3 cells across 2 nodes)
  
  Pruning:
    Partitions: 3 of 3 included (no partition filter)
    Cells: 5 of 8 after stat pruning (temp min/max)
  
  Assignment:
    pi-factory-03: 3 cells (384MB) — 2 cached, 1 fetch
      Reason: cache locality for 2 cells, idle capacity
    pi-factory-07: 2 cells (256MB) — 0 cached, 2 fetch
      Reason: idle capacity, load balancing

  Fragment: SELECT region, sum(temp), count(temp) GROUP BY region
  Merge: SELECT region, sum(_sum)/sum(_count) as avg_temp GROUP BY region
```

### EXPLAIN ANALYZE

Executes the query and reports what actually happened:

```sql
EXPLAIN ANALYZE SELECT region, avg(temp) FROM temperature GROUP BY region;
```

```
Execution Report:
  Total time: 1.84s
  
  Task on pi-factory-03: 1.12s
    Cells scanned: 3 (384MB)
    Cache hits: 2 (256MB from cache, 128MB from S3)
    S3 fetches: 1 (128MB, 0.45s)
    Peak memory: 280MB / 1024MB budget
    Rows processed: 2,400,000
    
  Task on pi-factory-07: 1.84s
    Cells scanned: 2 (256MB)
    Cache hits: 0 (256MB from S3)
    S3 fetches: 2 (256MB, 0.92s)
    Peak memory: 190MB / 1024MB budget
    Rows processed: 1,600,000
    
  Merge: 0.01s (4 rows merged)
  S3 coordination overhead: 0.38s (manifest write + result poll)
  
  Result: 3 rows
```

This gives the user complete visibility into why a query was fast or slow: which cells were cached, how much data was fetched from S3, which node was the bottleneck, and how much of the time was spent on coordination overhead versus actual computation.

---

## Design Rationale

### Why SQL Fragments Instead of Physical Plan Serialisation?

DataFusion's `PhysicalPlan` contains closures, Arc references, and runtime state that cannot be serialised across the network. Approaches like Substrait exist for plan serialisation, but they add complexity and are still maturing.

SQL fragments are simple, robust, and self-describing. Each node has a full DataFusion engine and can parse and optimise SQL independently. The coordinator's job is decomposition (what to compute where) and merging (how to combine partial results), not shipping compiled execution plans.

The tradeoff is that each node re-parses and re-plans its fragment locally, adding a few milliseconds of overhead. For batch queries that scan megabytes to gigabytes of data, this overhead is negligible.

### Why S3 for Shuffles Instead of Arrow Flight?

Arrow Flight provides low-latency, high-throughput data transfer directly between nodes. But it requires:
- A Flight server on every node
- TCP connectivity between all nodes
- Connection management, authentication, and retry logic

For v1, partial results are small (aggregated data) and temporary. Writing them to S3 and reading them back adds ~200ms of overhead but requires zero node-to-node networking.

v2 introduces Arrow Flight for performance-critical paths: large shuffles (joins), streaming results, and interactive queries where 200ms of coordination overhead is unacceptable.

### Why Not a Full CatalogProvider?

DataFusion's `CatalogProvider` and `TableProvider` traits allow deep integration with the query planning process. But for v1, the simpler approach of registering `ListingTable` instances before each query is sufficient. It avoids the complexity of managing a dynamic catalog that reflects real-time registry changes and concurrent DDL.

v2 implements the full catalog traits for seamless integration with DataFusion's optimizer, enabling features like cross-frame joins with pushed-down predicates and materialized views.
