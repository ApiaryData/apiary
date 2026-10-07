# Python SDK Reference

## Installation

```bash
pip install maturin
maturin develop
```

## Apiary Class

### Constructor

```python
Apiary(name: str, storage: str | None = None, harvest: str | None = None,
       retention_seconds: int | None = None)
```

Create an Apiary instance.

- **name** — Logical name for this apiary (used as root namespace).
- **storage** — Storage URI. Defaults to local filesystem. Use `"s3://bucket/path"` for S3-compatible storage.
- **harvest** — Storage URI for harvest tables, normally an `s3://` bucket. Capped cells are copied there. The store must support conditional writes (AWS S3, Cloudflare R2 and MinIO do); a store that does not is refused. Unset means nothing is harvested.
- **retention_seconds** — How long a harvested cell stays on the local store before it is retired from it. Unset keeps everything. A retired cell lives on only in the harvest, and queries do not read the harvest yet, so leave this unset unless a downstream system reads the harvest.

```python
from apiary import Apiary

# Local filesystem (solo mode)
ap = Apiary("my_project")

# S3-compatible storage (multi-node capable)
ap = Apiary("production", storage="s3://my-bucket/apiary")
```

### Lifecycle

#### `start()`

Initialize the node: detect hardware, start bee pool, begin heartbeat writer, start worker poller.

```python
ap.start()
```

#### `shutdown()`

Gracefully stop the node: drain tasks, stop heartbeat, clean up resources.

```python
ap.shutdown()
```

---

## Namespace Operations

### Create

```python
create_hive(name: str) -> None
create_box(hive: str, name: str) -> None
create_frame(hive: str, box_name: str, name: str, schema: dict, partition_by: list[str] | None = None) -> None
```

Traditional aliases: `create_database()`, `create_schema()`, `create_table()` (same signatures).

```python
ap.create_hive("warehouse")
ap.create_box("warehouse", "sales")
ap.create_frame("warehouse", "sales", "orders", {
    "order_id": "int64",
    "customer": "utf8",
    "amount": "float64",
    "region": "utf8",
}, partition_by=["region"])
```

Supported schema types: `int64`, `float64`, `utf8`, `boolean`, `date32`, `timestamp`.

### List

```python
list_hives() -> list[str]
list_boxes(hive: str) -> list[str]
list_frames(hive: str, box_name: str) -> list[str]
```

Traditional aliases: `list_databases()`, `list_schemas()`, `list_tables()`.

```python
ap.list_hives()                        # ["warehouse"]
ap.list_boxes("warehouse")             # ["sales"]
ap.list_frames("warehouse", "sales")   # ["orders"]
```

### Get Metadata

```python
get_frame(hive: str, box_name: str, name: str) -> dict
```

Traditional alias: `get_table()`.

Returns frame metadata including schema, partition columns, cell count, row count, and byte size.

```python
info = ap.get_frame("warehouse", "sales", "orders")
# {
#   "name": "orders",
#   "schema": {"order_id": "int64", "customer": "utf8", ...},
#   "partition_by": ["region"],
#   "cell_count": 3,
#   "row_count": 1500,
#   "total_bytes": 24576
# }
```

---

## Data Operations

### Write

```python
write_to_frame(hive: str, box_name: str, frame_name: str, ipc_data: bytes) -> dict
```

Append data to a frame. Input is Arrow IPC stream bytes. Returns a write result with cell count and row count.

```python
import pyarrow as pa

table = pa.table({
    "order_id": [1, 2, 3],
    "customer": ["alice", "bob", "alice"],
    "amount": [100.0, 250.0, 75.0],
    "region": ["us", "eu", "us"],
})

sink = pa.BufferOutputStream()
writer = pa.ipc.new_stream_writer(sink, table.schema)
writer.write_table(table)
writer.close()

result = ap.write_to_frame("warehouse", "sales", "orders", sink.getvalue().to_pybytes())
# {"cells_written": 2, "rows_written": 3}
```

### Ingest

```python
ingest(hive: str, box_name: str, frame_name: str, ipc_data: bytes) -> dict
flush_crop() -> dict
```

`ingest` is the streaming path. It lands the batch in the node's **crop**, on the node's own disk, and returns once the batch is synced there. The rows are queryable at once and show `_stage = 'crop'`; every deposit interval (10 seconds by default) the node commits them to the frame's Delta table, after which they show `_stage = 'comb'`.

Unlike `write_to_frame`, which commits before it returns, ingested rows exist only on this node until they are deposited. The crop survives restarts: rows ingested before a crash are deposited by the next run. `shutdown()` deposits whatever is left.

The batch is checked against the frame's schema first; one that does not fit is refused and nothing is written.

```python
result = ap.ingest("warehouse", "sales", "orders", ipc_bytes)
# {"rows": 3, "segment": 1, "crop_bytes": 2048}

ap.flush_crop()    # deposit now instead of waiting for the interval
# {"frames": 1, "segments": 1, "rows": 3}
```

Use `write_to_frame` for bulk loads that should be in the comb when the call returns, and `ingest` for a stream of small batches.

### Read

```python
read_from_frame(hive: str, box_name: str, frame_name: str, partition_filter: dict | None = None) -> bytes
```

Read data from a frame as Arrow IPC bytes. Optional partition filter for pruning.

```python
data = ap.read_from_frame("warehouse", "sales", "orders")
reader = pa.ipc.open_stream(data)
table = reader.read_all()

# With partition pruning
data = ap.read_from_frame("warehouse", "sales", "orders", partition_filter={"region": "us"})
```

### Overwrite

```python
overwrite_frame(hive: str, box_name: str, frame_name: str, ipc_data: bytes) -> dict
```

Atomically replace all data in a frame. Old cells are removed, new cells are written.

```python
result = ap.overwrite_frame("warehouse", "sales", "orders", sink.getvalue().to_pybytes())
```

---

## Ripening, Capping and Harvest

Data matures in stages. A deposit writes **nectar** cells (small, as they arrive). **Capping** merges a frame's nectar into standard-size **capped** cells and seals them: a capped cell never changes. **Harvest** copies capped cells to the harvest store. **Clearing** deletes the files capping replaced.

The node does all of this in the background, on the intervals in its config (`cap_interval`, `harvest_interval`, `clear_interval`). The calls below do one pass now.

### Recipe

```python
set_recipe(hive, box_name, frame_name, sort_by=None, dedup_by=None) -> None
recipe(hive, box_name, frame_name) -> dict
```

A frame's recipe says how it ripens, and is stored in the frame's Delta table properties (`apiary.ripen.sort_by`, `apiary.ripen.dedup_by`).

- **sort_by** — columns the rows are sorted by, ascending, nulls last.
- **dedup_by** — columns that identify a duplicate. Of rows sharing these values, the latest one to arrive wins.

The recipe is applied when the crop is deposited and again when cells are capped. Deduplication covers the rows merged together in one pass, not the whole table, so a duplicate that arrives long after its original is removed only if both are still nectar.

```python
ap.set_recipe("farm", "field", "readings", sort_by=["id"], dedup_by=["id"])
```

### Cap

```python
cap() -> dict
# {"nectar_cells": 2, "capped_cells": 1, "rows": 5, "aborted": 0}
```

Merges all nectar, grouped by partition, up to the standard cell size, ripens it with the recipe and seals it, in commits that change no data. The background pass is gentler: it waits for a group to fill half a standard cell, or for its oldest cell to be `cap_max_age` old (10 minutes by default).

Ripening yields to users. If a delete or overwrite touches the same cells, that group is abandoned (`aborted`) and the user's write stands.

### Harvest

```python
harvest() -> dict
# {"cells": 1, "bytes": 2048, "remaining": 0}
```

Copies capped cells, oldest first, into a Delta table of the same name on the harvest store. Only capped cells are harvested, so a refinery downstream never sees small unsorted files. A cell already harvested is skipped, so a pass that died halfway is simply repeated. Each pass copies at most `harvest_batch_bytes` per frame (1 GiB by default), which paces the uplink; `remaining` counts what is left. Raises an error if the node has no `harvest`.

### Clear

```python
clear() -> dict
# {"retired": 0, "deleted": 2}
```

Deletes the files no table version needs, once they are older than `clear_grace` (1 hour by default, so a query on an earlier version and a write in flight stay safe). If `retention_seconds` is set, first retires harvested cells that are past it. A cell that is not in the harvest is never retired, however old.

---

## SQL

```python
sql(query: str) -> bytes
```

Execute a SQL query. Returns Arrow IPC stream bytes. See [SQL Reference](sql-reference.md) for supported syntax.

```python
result_bytes = ap.sql("SELECT customer, SUM(amount) FROM warehouse.sales.orders GROUP BY customer")

reader = pa.ipc.open_stream(result_bytes)
table = reader.read_all()
print(table.to_pandas())
```

Custom commands (USE, SHOW, DESCRIBE) also return Arrow IPC with result metadata.

```python
ap.sql("USE HIVE warehouse")
ap.sql("USE BOX sales")
result = ap.sql("SELECT * FROM orders LIMIT 10")
```

---

## Status & Monitoring

### Node Status

```python
status() -> dict
```

```python
s = ap.status()
# {
#   "node_id": "abc123",
#   "cores": 4,
#   "memory_gb": 3.7,
#   "state": "running"
# }
```

### Bee Status

```python
bee_status() -> list[dict]
```

Returns per-bee (per-core) information: memory budget, current utilization, task state.

```python
bees = ap.bee_status()
for bee in bees:
    print(f"Bee {bee['bee_id']}: {bee['state']} — {bee['memory_used_mb']:.0f}/{bee['memory_budget_mb']:.0f} MB")
```

### Swarm Status

```python
swarm_status() -> dict
```

Returns the full swarm view: all discovered nodes, their state (alive/suspect/dead), and aggregate capacity.

```python
swarm = ap.swarm_status()
print(f"Nodes alive: {swarm['alive']}, Total bees: {swarm['total_bees']}")
for node in swarm['nodes']:
    print(f"  {node['node_id']}: {node['state']}")
```

### Colony Status

```python
colony_status() -> dict
```

Returns the biological model state: colony temperature, regulation classification, and abandonment stats.

```python
colony = ap.colony_status()
print(f"Temperature: {colony['temperature']:.2f}")
print(f"Regulation: {colony['regulation']}")  # "ideal", "warm", "hot", etc.
print(f"Abandoned tasks: {colony['abandoned_tasks']}")
```
