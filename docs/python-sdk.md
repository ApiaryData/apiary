# Python SDK Reference

## Installation

```bash
pip install maturin
maturin develop
```

## Apiary Class

### Constructor

```python
Apiary(name: str, storage: str | None = None)
```

Create an Apiary instance.

- **name** — Logical name for this apiary (used as root namespace).
- **storage** — Storage URI. Defaults to local filesystem. Use `"s3://bucket/path"` for S3-compatible storage.

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
