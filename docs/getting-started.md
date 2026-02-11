# Getting Started with Apiary

## Prerequisites

- **Rust 1.78+** — [Install via rustup](https://rustup.rs/)
- **Python 3.9+** — with `pip`
- **maturin** — Python/Rust build tool (`pip install maturin`)

## Installation

```bash
# Clone the repository
git clone https://github.com/ApiaryData/apiary.git
cd apiary

# Build the Rust workspace
cargo build --workspace

# Build and install the Python package
pip install maturin
maturin develop
```

Verify the install:

```bash
python -c "from apiary import Apiary; print('Apiary ready')"
```

## Your First Query in 5 Minutes

### 1. Create an Apiary and Write Data

```python
import pyarrow as pa
from apiary import Apiary

ap = Apiary("quickstart")
ap.start()

# Create namespace: hive → box → frame
ap.create_hive("warehouse")
ap.create_box("warehouse", "sales")
ap.create_frame("warehouse", "sales", "orders", {
    "order_id": "int64",
    "customer": "utf8",
    "amount": "float64",
})

# Build an Arrow table and write it
table = pa.table({
    "order_id": [1, 2, 3],
    "customer": ["alice", "bob", "alice"],
    "amount": [100.0, 250.0, 75.0],
})
sink = pa.BufferOutputStream()
writer = pa.ipc.new_stream_writer(sink, table.schema)
writer.write_table(table)
writer.close()

ap.write_to_frame("warehouse", "sales", "orders", sink.getvalue().to_pybytes())
```

### 2. Query with SQL

```python
result_bytes = ap.sql("SELECT customer, SUM(amount) FROM warehouse.sales.orders GROUP BY customer")

# Decode Arrow IPC result
reader = pa.ipc.open_stream(result_bytes)
result = reader.read_all()
print(result.to_pandas())
#   customer  SUM(amount)
# 0    alice        175.0
# 1      bob        250.0

ap.shutdown()
```

## Solo Mode

Solo mode runs on a single machine using the local filesystem. No configuration beyond a name is needed.

```python
from apiary import Apiary

ap = Apiary("my_project")
ap.start()

status = ap.status()
print(f"Bees: {status['cores']}, Memory: {status['memory_gb']:.1f} GB")

# All operations work identically in solo mode
ap.create_hive("analytics")
ap.create_box("analytics", "default")

ap.shutdown()
```

Data is stored under a local directory (the default storage path). Solo mode is a swarm of one — there is no special-casing.

## Multi-Node Mode

Point every node at the same object storage bucket. Nodes discover each other automatically through heartbeat files in storage.

```python
from apiary import Apiary

# Node 1 (e.g., Raspberry Pi in the closet)
ap = Apiary("production", storage="s3://my-bucket/apiary")
ap.start()

# Node 2 (e.g., old laptop on the shelf) — same code, same bucket
ap = Apiary("production", storage="s3://my-bucket/apiary")
ap.start()
```

No seed nodes, no tokens, no mesh configuration. Each node:

1. Writes heartbeats to storage so others can see it.
2. Reads heartbeats to build a world view of the swarm.
3. Picks up distributed query tasks assigned to it.

Check the swarm:

```python
swarm = ap.swarm_status()
print(f"Nodes alive: {swarm['alive']}")
for node in swarm['nodes']:
    print(f"  {node['node_id']}: {node['state']} — {node['cores']} cores")
```

## Next Steps

- [Core Concepts](concepts.md) — understand hives, bees, and the biological model
- [Python SDK Reference](python-sdk.md) — full API documentation
- [SQL Reference](sql-reference.md) — supported SQL syntax
- [Architecture Summary](architecture-summary.md) — how it all fits together
