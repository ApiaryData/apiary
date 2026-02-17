# apiary-python

Python bindings for the [Apiary](https://github.com/ApiaryData/apiary) distributed data processing framework.

## Overview

`apiary-python` provides a Python SDK via [PyO3](https://pyo3.rs/) and [maturin](https://www.maturin.rs/), exposing the full Apiary runtime to Python:

- **`Apiary` class** — Main entry point for creating and managing nodes
- **SQL queries** — Execute SQL and get results as Python dictionaries
- **Data writing** — Write Python dictionaries (lists of column data) to frames
- **Swarm visibility** — Inspect node status, colony temperature, and swarm health
- **Zero-copy interop** — Arrow IPC format for efficient data exchange between Rust and Python

## Installation

```bash
pip install maturin
maturin develop
```

## Usage

```python
from apiary import Apiary

# Start a node with local storage
ap = Apiary("my_database")
ap.start()

# Check system status
status = ap.status()
print(f"Running on {status['cores']} cores with {status['memory_gb']:.2f}GB memory")

# Write data
ap.sql("USE my_database.default")
ap.write("sensors", {
    "sensor_id": ["s1", "s2", "s3"],
    "temperature": [22.5, 23.1, 21.8],
})

# Query data
results = ap.sql("SELECT * FROM sensors WHERE temperature > 22.0")

# Multi-node: connect to the same storage
ap = Apiary("production", storage="s3://my-bucket/apiary")
ap.start()

# Shut down
ap.shutdown()
```

## Python API

| Method | Description |
|--------|-------------|
| `Apiary(name, storage=None)` | Create a new Apiary instance |
| `start()` | Start the node and join the swarm |
| `shutdown()` | Gracefully leave the swarm |
| `status()` | Node hardware and configuration |
| `swarm_status()` | All nodes in the swarm |
| `colony_status()` | Behavioral model (temperature, bee utilization) |
| `sql(query)` | Execute SQL and return results |
| `write(frame, data)` | Write data to a frame |

## License

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.

Part of the [Apiary](https://github.com/ApiaryData/apiary) project.
