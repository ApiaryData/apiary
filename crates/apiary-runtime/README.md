# apiary-runtime

Node runtime, bee pool execution, and swarm coordination for the [Apiary](https://github.com/ApiaryData/apiary) distributed data processing framework.

## Overview

`apiary-runtime` manages the lifecycle of an Apiary compute node and coordinates work across a swarm:

- **ApiaryNode** — Main entry point for starting and managing a compute node
- **BeePool** — Task queue with isolated execution chambers; each CPU core is one "bee" with its own memory budget
- **Heartbeat & WorldView** — Background heartbeat writer and node discovery via storage polling; detects alive, suspect, and dead nodes
- **CellCache** — LRU cache for Parquet cells with configurable size limits (default 2 GB)
- **Behavioral model** — Colony temperature measurement, temperature regulation (cold/ideal/warm/hot/critical), and task abandonment tracking

## Usage

```rust
use apiary_runtime::ApiaryNode;

// Start a node with local storage
let node = ApiaryNode::start("/tmp/apiary-data").await?;

// The node automatically:
//   - Detects hardware (cores, memory)
//   - Creates a bee pool (1 bee per core)
//   - Begins heartbeat broadcasting
//   - Discovers other nodes via storage
```

## Key Concepts

### Mason Bee Isolation

Each bee (CPU core) runs inside a sealed "mason chamber" with a fixed memory budget. If a task exceeds its budget, the chamber terminates the task without affecting other bees.

### Swarm Coordination

Nodes discover each other by reading heartbeat files from shared storage. There are no seed nodes, tokens, or gossip protocols — connecting to the same storage bucket is all that's needed.

### Colony Temperature

The behavioral model measures system health as a temperature:

| Temperature | Meaning |
|-------------|---------|
| Cold | Under-utilized, capacity available |
| Ideal | Healthy load |
| Warm | Approaching limits |
| Hot | High pressure, backpressure active |
| Critical | At risk of failure |

## License

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.

Part of the [Apiary](https://github.com/ApiaryData/apiary) project.
