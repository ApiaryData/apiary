# 🐝 Apiary

> A distributed data processing framework inspired by bee colony intelligence, designed for small compute that scales to the cloud.

[![CI](https://github.com/ApiaryData/apiary/actions/workflows/ci.yml/badge.svg)](https://github.com/ApiaryData/apiary/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.78%2B-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)

## What Is Apiary?

Apiary is a **lakehouse for small compute**. It's designed to run on inexpensive hardware — Raspberry Pis, NUCs, old laptops — while scaling seamlessly to cloud compute. Users write data via Python, query via SQL, and the swarm distributes work across available nodes with no central controller.

### Key Features

- **🏡 Runs on Small Hardware**: Designed for Raspberry Pi and similar devices, not cloud-first systems squeezed onto small hardware
- **☁️ Object Storage as Truth**: Data lives in S3, GCS, MinIO, or local filesystem — nodes are stateless workers
- **🤝 No Node-to-Node Communication**: Nodes coordinate through the storage layer itself, eliminating distributed systems complexity
- **🔒 ACID Transactions**: Ledger-based transactions using conditional writes (like Delta Lake)
- **🐝 Biology-Driven Design**: Memory budgets, task sizing, and failure recovery governed by bee-inspired behavioral patterns
- **📊 SQL Queries**: Apache DataFusion powers SQL queries over Parquet files
- **🐍 Python Interface**: PyO3-based SDK with zero-copy Arrow interop
- **🚀 Zero Configuration Multi-Node**: Add nodes by connecting to the same storage bucket — no seed nodes, no tokens

## What Makes Apiary Different

1. **Small-compute-first design**: Not a cloud system squeezed onto a Pi, but a system designed for resource-constrained hardware that scales to the cloud.

2. **Object storage as coordination layer**: No consensus protocol, no gossip network, no node-to-node communication. Nodes coordinate through the same storage that holds the data.

3. **Biology-driven resource management**: Memory budgets, cell sizing, backpressure, and failure recovery are governed by bee-inspired behavioral patterns — not metaphors, but algorithms.

4. **Zero-configuration multi-node**: A second node joins the swarm by connecting to the same storage bucket. No seed nodes, no tokens, no mesh network setup.

5. **Solo to swarm without code changes**: A single Pi with local filesystem storage is a fully functional data platform. Switching to S3 and adding nodes is a configuration change, not an architecture change.

## Quick Start

### Installation

#### Pre-built Binaries (no build required)

```bash
# Linux / macOS (x86_64 or ARM64 / Raspberry Pi)
curl -fsSL https://raw.githubusercontent.com/ApiaryData/apiary/main/scripts/install.sh | bash
```

```powershell
# Windows (PowerShell)
irm https://raw.githubusercontent.com/ApiaryData/apiary/main/scripts/install.ps1 | iex
```

#### Docker

```bash
docker pull ghcr.io/apiarydata/apiary:latest   # or build locally
docker build -t apiary:latest .
```

#### Build from Source

**Prerequisites**: Rust 1.78+, Python 3.9+

```bash
git clone https://github.com/ApiaryData/apiary.git
cd apiary
cargo build --workspace
pip install maturin
maturin develop
```

### Hello Apiary

```python
from apiary import Apiary

# Solo mode (local filesystem)
ap = Apiary("my_database")
ap.start()

# Check status
status = ap.status()
print(f"Running on {status['cores']} cores with {status['memory_gb']:.2f}GB memory")

# Shutdown when done
ap.shutdown()
```

### Multi-Node Mode

```python
from apiary import Apiary

# All nodes connect to the same storage
ap = Apiary("production", storage="s3://my-bucket/apiary")
ap.start()

# Nodes automatically discover each other through the storage layer
# No additional configuration needed!
```

## Architecture Overview

### The Beekeeping Metaphor

| Concept | Apiary Component | Description |
|---------|------------------|-------------|
| **Hive** | Database | Top-level logical grouping |
| **Box** | Schema | Namespace within a hive |
| **Frame** | Table | Queryable dataset |
| **Cell** | Parquet file | Physical storage unit |
| **Bee** | CPU core | Unit of compute (1 core = 1 bee) |
| **Swarm** | Compute mesh | All nodes and their bees |
| **Meadow** | Object storage | Where all data lives |

### Storage Model

```
Tier 1 — Memory: Arrow RecordBatches in bee chambers (active computation)
Tier 2 — Local Disk: Cache + spill + write buffer
Tier 3 — Object Storage: S3/GCS/MinIO/filesystem (canonical truth)
```

### Key Design Principles

- **Object storage is canonical**: All committed data, metadata, and coordination state lives in object storage
- **Conditional writes for serialization**: One write succeeds, the other retries — no Raft needed
- **1 core = 1 bee**: Each virtual core is an independent unit with its own memory budget
- **Biology is the runtime**: Mason bee isolation, leafcutter sizing, abandonment, and colony temperature are not metaphors but algorithms

## Development Status

Apiary is in active development. See [BUILD_STATUS.md](docs/BUILD_STATUS.md) for detailed progress and [BENCHMARKS.md](docs/BENCHMARKS.md) for performance metrics.

| Step | Component | Status |
|------|-----------|--------|
| 1 | Skeleton + StorageBackend | ✅ Complete |
| 2 | Registry + Namespace | ✅ Complete |
| 3 | Ledger + Cell Storage | ✅ Complete |
| 4 | DataFusion Integration | ✅ Complete |
| 5 | Mason Bee Isolation | ✅ Complete |
| 6 | Heartbeat + World View | ✅ Complete |
| 7 | Distributed Query Execution | ✅ Complete |
| 8 | Local Cell Cache | ✅ Complete |
| 9 | Behavioral Model | ✅ Complete |
| 10 | Testing + Hardening | ✅ Complete |

### Current Capabilities (Step 10 Complete — v1 Release Candidate)

- ✅ Rust workspace with 6 crates
- ✅ Python SDK via PyO3
- ✅ LocalBackend (filesystem storage)
- ✅ S3Backend (S3-compatible object storage)
- ✅ StorageBackend trait with atomic operations
- ✅ Node configuration with resource auto-detection
- ✅ Typed identifiers (HiveId, BoxId, FrameId, TaskId, etc.)
- ✅ Registry with DDL operations (create/list hives, boxes, frames)
- ✅ Dual terminology (bee-themed and traditional database naming)
- ✅ Every Frame is a Delta Lake table (`delta-rs`): commits by conditional put, readable by Spark and Databricks
- ✅ Parquet cells (Snappy), tagged `apiary.state` = nectar or capped in the Delta log
- ✅ File statistics and partitioning, with pruning and file skipping on read
- ✅ Ingest lands in a durable per-node crop, queryable at once as `_stage = 'crop'`, and is deposited into the Delta table on a cadence (idempotent across crashes)
- ✅ The `apiary` binary (`apiary node run --config`), a Flight SQL entrance with bulk ingest and DDL actions, an MQTT entrance, Guards that refuse or set aside deposits that do not fit their frame, and `apiary.connect()` in Python
- ✅ Ripening on deposit, capping of nectar into sealed standard-size cells, harvest of capped cells to a second store (refused unless it supports conditional writes), and clearing of unneeded files
- ✅ Leafcutter cell sizing
- ✅ Schema validation (declared schema enforced; null partition rejection)
- ✅ Frame overwrite (atomic cell replacement)
- ✅ Ledger checkpointing
- ✅ SQL queries via Apache DataFusion
- ✅ Custom SQL commands (USE, SHOW, DESCRIBE)
- ✅ Cell pruning from WHERE predicates
- ✅ Projection pushdown via DataFusion
- ✅ Aggregation (GROUP BY, AVG, SUM, COUNT, MIN, MAX)
- ✅ DML blocking (DELETE/UPDATE with clear error messages)
- ✅ Mason bee sealed chambers (memory-budgeted isolated execution per bee)
- ✅ BeePool with task queuing and concurrent execution
- ✅ Task timeout enforcement
- ✅ Scratch directory isolation per bee
- ✅ SQL queries routed through BeePool
- ✅ Heartbeat writer (background task writing node status to storage)
- ✅ World view builder (discovers all nodes via heartbeat polling)
- ✅ Node state detection (Alive, Suspect, Dead based on heartbeat age)
- ✅ Graceful departure (heartbeat file deleted on shutdown)
- ✅ Stale heartbeat cleanup (dead nodes cleaned after threshold)
- ✅ `swarm_status()` Python API for swarm visibility
- ✅ Solo mode works as a swarm of one (zero special-casing)
- ✅ Distributed query planner (cache-aware, capacity-based cell assignment)
- ✅ Query coordinator (manifest writing, task execution, result merging)
- ✅ Worker task poller (background task polling for query manifests)
- ✅ Storage-based coordination (query manifests via object storage)
- ✅ Partial result exchange (Arrow IPC format)
- ✅ Transparent distribution (single-node fallback for small queries)
- ✅ Local cell cache (LRU eviction, 2GB default)
- ✅ Cache reporting in heartbeats (enables cache-aware planning)
- ✅ Cache-aware distributed query planning (preferential assignment)
- ✅ Colony temperature measurement (composite system health metric)
- ✅ Temperature regulation classification (cold/ideal/warm/hot/critical)
- ✅ Task abandonment tracker (retry logic with trial limits)
- ✅ Behavioral model Python API (colony_status method)

- ✅ Integration tests (solo mode, multi-node, mason isolation, concurrent writes, backpressure, chaos)
- ✅ Documentation (getting started, concepts, Python SDK, SQL reference, architecture summary)

## Project Structure

```
apiary/
├── crates/
│   ├── apiary-core/       # Core types and traits
│   ├── apiary-comb/       # The comb (Delta tables) and storage backends
│   ├── apiary-runtime/    # Node runtime
│   ├── apiary-floor/      # Dance floor (skeleton, phase 5)
│   ├── apiary-colony/     # Bees, roles, signals (skeleton, phase 4)
│   ├── apiary-forage/     # Patch execution (skeleton, phase 5)
│   ├── apiary-entrance/   # Flight SQL and ingest (skeleton, phase 1e)
│   ├── apiary-net/        # iroh networking (skeleton, phase 2)
│   ├── apiary-observe/    # Deterministic simulator (skeleton, phase 3)
│   ├── apiary-plan/       # DataFusion SQL engine
│   ├── apiary-py/         # PyO3 bindings
│   └── apiary-cli/        # Command-line interface
├── python/                # Python package source
├── docs/                  # Documentation
│   ├── design/            # The redesign (source of truth)
│   ├── architecture/      # V1 design, kept as history
│   └── development/       # Step completion records
├── tests/                 # Acceptance tests
├── deploy/                # Raspberry Pi docker-compose configs
└── scripts/               # Benchmark and utility scripts
```

## Technology Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Runtime | Rust | Memory safety, zero-cost abstractions, ARM64 cross-compilation |
| Python Bridge | PyO3 + maturin | Zero-copy Arrow interop, native wheels |
| SQL Engine | Apache DataFusion | Rust-native, Arrow-native, extensible |
| Storage Format | Apache Parquet | Columnar, compressed, universal |
| In-Memory | Apache Arrow | Zero-copy, columnar, cross-language |
| Object Storage | S3 API | Universal, battle-tested |
| Async Runtime | Tokio | Standard Rust async |

## Development

### Build

```bash
# Build all Rust crates
cargo build --workspace

# Run tests
cargo test --workspace

# Run linter
cargo clippy --workspace

# Build Python package
maturin develop
```

### Run Tests

```bash
# Rust tests
cargo test --workspace

# Python acceptance tests
python tests/test_step1_acceptance.py
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to get started.

## Roadmap

### v1 — Prove the Core (In Progress)

- Single-node and multi-node operation
- Python SDK with data writing
- SQL queries via DataFusion
- ACID transactions via conditional writes
- Distributed query execution
- Mason bee isolation and leafcutter sizing

### v2 — Direct Communication

- SWIM (Scalable Weakly-consistent Infection-style Membership) gossip for sub-second failure detection
- Arrow Flight for low-latency data shuffles
- Full 20-behavior biological model
- Streaming ingestion
- Time travel queries

### v3 — Enterprise & Federation

- Multi-apiary federation
- Regulatory compliance
- Data lineage
- Advanced access control

See [docs/architecture/06-roadmap.md](docs/architecture/06-roadmap.md) for details.

## Documentation

### Getting Started
- [Getting Started](docs/getting-started.md)
- [Concepts](docs/concepts.md)

### Deployment Guides
- [Quick Deployment Guide](docs/deployment-quickstart.md) — Fast setup for Pi and containers
- [Raspberry Pi Deployment](docs/deployment-raspberry-pi.md) — Complete guide for edge devices
- [Cloud Container Deployment](docs/deployment-containers.md) — Docker and Kubernetes

### API & SQL
- [Python SDK Reference](docs/python-sdk.md)
- [SQL Reference](docs/sql-reference.md)

### Performance & Testing
- [Live Benchmark Results](https://apiarydata.github.io/apiary/) — View latest performance metrics
- [Performance Benchmarks](docs/BENCHMARKS.md) — Documentation and baseline metrics
- [Running Benchmarks](docs/benchmarking.md) — How to run performance tests
- [Raspberry Pi Benchmarking](docs/raspberry-pi-benchmarking.md) — Pi-specific resource configurations

### Architecture
- [Architecture Summary](docs/architecture-summary.md)
- [Architecture Overview](docs/architecture/01-architecture-overview.md)
- [Storage Engine](docs/architecture/02-storage-engine.md)
- [Swarm Coordination](docs/architecture/03-swarm-coordination.md)
- [Query Execution](docs/architecture/04-query-execution.md)
- [Behavioral Model](docs/architecture/05-behavioral-model.md)
- [Roadmap](docs/architecture/06-roadmap.md)

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Acknowledgments

Apiary draws inspiration from:
- The biological intelligence of bee colonies
- Apache Arrow ecosystem (DataFusion, Parquet, object_store)
- Delta Lake's transaction log design
- Modern lakehouse architectures

---

**Status**: v1 Release Candidate. All 10 steps complete. 🐝