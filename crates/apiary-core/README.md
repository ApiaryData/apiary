# apiary-core

Core types, traits, and configuration for the [Apiary](https://github.com/ApiaryData/apiary) distributed data processing framework.

## Overview

`apiary-core` provides the foundational building blocks shared across all Apiary crates:

- **Configuration** — `NodeConfig` with automatic hardware detection (CPU cores, memory, disk)
- **Error handling** — Unified `ApiaryError` type and `Result<T>` alias
- **Registry types** — `Hive`, `Box`, and `Frame` types representing the namespace hierarchy (database → schema → table)
- **Ledger types** — `LedgerEntry`, `LedgerAction`, `WriteResult`, and `CellMetadata` for ACID transactions
- **Storage trait** — `StorageBackend` trait defining the interface for all storage implementations
- **Typed identifiers** — `HiveId`, `BoxId`, `FrameId`, `NodeId`, `TaskId`, and other strongly-typed IDs

## Usage

```rust
use apiary_core::{NodeConfig, StorageBackend, ApiaryError, Result};
use apiary_core::{Registry, Hive, Frame};

// Auto-detect node hardware
let config = NodeConfig::detect();
println!("Cores: {}, Memory: {} bytes", config.cores, config.memory_bytes);
```

## The Beekeeping Metaphor

| Apiary Term | Database Equivalent | Description |
|-------------|-------------------|-------------|
| Hive | Database | Top-level logical grouping |
| Box | Schema | Namespace within a hive |
| Frame | Table | Queryable dataset |
| Cell | Parquet file | Physical storage unit |

## License

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.

Part of the [Apiary](https://github.com/ApiaryData/apiary) project.
