# apiary-storage

Storage backends and transaction ledger for the [Apiary](https://github.com/ApiaryData/apiary) distributed data processing framework.

## Overview

`apiary-storage` implements the storage layer that all Apiary nodes interact with:

- **LocalBackend** — Filesystem-based storage for single-node and development use
- **S3Backend** — S3-compatible object storage (AWS S3, MinIO, GCS) for production and multi-node deployments
- **Ledger** — ACID transaction log using conditional writes for optimistic concurrency control
- **CellWriter** — Writes Arrow RecordBatches to Parquet files with LZ4 compression, partitioning, and column-level statistics
- **CellReader** — Reads Parquet cells with projection pushdown and partition pruning

## Usage

```rust
use apiary_storage::{LocalBackend, Ledger, CellWriter, CellReader};

// Create a local storage backend
let backend = LocalBackend::new("/tmp/apiary-data").await?;

// Or connect to S3
// let backend = S3Backend::new("s3://my-bucket/apiary").await?;
```

## Design

Object storage is the canonical source of truth in Apiary. Nodes are stateless workers that read from and write to the storage layer. Coordination between nodes happens through the storage layer itself — no node-to-node communication is needed.

The ledger provides ACID transactions using conditional writes (similar to Delta Lake's transaction log). When two nodes attempt conflicting writes, one succeeds and the other retries.

## License

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.

Part of the [Apiary](https://github.com/ApiaryData/apiary) project.
