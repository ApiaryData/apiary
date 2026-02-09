# Apiary Project — Copilot Instructions

## What Is This Project?

Apiary is a distributed data processing framework inspired by bee colony
intelligence. It runs on Raspberry Pis and small compute. Data lives in object
storage (S3/MinIO/local filesystem). Nodes coordinate through the storage layer
— no consensus protocol, no gossip, no direct node-to-node communication.

## Architecture Documents

Read these before implementing anything. They are the source of truth:

- `docs/architecture/01-architecture-overview.md` — project vision, design principles
- `docs/architecture/02-storage-engine.md` — StorageBackend trait, ledger, cells, ACID
- `docs/architecture/03-swarm-coordination.md` — heartbeat table, world view, node lifecycle
- `docs/architecture/04-query-execution.md` — DataFusion integration, distributed planning
- `docs/architecture/05-behavioral-model.md` — mason chambers, leafcutter sizing, colony temperature
- `docs/architecture/06-roadmap.md` — what ships in v1, what is deferred

## Development Prompts

`docs/prompts/07-v1-development-prompts.md` contains 10 sequential implementation
steps. Check `BUILD_STATUS.md` for which steps are complete.

## Technical Stack

- **Language:** Rust (2021 edition)
- **Python bridge:** PyO3 + maturin
- **SQL engine:** Apache DataFusion
- **Storage format:** Apache Parquet (LZ4 compression)
- **In-memory format:** Apache Arrow
- **Object storage:** `object_store` crate (S3/GCS/local)
- **Async runtime:** Tokio
- **Serialisation:** serde + serde_json

## Code Conventions

- Use `thiserror` for error types, not anyhow
- Use `tracing` for logging, not println or log
- All public APIs documented with rustdoc
- Tests go in `#[cfg(test)]` modules within the same file for unit tests
- Integration tests go in `tests/integration/`
- Python SDK mirrors Rust API naming (snake_case)
- All storage operations go through the `StorageBackend` trait — never raw filesystem

## Key Design Decisions (Do Not Deviate)

1. **Object storage is canonical.** All committed data lives in S3/local filesystem
   via the StorageBackend trait. Local disk is cache and spill only.
2. **No consensus protocol in v1.** Serialisation is via conditional writes
   (`put_if_not_exists`). No Raft, no Paxos.
3. **No gossip protocol in v1.** Node discovery via heartbeat files in storage.
   No SWIM, no epidemic broadcast.
4. **No direct node-to-node communication in v1.** All coordination through
   the shared storage layer. No Arrow Flight, no TCP between nodes.
5. **SQL fragments, not physical plans.** Distributed queries send SQL strings
   to workers, not serialised DataFusion PhysicalPlans.
6. **Four behaviors in v1.** Mason chambers, leafcutter sizing, abandonment,
   colony temperature. The other 16 are documented but deferred to v2.
7. **ListingTable, not CatalogProvider.** DataFusion integration via registering
   ListingTables, not custom catalog traits. Custom traits come in v2.

## When You Are Unsure

Read the architecture document. If the answer is not there, flag it as an
open question in a code comment prefixed with `// DESIGN:` and continue
with the simplest reasonable implementation.