# Apiary v1 Build Status

## Steps

| Step | Name | Status | Date | Notes |
|------|------|--------|------|-------|
| 1 | Skeleton + StorageBackend | complete | 2026-02-09 | All acceptance criteria met |
| 2 | Registry + Namespace | complete | 2026-02-10 | DDL operations, versioning, Python API with dual terminology |
| 3 | Ledger + Cell Storage | complete | 2026-02-10 | Transaction ledger, Parquet cells, partitioning, stats, write/read/overwrite |
| 4 | DataFusion Integration | complete | 2026-02-10 | SQL queries via DataFusion, custom commands, cell pruning, **SOLO MODE MILESTONE** |
| 5 | Mason Bee Isolation | complete | 2026-02-10 | BeePool, sealed chambers, memory budgets, task timeout, scratch isolation |
| 6 | Heartbeat + World View | complete | 2026-02-10 | Heartbeat writer, world view builder, node lifecycle, swarm_status, **MULTI-NODE MILESTONE** |
| 7 | Distributed Queries | complete | 2026-02-10 | Query planner, coordinator protocol, worker polling, result merging, **DISTRIBUTED EXECUTION MILESTONE** |
| 8 | Local Cell Cache | complete | 2026-02-10 | LRU cache, cache reporting in heartbeats, cache-aware query planning |
| 9 | Behavioral Model | complete | 2026-02-10 | Colony temperature, task abandonment tracker, Python API |
| 10 | Testing + Hardening | not started | | **v1 RC MILESTONE** |

## Design Decisions Made During Implementation

Record deviations from the architecture docs here:

- Updated Python bindings to use `PyDict::new_bound()` for PyO3 0.22 compatibility instead of deprecated `PyDict::new()`
- Removed ID fields from Hive, Box, and Frame structs to simplify serialization - names serve as identifiers in the registry
- Added dual terminology support (bee-themed and traditional database naming) in Python API for broader accessibility
- Step 7: Simplified distributed execution for v1 - no aggregation decomposition, EXPLAIN commands, or sophisticated retry logic (can be added in future versions)

## Open Questions

Record questions that need human input:

(none yet)

## Known Issues

(none yet)