# Apiary v1 Build Status

## Steps

| Step | Name | Status | Date | Notes |
|------|------|--------|------|-------|
| 1 | Skeleton + StorageBackend | complete | 2026-02-09 | All acceptance criteria met |
| 2 | Registry + Namespace | complete | 2026-02-10 | DDL operations, versioning, Python API with dual terminology |
| 3 | Ledger + Cell Storage | complete | 2026-02-10 | Transaction ledger, Parquet cells, partitioning, stats, write/read/overwrite |
| 4 | DataFusion Integration | complete | 2026-02-10 | SQL queries via DataFusion, custom commands, cell pruning, **SOLO MODE MILESTONE** |
| 5 | Mason Bee Isolation | complete | 2026-02-10 | BeePool, sealed chambers, memory budgets, task timeout, scratch isolation |
| 6 | Heartbeat + World View | not started | | **MULTI-NODE MILESTONE** |
| 7 | Distributed Queries | not started | | |
| 8 | Local Cell Cache | not started | | |
| 9 | Behavioral Model | not started | | |
| 10 | Testing + Hardening | not started | | **v1 RC MILESTONE** |

## Design Decisions Made During Implementation

Record deviations from the architecture docs here:

- Updated Python bindings to use `PyDict::new_bound()` for PyO3 0.22 compatibility instead of deprecated `PyDict::new()`
- Removed ID fields from Hive, Box, and Frame structs to simplify serialization - names serve as identifiers in the registry
- Added dual terminology support (bee-themed and traditional database naming) in Python API for broader accessibility

## Open Questions

Record questions that need human input:

(none yet)

## Known Issues

(none yet)