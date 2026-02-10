# Step 4 Completion Report

## Summary

Step 4 "DataFusion Integration" has been successfully completed with all acceptance criteria met. **Solo mode is now functionally complete.**

## What Was Implemented

### New Crate: apiary-query

**ApiaryQueryContext** — wraps DataFusion SessionContext with Apiary namespace resolution:
- SQL query execution over frames via DataFusion
- Custom command interception (USE, SHOW, DESCRIBE)
- Table reference resolution (3-part: hive.box.frame, 2-part with context, 1-part with context)
- Cell pruning from WHERE predicates (partition + stat pruning)
- DML blocking (DELETE, UPDATE, INSERT, DROP, CREATE, ALTER)

### Query Execution Flow
1. Parse SQL to detect custom commands vs standard queries
2. For custom commands: query registry, return Arrow tables
3. For standard SQL:
   - Extract table references from FROM/JOIN clauses
   - Extract WHERE predicates for pruning
   - Resolve each table reference via registry
   - Open ledger, apply partition + stat pruning
   - Read surviving cells via CellReader
   - Register as DataFusion MemTable
   - Execute SQL via DataFusion
   - Return results as Arrow RecordBatches

### Custom SQL Commands
- `USE HIVE <name>` — set default hive context
- `USE BOX <name>` — set default box context (requires hive)
- `SHOW HIVES` — list all hives
- `SHOW BOXES IN <hive>` — list boxes in a hive
- `SHOW FRAMES IN <hive>.<box>` — list frames in a box
- `DESCRIBE <hive>.<box>.<frame>` — show schema, partition info, cell count, row count

### Cell Pruning
- **Partition pruning**: WHERE column = 'value' filters cells by partition values
- **Stat pruning**: WHERE column > N / < N skips cells whose min/max don't overlap

### Runtime Integration
- Added `sql()` method to `ApiaryNode`
- Uses `tokio::sync::Mutex` for async-safe query context access

### Python SDK
- Added `sql()` method to `Apiary` class
- Returns Arrow IPC bytes (deserializable by PyArrow)
- Supports all custom commands and standard SQL

## Testing Results

### Cargo Tests
- 38 tests in apiary-core
- 31 tests in apiary-storage
- 13 tests in apiary-query (new)
- 2 tests in apiary-runtime
- All 85 tests passing ✅

### Python Acceptance Tests (test_step4_acceptance.py)
- Test 1: SELECT * End-to-End (3 checks) ✓
- Test 2: Partition Pruning (2 checks) ✓
- Test 3: Cell Stat Pruning (2 checks) ✓
- Test 4: Projection Pushdown (2 checks) ✓
- Test 5: Aggregation / GROUP BY (6 checks) ✓
- Test 6: USE HIVE / USE BOX (2 checks) ✓
- Test 7: SHOW Commands (6 checks) ✓
- Test 8: DESCRIBE (5 checks) ✓
- Test 9: DELETE / UPDATE Errors (2 checks) ✓
- Test 10: Solo Mode E2E (3 checks) ✓
- Test 11: ORDER BY + LIMIT (2 checks) ✓
- **Total: 35 checks, all passing ✅**

### Backward Compatibility
- Step 1 acceptance tests: All passing ✅
- Step 2 acceptance tests: All passing ✅
- Step 3 acceptance tests: All passing ✅
- Cargo clippy: No new warnings ✅

## Dependencies Added

- `datafusion` 43 (SQL query engine with Arrow-native execution)

## Acceptance Criteria Met

1. ✅ `ap.sql("SELECT * FROM hive.box.frame")` works end-to-end
2. ✅ Partition pruning: `WHERE region = 'north'` only registers north cells
3. ✅ Cell stat pruning: `WHERE temp > 25` skips cells whose max < 25
4. ✅ Projection pushdown: `SELECT temp FROM ...` returns only the temp column
5. ✅ Aggregations: `GROUP BY` with AVG, SUM, COUNT produce correct results
6. ✅ USE HIVE / USE BOX set context correctly
7. ✅ SHOW and DESCRIBE return correct results
8. ✅ DELETE / UPDATE produce clear error messages
9. ✅ **Solo mode is complete:** create → write → query on a single node
10. ✅ `cargo clippy` and `cargo test` pass

## Ready for Next Steps

With Step 4 complete, solo mode is functionally complete:
- Step 5: Mason Bee Isolation (memory budgets, sealed chambers)
- Step 6: Heartbeat + World View (multi-node discovery)
