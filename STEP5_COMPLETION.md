# Step 5 Completion Report

## Summary

Step 5 "Mason Bee Isolation + BeePool" has been successfully completed with all acceptance criteria met. Each bee (virtual core) now executes tasks in a sealed chamber with hard memory budgets, task timeout, and scratch directory isolation.

## What Was Implemented

### New Module: apiary-runtime/src/bee.rs

**MasonChamber** — sealed execution context per bee:
- `request_memory(bytes)` — cooperative memory enforcement with hard budget
- `release_memory(bytes)` — release previously allocated memory
- `utilisation()` — current memory usage as fraction (0.0 to 1.0)
- `reset()` — zero memory tracking and clean scratch directory after each task
- Hard memory budget prevents one bee from starving others
- Isolated scratch directory per bee (`{cache_dir}/scratch/bee_{n}/`)

**Bee** — virtual core with state and chamber:
- BeeId, BeeState (Idle / Busy(TaskId)), MasonChamber
- State tracked via async Mutex for concurrent access

**BeePool** — pool of bees routing tasks to idle workers:
- `BeePool::new(config)` — creates N bees (N = cores), each with `memory_per_bee` budget
- `BeePool::submit(task)` — find idle bee, mark busy, execute in chamber. If all busy, queue.
- `BeePool::status()` — state and memory per bee
- Queued tasks automatically drain to idle bees as they become available
- Task timeout enforcement via `tokio::time::timeout`

### Core Type Addition

- Added `TaskId` to `apiary-core/src/types.rs` — typed identifier for tasks submitted to the pool

### Runtime Integration

- `ApiaryNode` now contains a `BeePool` (created during `start()`)
- `sql()` method routes queries through the BeePool — each query is assigned to an idle bee
- Added `bee_status()` method to `ApiaryNode`
- Query context is `Arc<Mutex<ApiaryQueryContext>>` for safe sharing across bees

### Python SDK

- Added `bee_status()` method to `Apiary` class
- Returns list of dicts: `[{'bee_id': 'bee-0', 'state': 'idle', 'memory_used': 0, 'memory_budget': N}, ...]`

## Testing Results

### Cargo Tests
- 38 tests in apiary-core
- 31 tests in apiary-storage
- 13 tests in apiary-query
- 13 tests in apiary-runtime (11 new bee tests + 2 existing node tests)
- All 97 tests passing ✅

### New Rust Unit Tests (bee.rs)
- `test_bee_pool_creates_correct_number_of_bees` ✓
- `test_bee_pool_executes_task` ✓
- `test_bee_returns_to_idle_after_task` ✓
- `test_memory_enforcement` ✓
- `test_memory_exceeded_does_not_affect_other_bees` ✓
- `test_task_timeout` ✓
- `test_scratch_directory_isolated_and_cleaned` ✓
- `test_concurrent_tasks_on_separate_bees` ✓
- `test_tasks_queue_when_all_bees_busy` ✓
- `test_chamber_utilisation` ✓
- `test_chamber_reset` ✓

### Python Acceptance Tests (test_step5_acceptance.py)
- Test 1: BeePool creates correct bees (2 checks) ✓
- Test 2: bee_status() structure (8 checks) ✓
- Test 3: Bee IDs sequential (2 checks) ✓
- Test 4: SQL through BeePool (2 checks) ✓
- Test 5: Aggregation through pool (2 checks) ✓
- Test 6: Custom commands through pool (2 checks) ✓
- Test 7: Concurrent SQL queries (5 checks) ✓
- Test 8: Bees return to idle (1 check) ✓
- Test 9: Memory budget per bee (2 checks) ✓
- Test 10: Backward compat solo mode (2 checks) ✓
- **Total: 28 checks, all passing ✅**

### Backward Compatibility
- Step 1 acceptance tests: All passing ✅
- Step 2 acceptance tests: All passing ✅
- Step 3 acceptance tests: All passing ✅
- Step 4 acceptance tests: All passing ✅
- Cargo clippy: No new warnings ✅

## Acceptance Criteria Met

1. ✅ BeePool creates correct number of bees (N = cores)
2. ✅ Tasks execute with enforced memory budgets
3. ✅ Memory exceeded → task terminated, other bees unaffected
4. ✅ Task timeout terminates long-running tasks
5. ✅ Scratch directories isolated and cleaned after each task
6. ✅ Multiple concurrent SQL queries run on separate bees
7. ✅ When all bees busy, tasks queue and execute when bees become available
8. ✅ `cargo clippy` and `cargo test` pass

## Ready for Next Steps

With Step 5 complete, solo mode has bee isolation:
- Step 6: Heartbeat + World View (multi-node discovery)
- Step 7: Distributed Queries
