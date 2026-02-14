# Step 6 Completion: Heartbeat Table + World View

## Summary

Step 6 adds multi-node awareness to Apiary through heartbeat writing, world view building, and node lifecycle management. This is the **Multi-Node Milestone** — nodes can now discover each other through shared object storage.

## What Was Implemented

### Heartbeat Writer (`heartbeat.rs`)
- Background task writes `_heartbeats/node_{id}.json` every `heartbeat_interval` (default 5s)
- Heartbeat contains: node_id, timestamp, version, capacity (cores, memory, memory_per_bee, target_cell_size), load (bees_busy, bees_idle, memory_pressure, queue_depth, colony_temperature), cache summary
- Version increments monotonically on each write
- Initial heartbeat written synchronously during `start()` for immediate availability

### World View Builder (`heartbeat.rs`)
- Background task polls `_heartbeats/` prefix every `poll_interval`
- Builds `WorldView` with `HashMap<NodeId, NodeStatus>`
- Node state detection:
  - **Alive**: heartbeat age < `dead_threshold / 2`
  - **Suspect**: heartbeat age between `dead_threshold / 2` and `dead_threshold`
  - **Dead**: heartbeat age > `dead_threshold`
- Initial world view built synchronously during `start()`

### Node Lifecycle
- **Join**: Node writes first heartbeat on start → other nodes discover on next poll
- **Graceful departure**: Node deletes heartbeat file on `shutdown()` → removed from world view
- **Crash**: Heartbeat goes stale → marked Suspect then Dead after threshold

### Cleanup
- `WorldViewBuilder::cleanup_stale()` deletes heartbeat files for nodes dead > configurable threshold (default 1 hour)

### Python SDK
- `ap.swarm_status()` returns accurate swarm information:
  ```python
  {
      'nodes': [
          {'node_id': '...', 'state': 'alive', 'bees': 4, 'idle_bees': 3, ...},
      ],
      'total_bees': 4,
      'total_idle_bees': 3
  }
  ```

### Solo Mode
- Zero special-casing: solo mode is a swarm of one
- Heartbeat writer and world view builder still run
- `swarm_status()` shows exactly 1 alive node

## Tests

### Rust Unit Tests (12 tests in `heartbeat.rs`)
1. `test_heartbeat_write_and_read` — Write and read back heartbeat JSON
2. `test_heartbeat_version_increments` — Version increases on each write
3. `test_heartbeat_delete` — Graceful departure deletes file
4. `test_world_view_discovers_nodes` — Finds multiple node heartbeats
5. `test_world_view_stale_heartbeat_becomes_dead` — Old heartbeat → Dead state
6. `test_world_view_suspect_state` — Moderately old heartbeat → Suspect
7. `test_world_view_poll_updates_shared_state` — Shared RwLock updated
8. `test_world_view_graceful_departure` — Deleted heartbeat removed from view
9. `test_world_view_solo_mode` — Single node = swarm of one
10. `test_world_view_totals` — total_bees and total_idle_bees correct
11. `test_cleanup_stale_heartbeats` — Old dead heartbeats cleaned up
12. `test_heartbeat_writer_with_cancel` — Background task stops on cancel

### Python Acceptance Tests (46 checks in `test_step6_acceptance.py`)
1. Heartbeat file written to `_heartbeats/` with correct structure
2. Heartbeat version increments
3. `swarm_status()` returns correct structure (dict with nodes, total_bees, total_idle_bees)
4. Node info includes node_id, state, bees, idle_bees
5. Totals match node capacity
6. Graceful shutdown deletes heartbeat file
7. Solo mode E2E works with heartbeats active
8. Backward compatibility with Step 5 capabilities

## Acceptance Criteria Met

1. ✅ Node writes heartbeat to storage backend every interval
2. ✅ World view builder discovers other nodes' heartbeats
3. ✅ Stale heartbeat → node marked Suspect then Dead
4. ✅ Graceful departure → heartbeat deleted, node removed from world view
5. ✅ Solo mode still works (node sees only its own heartbeat)
6. ✅ `ap.swarm_status()` returns accurate information
7. ✅ `cargo clippy` and `cargo test` pass
