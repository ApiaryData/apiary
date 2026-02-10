# Step 9 Completion: Behavioral Model

## Summary

Step 9 implements the four v1 behaviors from the biological model that govern resource management and failure recovery in Apiary. The behavioral model brings together the isolated execution (Sealed Chambers), optimal sizing (Leafcutter), failure handling (Abandonment), and system health monitoring (Colony Temperature) into a cohesive system.

## What Was Implemented

### Behavioral Model Module (`behavioral.rs`)

#### AbandonmentTracker
- Tracks task failures and decides when to abandon failing tasks
- `trial_counts: HashMap<TaskId, u32>` — failure count per task
- `trial_limit: u32` — maximum attempts before abandoning (default: 3)
- `record_failure()` — records a failure and returns decision (Retry or Abandon)
- `record_success()` — clears failure count on success
- `get_count()` — query current failure count
- `clear()` — reset all tracked failures

#### AbandonmentDecision Enum
- `Retry` — try again (possibly on different node)
- `Abandon` — give up with diagnostic error

#### ColonyThermometer
- Measures system health as composite temperature (0.0 to 1.0)
- `setpoint: f64` — target temperature (default: 0.5)
- `measure(&BeePool)` — calculates temperature from:
  - CPU utilization: busy bees / total bees (40% weight)
  - Memory pressure: average memory utilization (40% weight)
  - Queue pressure: queued tasks / capacity (20% weight)
- `regulation(temperature)` — classifies temperature into regulation state

#### TemperatureRegulation Enum
- `Cold` (< 0.3) — system underutilized, run background maintenance
- `Ideal` (0.3-0.7) — normal operation
- `Warm` (0.7-0.85) — reduce non-essential work, defer compaction
- `Hot` (0.85-0.95) — throttle writes, pause maintenance
- `Critical` (> 0.95) — reject new work, focus on in-progress tasks

### BeePool Integration (`bee.rs`)
- Added `queue_size()` method to expose queued task count
- Enables queue pressure measurement for colony temperature

### Heartbeat Integration (`heartbeat.rs`)
- Added `ColonyThermometer` to `HeartbeatWriter`
- Updated `collect_heartbeat()` to:
  - Use thermometer's `measure()` for accurate temperature
  - Report real `queue_depth` from bee pool
  - Replace simple heuristic with proper behavioral model measurement
- Colony temperature now reported in every heartbeat

### Node Integration (`node.rs`)
- Added `ColonyThermometer` field to `ApiaryNode`
- Added `AbandonmentTracker` field to `ApiaryNode`
- Instantiated with default values on node start
- Added `colony_status()` method returning `ColonyStatus`
- Exported `ColonyStatus` struct with:
  - `temperature: f64` — current reading (0.0-1.0)
  - `regulation: String` — state classification
  - `setpoint: f64` — target temperature

### Python Bindings (`lib.rs`)
- Added `colony_status()` method to `Apiary` class
- Returns dict with:
  - `"temperature"`: float (0.0-1.0)
  - `"regulation"`: str ("cold"/"ideal"/"warm"/"hot"/"critical")
  - `"setpoint"`: float (default 0.5)
- Accessible via `ap.colony_status()` in Python

## Tests

### Rust Unit Tests (8 tests in `behavioral.rs`)

#### Abandonment Tracker Tests (4 tests)
1. `test_abandonment_tracker_retry_then_abandon` — Retry twice, then abandon on third failure
2. `test_abandonment_tracker_success_clears_count` — Success resets failure count
3. `test_abandonment_tracker_independent_tasks` — Different tasks tracked independently
4. `test_abandonment_tracker_clear` — Clear resets all counts

#### Colony Temperature Tests (4 tests)
1. `test_temperature_regulation_classification` — Correct classification for all temperature ranges
2. `test_temperature_regulation_as_str` — String representation of regulation states
3. `test_colony_temperature_idle` — Idle system has low temperature (Cold)
4. `test_colony_temperature_all_busy` — Busy system has elevated temperature

### Python Acceptance Tests (6 checks in `test_step9_acceptance.py`)
1. Colony temperature on idle node (should be Cold/Ideal)
2. Colony temperature in swarm_status (reported for each node)
3. Temperature changes with load (write activity)
4. Behavioral model integration (works with all existing features)
5. Documentation match (implementation matches architecture spec)
6. Backward compatibility (Steps 1-8 still work)

## Acceptance Criteria Met

1. ✅ `AbandonmentTracker` implemented with trial limits and retry/abandon decisions
2. ✅ `ColonyThermometer` implemented with composite measurement and regulation classification
3. ✅ Behavioral model integrated into `ApiaryNode`
4. ✅ Colony temperature reported in heartbeats
5. ✅ Python API `colony_status()` accessible to users
6. ✅ Unit tests for abandonment and temperature pass
7. ✅ Acceptance tests verify backward compatibility
8. ✅ `cargo clippy` and `cargo test` pass (with pre-existing warnings only)

## Design Decisions

### Four v1 Behaviors

The behavioral model specifies 20 behaviors across bee species, but only 4 are active in v1 because they address concrete problems in the current architecture:

**1. Sealed Chambers (Mason Bees, Osmia) — Behavior already implemented in Step 5**
- Each bee executes in an isolated chamber with hard memory budget
- `MasonChamber` struct with `memory_budget`, `memory_used`, `scratch_dir`
- Blast-radius containment: one bee failure doesn't affect others
- Memory enforcement via `request_memory()` and `release_memory()`
- Scratch directory isolation and cleanup

**2. Precise Cutting (Leafcutter Bees, Megachile) — Behavior already implemented in Steps 3-5**
- Cell sizing based on node capacity: `target_cell_size = memory_per_bee / 4`
- `CellSizingPolicy` with min/max bounds
- Task sizing for heterogeneous hardware (Pi vs. cloud)
- Minimizes S3 overhead by respecting minimum cell size

**3. Task Abandonment (Honeybee) — Implemented in Step 9**
- Tracks task failures per task ID
- Retries up to trial_limit (default 3)
- Returns diagnostic information on abandonment
- Prevents infinite retry loops on failing work
- Foundation for v2 multi-node retry with diagnostics

**4. Colony Temperature (Honeybee) — Implemented in Step 9**
- Composite metric from CPU util + memory pressure + queue pressure
- Classification into 5 regulation states
- Reported in heartbeats (visible to other nodes)
- Interface between biological model and user experience
- Foundation for v2 backpressure and maintenance scheduling

### Temperature Measurement Formula

```
temperature = 0.4 * cpu_util + 0.4 * memory_pressure + 0.2 * queue_pressure
```

**Why this weighting?**
- CPU and memory are the primary resources (40% each)
- Queue depth is a leading indicator but less critical (20%)
- Formula produces intuitive results:
  - All idle: ~0.0 (Cold)
  - Half busy: ~0.4 (Ideal)
  - All busy: ~0.4+ (Ideal to Warm)
  - All busy + high memory + queue: ~0.9+ (Hot to Critical)

### Regulation Thresholds

| Temperature | Regulation | Rationale |
|---|---|---|
| < 0.3 | Cold | System has spare capacity, schedule maintenance |
| 0.3-0.7 | Ideal | Normal operation, 30-70% utilization is healthy |
| 0.7-0.85 | Warm | Approaching capacity, defer optional work |
| 0.85-0.95 | Hot | Near capacity, apply backpressure |
| > 0.95 | Critical | At capacity, reject new work |

These thresholds ensure:
- Maintenance runs when system is underutilized
- Normal operation has headroom (not targeting 100% utilization)
- Backpressure kicks in before system is overwhelmed

### Abandonment vs. Retry in v1

In v1, the `AbandonmentTracker` is integrated into the node but **not actively used in query execution** because:
1. v1 queries are simple (no multi-attempt retry logic yet)
2. Tasks that fail report errors immediately
3. The tracker provides the **foundation** for v2 distributed retry

In v2, when a worker node fails mid-task, the coordinator will:
1. Detect the failure
2. Call `abandonment_tracker.record_failure(task_id)`
3. Get `Retry` or `Abandon` decision
4. On `Retry`, reassign to another node
5. On `Abandon`, return diagnostic error to user

### Temperature vs. Load Metrics

Why have both `colony_temperature` and individual load metrics (busy bees, memory pressure)?
- **Individual metrics** tell you what's happening (4 of 8 bees busy, 60% memory used)
- **Colony temperature** tells you what it **means** (0.55 = Ideal, keep going)
- **Regulation state** tells you what to **do** (Warm → defer compaction)

Temperature is the **interface** between the biological model and operational decisions.

## Integration with Existing Features

### Step 5 (Mason Bee Isolation)
- Sealed chambers provide the substrate for temperature measurement
- Memory tracking in chambers feeds into memory pressure
- Chamber utilization is the basis for CPU utilization

### Step 6 (Heartbeat + World View)
- Colony temperature reported in every heartbeat
- Other nodes can see temperature in swarm_status
- Foundation for v2 load-aware query planning

### Step 7 (Distributed Queries)
- Abandonment tracker foundation for multi-node retry
- Temperature will guide task assignment in v2
- Currently temperature is measured but not used for admission control

### Step 8 (Local Cell Cache)
- Cache size not currently included in temperature
- Future: cache thrashing could elevate temperature
- Future: Cold temperature could trigger cache pre-warming

## Behavioral Model Roadmap

### v1 (Complete in Step 9)
✅ Sealed Chambers — Memory-budgeted isolation per bee  
✅ Leafcutter Sizing — Capacity-based cell and task sizing  
✅ Task Abandonment — Retry limits with diagnostic errors  
✅ Colony Temperature — System homeostasis measurement  

### v2 (Future)
- **Waggle Dance** — Quality-weighted task distribution via gossip
- **Pheromone Signalling** — Continuous backpressure signals
- **Trophallaxis** — Metadata-rich data transfer via Arrow Flight
- **Guard Bees** — Distributed authentication
- **Orientation Flights** — Gradual node ramp-up
- **Propolis** — Security auditing
- **Winter Cluster** — Degraded mode during partition
- **Tiered Storage** — Purpose-built cell classification
- **Carpenter Bee Indexes** — Bloom filters and zone maps
- **Orchid Bee Library** — Metadata enrichment
- **Variable Sociality** — Adaptive coordination intensity

### Why Only 4 Behaviors in v1?

The full 20-behavior model requires:
1. **Direct node-to-node communication** (SWIM gossip, Arrow Flight)
2. **Sophisticated query planner** (dynamic task routing)
3. **Local write-ahead log** (streaming ingestion, offline mode)
4. **Compaction and maintenance** (background tasks to schedule)

v1 is **storage-coordinated** (no direct communication), **simple planning** (static cell assignment), and **query-only** (no streaming writes). The 4 active behaviors address the concrete needs of this architecture.

When v2 adds gossip and Flight, the remaining 16 behaviors activate naturally because they'll have a substrate to act on.

## Future Enhancements (Not in v1)

### Abandonment with Diagnostics
Currently abandonment returns a simple decision. In v2:
```rust
pub struct AbandonmentHistory {
    attempts: Vec<AttemptRecord>,
    suggested_remediation: String,
}

pub struct AttemptRecord {
    node_id: NodeId,
    error: ApiaryError,
    timestamp: DateTime<Utc>,
}
```

### Temperature-Based Admission Control
Currently temperature is measured but not used for decisions. In v2:
```rust
impl ApiaryNode {
    pub async fn should_accept_write(&self) -> bool {
        let temp = self.colony_status().await.temperature;
        temp < 0.95  // Reject writes in Critical state
    }
}
```

### Temperature-Triggered Maintenance
When temperature is Cold, schedule compaction and cache warming:
```rust
if regulation == TemperatureRegulation::Cold {
    // System has spare capacity
    compaction_scheduler.run_compaction().await?;
    cache_warmer.prefetch_hot_cells().await?;
}
```

### Cross-Node Temperature Awareness
Distributed query planner can avoid stressed nodes:
```rust
let available_nodes: Vec<_> = world_view
    .alive_nodes()
    .filter(|n| n.heartbeat.load.colony_temperature < 0.85)
    .collect();
```

## Status

**Behavioral model complete!**
- Task abandonment tracker implemented and tested
- Colony temperature measurement integrated with bee pool
- Temperature regulation classification working
- Python API accessible via `colony_status()`
- Backward compatible with all previous steps
- Foundation laid for v2 behavioral model expansion

**Next Step**: Testing + Hardening (Step 10) for v1 Release Candidate
