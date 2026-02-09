# Behavioral Model

## Overview

The behavioral model is the biological core of Apiary. It governs how each node manages its resources: how much memory each task gets, how large cells should be, when to give up on failing work, and when the system is too stressed to accept more.

Twenty bee behaviors — drawn from honeybees and seven additional species — form the complete model. But behaviors are only meaningful when they have a substrate to act on. In v1, where nodes are stateless compute against object storage with no direct communication, four behaviors are active. They address the concrete problems that exist in v1: resource isolation, work sizing, failure recovery, and system-wide health.

The remaining sixteen behaviors activate in v2 when the system gains direct node-to-node communication (gossip, Arrow Flight) and a more sophisticated planner. The full model is documented here so the architectural vision is clear, even though most of it is deferred.

---

## v1 Behaviors

### Behavior 1: Sealed Chambers — Isolated Execution (Mason Bees, Osmia)

#### Biological Basis

Mason bees are solitary. Each female builds a series of sealed chambers in a hollow tube, each containing one egg with a food provision. The chambers are independent — if one fails, the others are unaffected.

#### Software Pattern

Each bee (virtual core) executes tasks in its own sealed chamber — an isolated execution context with hard resource boundaries:

```rust
pub struct MasonChamber {
    pub bee_id: BeeId,
    pub memory_budget: u64,        // hard limit in bytes
    pub memory_used: AtomicU64,    // current usage tracking
    pub scratch_dir: PathBuf,      // isolated temp directory
    pub task_timeout: Duration,    // max execution time per task
}
```

**Blast-radius containment.** If one chamber fails (out of memory, corrupt data, timeout), the other chambers on the same node continue unaffected. A single runaway task cannot bring down all 4 bees on a Pi.

**Memory enforcement.** The memory budget is cooperative in v1 — tasks call `request_memory()` before large allocations and `release_memory()` after. A bee that exceeds its budget has its task terminated, the chamber is cleaned (scratch directory cleared), and the bee returns to idle.

```rust
impl MasonChamber {
    pub fn request_memory(&self, bytes: u64) -> Result<()> {
        let current = self.memory_used.load(Ordering::Relaxed);
        if current + bytes > self.memory_budget {
            return Err(ApiaryError::MemoryExceeded {
                bee_id: self.bee_id.clone(),
                budget: self.memory_budget,
                requested: bytes,
            });
        }
        self.memory_used.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
    
    pub fn release_memory(&self, bytes: u64) {
        self.memory_used.fetch_sub(bytes, Ordering::Relaxed);
    }
    
    pub fn utilisation(&self) -> f64 {
        self.memory_used.load(Ordering::Relaxed) as f64 / self.memory_budget as f64
    }
}
```

**Scratch directory isolation.** Each chamber has its own temp directory. Spill files from large sorts or joins are written here. When a task completes or fails, the directory is cleared. One bee's scratch files cannot affect another bee.

#### Architectural Impact

On a 4GB Raspberry Pi with 4 bees, each bee gets 1GB. Without mason chambers, one bee scanning a large cell could consume all 4GB, starving the other three and crashing the node. With chambers, the blast radius of any failure is one bee.

---

### Behavior 2: Precise Cutting — Optimal Cell and Task Sizing (Leafcutter Bees, Megachile)

#### Biological Basis

Leafcutter bees cut precise circular and oval pieces from leaves to construct their nest cells. Every cut is the right size, with minimal waste.

#### Software Pattern

Cell sizing and task partitioning are precisely calculated to match each node's capacity.

**Cell sizing during writes:**

```
Pi with 1GB per bee:
  target_cell_size = 256MB  (memory_per_bee / 4, with headroom for processing)
  min_cell_size = 16MB      (S3 per-request overhead floor)
  max_cell_size = 512MB

Cloud node with 4GB per bee:
  target_cell_size = 1GB
  min_cell_size = 64MB
  max_cell_size = 2GB
```

**Task sizing during queries:**

```
Query needs to scan 2GB of data.

Assigning to pi-factory-03 (1GB per bee):
  4 tasks of ~500MB each — each fits one bee's chamber

Assigning to cloud-eu-01 (4GB per bee):
  1 task of 2GB — single bee handles it efficiently
```

The leafcutter principle: **cut the work to fit the worker, not the other way around.** A Pi bee and a cloud bee receive differently-sized work from the same query.

**Why the minimum cell size matters.** Object storage has per-request overhead (20-200ms for each GET). Very small cells (< 1MB) create excessive overhead — the time to open and close the HTTP connection dominates the time to read the data. The minimum cell size ensures I/O cost is amortised.

#### Architectural Impact

This directly addresses the heterogeneous hardware challenge. The planner does not treat all bees as equal — it sizes work to fit each node's capacity, producing different cuts for different workers.

---

### Behavior 3: Task Abandonment — Self-Healing Failure Recovery (Honeybee, Apis mellifera)

#### Biological Basis

In a real colony, a forager bee that repeatedly returns to an unprofitable food source eventually abandons it and returns to the dance floor to find better work. This prevents the colony from wasting effort on diminishing sources.

#### Software Pattern

Tasks that repeatedly fail are abandoned rather than retried indefinitely:

```rust
pub struct AbandonmentTracker {
    trial_counts: HashMap<TaskId, u32>,
    trial_limit: u32,            // default 3
}

impl AbandonmentTracker {
    pub fn record_failure(&mut self, task_id: &TaskId) -> AbandonmentDecision {
        let count = self.trial_counts.entry(task_id.clone()).or_insert(0);
        *count += 1;
        if *count >= self.trial_limit {
            AbandonmentDecision::Abandon
        } else {
            AbandonmentDecision::Retry
        }
    }
}

pub enum AbandonmentDecision {
    Retry,       // try again (possibly on a different node)
    Abandon,     // give up — query fails with diagnostic error
}
```

**Failure types and responses:**

| Failure | First occurrence | After trial limit |
|---|---|---|
| Memory exceeded | Retry on a node with more capacity | Abandon with memory diagnostic |
| Task timeout | Retry with extended timeout | Abandon with timeout diagnostic |
| Cell read failure (S3 error) | Retry after backoff | Abandon with storage diagnostic |
| Node died mid-task | Retry on another node | Abandon if no nodes available |
| Corrupt cell data | Abandon immediately (no retry) | — |

**Diagnostic reporting.** When a task is abandoned, the error message includes the full history: how many attempts, which nodes were tried, what errors occurred, and suggested remediation.

```
Error: Query failed — task t_002 abandoned after 3 attempts.
  Attempt 1 on pi-factory-03: MemoryExceeded (requested 1.2GB, budget 1GB)
  Attempt 2 on pi-factory-07: TaskTimeout (exceeded 30s limit)
  Attempt 3 on cloud-eu-01: S3 GetObject failed (connection timeout)
  
  Suggestion: The cells being scanned are large. Consider:
  - Running compaction to merge small cells (apiary vacuum)
  - Increasing the task timeout (SET query_timeout = 120)
```

#### Architectural Impact

Without abandonment, a failing task retries forever, wasting bee capacity and blocking the query. With abandonment, the system gives up gracefully and gives the user actionable information about what went wrong.

---

### Behavior 4: Colony Temperature — System Homeostasis (Honeybee, Apis mellifera)

#### Biological Basis

A real honeybee colony regulates its internal temperature to a precise 35°C. When the hive is too hot, bees fan their wings and bring water for evaporative cooling. When too cold, bees cluster and vibrate their muscles to generate heat.

#### Software Pattern

Colony temperature is a composite health metric for the local node (0.0 to 1.0) calculated from system state:

```rust
pub struct ColonyThermometer {
    setpoint: f64,              // target temperature (default 0.5)
}

impl ColonyThermometer {
    pub fn measure(&self, bee_pool: &BeePool) -> f64 {
        let cpu_util = bee_pool.busy_bees() as f64 / bee_pool.total_bees() as f64;
        let memory_pressure = bee_pool.avg_memory_utilisation();
        let queue_pressure = bee_pool.queued_tasks() as f64 / (bee_pool.total_bees() as f64 * 2.0);
        
        // Weighted composite
        0.4 * cpu_util + 0.4 * memory_pressure + 0.2 * queue_pressure
    }
    
    pub fn regulation(&self, temperature: f64) -> TemperatureRegulation {
        match temperature {
            t if t < 0.3 => TemperatureRegulation::Cold,
            t if t <= 0.7 => TemperatureRegulation::Ideal,
            t if t <= 0.85 => TemperatureRegulation::Warm,
            t if t <= 0.95 => TemperatureRegulation::Hot,
            _ => TemperatureRegulation::Critical,
        }
    }
}

pub enum TemperatureRegulation {
    Cold,       // system underutilised — run background maintenance (compaction)
    Ideal,      // normal operation
    Warm,       // reduce non-essential work — defer compaction
    Hot,        // throttle writes (backpressure), pause maintenance
    Critical,   // reject new work, focus on completing in-progress tasks
}
```

**What temperature regulates:**

| Temperature | Write behavior | Compaction | Query admission |
|---|---|---|---|
| Cold (< 0.3) | Normal | Actively scheduled | Normal |
| Ideal (0.3-0.7) | Normal | Opportunistic | Normal |
| Warm (0.7-0.85) | Normal | Deferred | Normal |
| Hot (0.85-0.95) | Backpressure (block/error) | Paused | Queued |
| Critical (> 0.95) | Rejected | Paused | Rejected |

**Reporting.** Colony temperature is included in:
- The heartbeat file (other nodes see it in the world view)
- Write results (`result.temperature`)
- The `ap.colony_status()` SDK method
- Query explanations (`EXPLAIN ANALYZE`)

#### Architectural Impact

Colony temperature is the interface between the biological model and the user experience. When a write is slow because of backpressure, the user sees the temperature in the write result. When a query is planned suboptimally because nodes are stressed, the temperature appears in the EXPLAIN output. It makes the swarm's internal state legible.

---

## v2+ Behaviors: The Full Model

The following behaviors activate when v2 introduces direct node-to-node communication (SWIM gossip, Arrow Flight) and a more sophisticated query planner.

### Honeybee Behaviors (Apis mellifera)

**Waggle Dance (v2)** — Quality-weighted task distribution. When a bee completes a task, it broadcasts quality metrics (duration, efficiency, errors). Other bees probabilistically select work based on dance quality. Requires gossip for dance propagation.

**Three-Tier Workers (v2)** — Employed bees work on tasks, onlooker bees watch dances and select work, scout bees explore (health checks, compaction discovery, latency probes). Requires a substrate of activities for scouts to discover.

**Pheromone Signalling (v2)** — Continuous backpressure signals (memory pressure, queue depth, I/O saturation) propagated via gossip. The query planner reads pheromone levels to avoid stressed nodes. In v1, the heartbeat file carries the same information but with higher latency.

**Trophallaxis (v2)** — Data transfer with embedded metadata (freshness, lineage, confidence). When bees exchange data via Arrow Flight, the metadata travels with it. Not applicable when shuffles go through S3.

**Nectar Ripening (v2)** — Progressive data maturation for streaming writes. Raw data (nectar) enters a local buffer, passes through validation and quality stages, and is committed to object storage only when mature (capped honey). Requires a local write-ahead log.

**Guard Bees (v2)** — Distributed authentication at node boundaries. Nodes present credentials to join the swarm, validated by existing members. Not applicable when swarm membership is determined by storage access.

**Swarming (v2)** — Horizontal scaling by colony division. When the swarm grows beyond a threshold, it splits into sub-swarms with independent coordination but shared storage. Requires dynamic membership management.

**Orientation Flights (v2)** — Gradual new node ramp-up. A new node starts with light load and progressively takes on more work as it caches data and proves stability. In v1, the heartbeat table and cache-aware planner provide a simpler version of this.

**Propolis (v2)** — Continuous security auditing. Background processes verify ledger integrity, cell checksums, and access patterns. Can be implemented as a v1 extension without direct communication.

**Winter Cluster (v2)** — Degraded mode during network partition. When a node loses connectivity to object storage, it operates from local cache, buffers writes locally, and flushes when connectivity returns. Requires a local write-ahead log.

### Cross-Species Behaviors

**Stingless Bee Tiered Storage (v2)** — Purpose-built cell classification (brood/honey/pollen). Active ingestion cells are hot and local (brood). Mature queryable cells are in object storage (honey). Reference data is cached everywhere (pollen). Extends the simple LRU cache with intelligent tiering.

**Bumblebee Buzz Pollination (v2)** — Forced extraction from reluctant sources (rate-limited APIs, IoT protocols, proprietary formats). Extends the write path with specialised extractors.

**Cuckoo Bee Adversarial Testing (v2)** — Continuous chaos engineering as a swarm behavior. Simulated adversarial agents test security, consistency, and failure handling. Requires the system to have defences worth testing.

**Carpenter Bee Embedded Indexes (v2)** — Bloom filters in ledger metadata, zone maps within cells, hierarchical partition trees. Extends cell pruning with more sophisticated navigation structures.

**Orchid Bee Chemical Library (v2)** — Accumulated metadata enrichment. Each cell accrues metadata from observed behavior: query frequency, access patterns, common filters, staleness. Over time, the swarm builds a self-generating data catalog.

**Sweat Bee Variable Sociality (v2)** — Adaptive coordination intensity. The system scales its coordination complexity to the deployment size: solo (no coordination), semi-social (gossip only), fully eusocial (full behavioral model). In v1, the system is naturally "solo or simple multi-node" because the coordination is through object storage.

---

## Behavior Interaction Map

Even in v1, the four active behaviors interact:

```
Colony Temperature
  ├── Measures bee utilisation (from BeePool / Mason Chambers)
  ├── Controls write backpressure
  ├── Controls compaction scheduling
  └── Reported in heartbeat (informs other nodes' planning)

Mason Chambers
  ├── Enforce memory budgets per bee
  ├── Failed chambers trigger Abandonment
  └── Utilisation feeds Colony Temperature

Leafcutter Sizing
  ├── Cell sizes match node capacity (informed by mason chamber budget)
  ├── Task sizes match target node (informed by world view)
  └── Minimum size respects S3 overhead

Abandonment
  ├── Failed tasks retry on different nodes (from world view)
  ├── Abandoned tasks report diagnostics
  └── Failure rate feeds Colony Temperature (via queue depth)
```

In v2, the full twenty-behavior interaction map creates emergent intelligence — the waggle dance recruits bees to productive work, pheromones steer the planner away from stressed nodes, scouts discover maintenance opportunities, and the colony temperature regulates all of it. But the v1 subset already provides the essential properties: resource safety, work sizing, failure recovery, and health visibility.

---

## Complete Behavior Reference

### v1 Active Behaviors

| Species | Behavior | Software Pattern | When It Matters |
|---|---|---|---|
| Mason Bees (Osmia) | Sealed Chambers | Isolated execution per bee | Every task execution |
| Leafcutter Bees (Megachile) | Precise Cutting | Cell sizing / task partitioning | Every write and query |
| Honeybee | Abandonment | Task retry with diagnostic | Failure handling |
| Honeybee | Colony Temperature | System homeostasis | Write backpressure, maintenance |

### v2 Behaviors

| Species | Behavior | Software Pattern | Requires |
|---|---|---|---|
| Honeybee | Waggle Dance | Quality-weighted task distribution | Gossip for propagation |
| Honeybee | Three-Tier Workers | Employed / Onlooker / Scout | Scout activities to discover |
| Honeybee | Pheromone Signalling | Backpressure via gossip | Gossip protocol |
| Honeybee | Trophallaxis | Metadata-rich data transfer | Arrow Flight |
| Honeybee | Nectar Ripening | Streaming data maturation | Local write-ahead log |
| Honeybee | Guard Bees | Node authentication | Direct node communication |
| Honeybee | Swarming | Colony division for scaling | Dynamic membership |
| Honeybee | Orientation Flights | Gradual node ramp-up | Gossip for load sharing |
| Honeybee | Propolis | Security auditing | — (can be v1 extension) |
| Honeybee | Winter Cluster | Degraded offline mode | Local write-ahead log |
| Stingless Bees | Tiered Storage | Cell classification | Intelligent cache tiering |
| Bumblebees | Buzz Pollination | Forced extraction | Specialised extractors |
| Cuckoo Bees | Adversarial Testing | Chaos engineering | System with defences to test |
| Carpenter Bees | Embedded Indexes | Bloom filters, zone maps | Sufficient data volume |
| Orchid Bees | Chemical Library | Metadata enrichment | Long-running system |
| Sweat Bees | Variable Sociality | Adaptive coordination | Gossip + direct comms |
