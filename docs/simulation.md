# The observation hive

`apiary-observe` runs the real engine and the real membership layer on a virtual clock, a
simulated bucket and a simulated network, and replays any run exactly from its seed.
Seeley studied colonies in glass-walled hives with paint-marked bees; this is Apiary's
version. It is a test harness: nothing depends on it at run time.

## A first scenario

```rust
#[test]
fn a_node_survives_a_bucket_outage() {
    let run = Sim::run(7, |sim| async move {
        let store = sim.store("bucket");
        store.set_faults(|f| f.latency = Duration::from_millis(20));
        let config = sim.node_config("a", "bucket", cache_dir.path());
        let node = ApiaryNode::start_with_env(config, sim.env()).await.unwrap();
        // ... ingest, take the bucket down with store.set_down(true), flush, bring it back ...
    });
    // run.trace holds everything that happened; run.elapsed is virtual time.
}
```

`Sim::run(seed, scenario)` builds a single-threaded runtime whose clock is virtual: a
sleep costs no real time, and time advances only when every task is waiting. The same
code with the same seed gives the same run, to the event. A failing seed is a
reproduction: `Sim::run(<seed>, ...)` and read `run.trace.render()`.

## What is simulated

| Piece | What it does |
|---|---|
| `Sim` / `SimClock` | Virtual monotonic and wall time (wall starts 2040-01-01, after any date a test runs on), the seed, seeded keys and Node ids. |
| `SimStore` | An in-memory comb bucket with latency and jitter, a requests-per-second ceiling, transient errors, replies lost after the write landed, and outages (now, or scheduled). `Sim::store_view` gives each Node its own way into a shared bucket, so one Node can be cut off alone. |
| `SimNetwork` | The colony's `Transport` over delayed in-memory streams: per-link latency, jitter, loss and bandwidth; site partitions and isolated Nodes; NAT kinds; a relay that is none, plain or TLS, and can fail. |
| `Colony` | Real `Mesh`es (admission, revocation, control protocols) with seeded keys on a `SimNetwork`. |
| `Trace` | Every store request, connection, cut, path change and Node deed, with Node, Bee and virtual time, and a `digest()` that fingerprints the run. |

The NAT model is what the Phase 2 gate measured with real NAT: Nodes on one site and
Nodes dialling a public Node are direct; two Nodes behind ordinary NATs, or a public one
dialling a NATed one, go through the relay and, if the relay has TLS and address
discovery, punch through after a moment; a symmetric NAT stays relayed.

## Marked bees

A Node records what it does (an ingest, a deposit, a cap, a harvest, a query) as a
`tracing` event on the `apiary::mark` target. In production that is an info log line.
Under a `Sim`, `MarkLayer` copies each into the trace with the Node's id and the virtual
time. Bees arrive in phase 4; the layer already reads a `bee` field, and `Trace::mark`
takes one.

## Determinism: what the Node does to make it hold, and what it cannot

The production code is unchanged apart from two seams.

- **`Env`** carries the clock and seed through a Node. Everything that sleeps or reads
  time goes through it. The sweeps found one leak (the registry stamped hives, boxes and
  frames with the system clock); it now uses the Node's clock. The Bees' own polling runs
  on the runtime's timer, which in a simulation is the virtual clock.
- Queries run as Forager Patches on the Node's runtime (see `docs/colony.md`), not on
  blocking threads, so a query waiting for a simulated bucket does not hold virtual time
  still. Real disks are the other thing to keep out of a scenario: a blocking read
  finishes in real time, and the order several finish in would leak into the run, so
  hosts in a simulation serve from memory.
- **Delta's kernel** reads the log on a private thread while the caller waits, so the
  simulator keeps a simulated bucket's clock reads and latency consistent across that
  thread (a request there takes no virtual time) and records Delta's random data-file
  names as `<id>` and says nothing of a commit's size (it holds real timings).

## Not covered

- **A Delta table over the drive inside a simulation.** The same kernel bridge blocks the
  simulation's one thread while the host, in the same simulation, would have to answer.
  The drive protocol itself is simulated through the object-store interface (put,
  get, list, create-if-absent, across NAT, a lossy link and a relay outage), and Delta
  over the drive is covered over real QUIC by `apiary-net`'s tests and the Phase 2 gate.
- **Real time.** A simulated bucket's latency is virtual; nothing here measures speed.

## Seed sweeps

Each scenario family has a sweep that runs every seed twice and compares digests:
`SIM_SEEDS=500 cargo test -p apiary-observe` widens it from the default 20. If a seed does
not replay, the test prints the first event where the two runs differ.
