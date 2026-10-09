//! A run: a virtual clock, a seed, a trace, and a runtime to put them in.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::ThreadId;
use std::time::Duration;

use apiary_core::config::NodeConfig;
use apiary_core::rng::StdSeededRng;
use apiary_core::{Clock, Env, NodeId};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};

use crate::store::SimStore;
use crate::trace::Trace;

/// Where the wall clock starts in a simulation: after the commit gate's earliest
/// plausible date, so Nodes commit, and nowhere near a real clock reading.
pub fn wall_origin() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("a valid date")
}

/// The simulation's clock. Monotonic time is Tokio's, which under [`Sim::run`] is
/// virtual and only moves when every task is waiting; the wall clock is that time
/// added to a fixed origin.
///
/// Tokio's virtual clock belongs to the runtime that owns it. Some code the Node
/// uses runs on a thread of its own (Delta's kernel does its store reads on a
/// private thread while the Node's thread waits), and a clock read there would
/// be a different, real clock. So off the simulation's thread, `monotonic` returns
/// the last time the simulation's thread saw, which is exactly right: that
/// thread is waiting for the other one, so time has not moved.
#[derive(Debug)]
pub struct SimClock {
    origin: tokio::time::Instant,
    wall_origin: DateTime<Utc>,
    main: ThreadId,
    published: AtomicU64,
}

impl SimClock {
    /// A clock that starts now (virtual) at `wall_origin`. Call it on the
    /// simulation's thread.
    pub fn new(wall_origin: DateTime<Utc>) -> Self {
        Self {
            origin: tokio::time::Instant::now(),
            wall_origin,
            main: std::thread::current().id(),
            published: AtomicU64::new(0),
        }
    }

    /// Whether the caller is on the simulation's own thread.
    pub fn on_main_thread(&self) -> bool {
        std::thread::current().id() == self.main
    }
}

#[async_trait]
impl Clock for SimClock {
    fn now_utc(&self) -> DateTime<Utc> {
        self.wall_origin
            + chrono::Duration::from_std(self.monotonic()).expect("virtual time fits chrono")
    }

    fn monotonic(&self) -> Duration {
        if self.on_main_thread() {
            let now = self.origin.elapsed();
            self.published
                .store(now.as_nanos() as u64, Ordering::Relaxed);
            now
        } else {
            Duration::from_nanos(self.published.load(Ordering::Relaxed))
        }
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// What a scenario sees: the seed, the clock and the trace.
#[derive(Clone)]
pub struct Sim {
    seed: u64,
    clock: Arc<SimClock>,
    trace: Trace,
}

/// The outcome of [`Sim::run`].
pub struct Run<T> {
    /// What the scenario returned.
    pub value: T,
    /// Everything marked during the run.
    pub trace: Trace,
    /// How much virtual time the run took.
    pub elapsed: Duration,
}

impl Sim {
    fn new(seed: u64) -> Self {
        let clock = Arc::new(SimClock::new(wall_origin()));
        let trace = Trace::new(clock.clone());
        Self { seed, clock, trace }
    }

    /// Run `scenario` on a single-threaded runtime with a virtual clock. The same
    /// seed (and the same scenario) gives the same run.
    pub fn run<T, F, Fut>(seed: u64, scenario: F) -> Run<T>
    where
        F: FnOnce(Sim) -> Fut,
        Fut: Future<Output = T>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("simulation runtime");
        runtime.block_on(async {
            let sim = Sim::new(seed);
            let started = sim.clock.monotonic();
            let trace = sim.trace.clone();
            let clock = sim.clock.clone();
            let value = scenario(sim).await;
            Run {
                value,
                trace,
                elapsed: clock.monotonic() - started,
            }
        })
    }

    /// The seed this run came from.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The environment to start Nodes with: the virtual clock and the seed.
    pub fn env(&self) -> Env {
        Env::new(self.clock.clone(), self.seed).with_inline_cpu()
    }

    /// The virtual clock.
    pub fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }

    /// The trace.
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// An independent random stream for `owner` and `index`, fixed by the seed.
    pub fn rng(&self, owner: &str, index: u64) -> StdSeededRng {
        self.env().rng(owner, index)
    }

    /// Wait `duration` of virtual time.
    pub async fn sleep(&self, duration: Duration) {
        self.clock.sleep(duration).await;
    }

    /// A comb store named `name`, registered so [`drive_uri`](Self::drive_uri)
    /// opens it. Its faults start at none; change them with [`SimStore::faults`].
    pub fn store(&self, name: &str) -> SimStore {
        let store = SimStore::new(
            name,
            Arc::clone(&self.clock),
            self.trace.clone(),
            self.rng(&format!("store:{name}"), 0),
        );
        store.register();
        store
    }

    /// The `storage_uri` that reaches the store named `name`.
    pub fn drive_uri(&self, name: &str) -> String {
        format!("apiary-drive://sim-{name}/")
    }

    /// A Node configuration for the Node called `name`, on the store `store`:
    /// a Node id fixed by the seed, two cores, a small memory budget, and a cache
    /// under `cache_dir`.
    pub fn node_config(&self, name: &str, store: &str, cache_dir: &std::path::Path) -> NodeConfig {
        let mut config = NodeConfig::detect(self.drive_uri(store));
        config.node_id = NodeId::generate_with(&mut self.rng(&format!("node:{name}"), 0));
        config.cores = 2;
        config.memory_per_bee = 64 * 1024 * 1024;
        config.cache_dir = cache_dir.to_path_buf();
        config
    }
}

impl std::fmt::Debug for Sim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sim(seed {})", self.seed)
    }
}
