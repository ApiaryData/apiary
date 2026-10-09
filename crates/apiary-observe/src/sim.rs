//! A run: a virtual clock, a seed, a trace, and a runtime to put them in.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::ThreadId;
use std::time::Duration;

use apiary_core::config::NodeConfig;
use apiary_core::rng::{SeededRng, StdSeededRng};
use apiary_core::{Clock, Env, NodeId};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};

use tracing_subscriber::layer::SubscriberExt;

use crate::marks::MarkLayer;
use crate::net::SimNetwork;
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
    /// Distinguishes this run's stores in the process-wide store registry, so
    /// runs going at once (tests do) never see each other's buckets.
    run_id: u64,
    registered: Arc<std::sync::Mutex<Vec<String>>>,
}

static NEXT_RUN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

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
        Self {
            seed,
            clock,
            trace,
            run_id: NEXT_RUN.fetch_add(1, Ordering::Relaxed),
            registered: Arc::default(),
        }
    }

    /// The registry authority for the store called `name` in this run.
    fn authority(&self, name: &str) -> String {
        format!("sim{}-{name}", self.run_id)
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
            // The Nodes' own marks become trace events for as long as the run lasts.
            let _marks = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(MarkLayer::new(sim.trace.clone())),
            );
            let started = sim.clock.monotonic();
            let trace = sim.trace.clone();
            let clock = sim.clock.clone();
            let registered = Arc::clone(&sim.registered);
            let value = scenario(sim).await;
            for authority in registered.lock().expect("registry list").drain(..) {
                apiary_comb::custom_store::unregister_store(&authority);
            }
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
        self.register(&store);
        store
    }

    fn register(&self, store: &SimStore) {
        let authority = self.authority(store.name());
        store.register(&authority);
        self.registered
            .lock()
            .expect("registry list")
            .push(authority);
    }

    /// A second way into the bucket `of`: the same objects, but its own faults. A
    /// Node given a view of the shared bucket can be cut off from it alone, which
    /// is how a Node that has lost its link looks to the rest of the colony.
    pub fn store_view(&self, of: &SimStore, name: &str) -> SimStore {
        let view = of.view(
            name,
            Arc::clone(&self.clock),
            self.trace.clone(),
            self.rng(&format!("store:{name}"), 0),
        );
        self.register(&view);
        view
    }

    /// A simulated network for the colony's transport.
    pub fn network(&self) -> SimNetwork {
        SimNetwork::new(
            Arc::clone(&self.clock),
            self.trace.clone(),
            self.rng("network", 0),
        )
    }

    /// The Apiary key for this run: the same seed always gives the same key.
    pub fn apiary_key(&self) -> apiary_net::ApiaryKey {
        apiary_net::ApiaryKey::from_bytes(&self.key_bytes("apiary", 0))
    }

    /// The key of the Node called `name`: fixed by the seed, so its id is too.
    pub fn node_key(&self, name: &str) -> apiary_net::NodeKey {
        apiary_net::NodeKey::from_bytes(&self.key_bytes(name, 1))
    }

    fn key_bytes(&self, owner: &str, index: u64) -> [u8; 32] {
        let mut rng = self.rng(&format!("key:{owner}"), index);
        let mut bytes = [0u8; 32];
        for chunk in bytes.chunks_mut(8) {
            chunk.copy_from_slice(&rng.next_u64().to_le_bytes());
        }
        bytes
    }

    /// The `storage_uri` that reaches the store named `name`.
    pub fn drive_uri(&self, name: &str) -> String {
        format!("apiary-drive://{}/", self.authority(name))
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
