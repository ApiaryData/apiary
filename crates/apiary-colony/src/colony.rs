//! A Node's Bees at work.
//!
//! Each Bee is a task that loops: wait until the Node is cool enough to claim
//! work, read the stimuli the Node shows, reconsider its role, and, if some role
//! calls, do one Patch of it. Nothing assigns a Bee its role; the Node only says
//! what is calling ([`Duties::stimuli`]) and what to do when a Bee answers
//! ([`Duties::perform`]).
//!
//! The Node's temperature is read from the Node itself: busy Bees over Bees,
//! memory reserved over the pool, the queue of waiting Patches over what the
//! Node tolerates, and the SoC's heat.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apiary_core::{Clock, Env};
use async_trait::async_trait;
use datafusion::common::Result as DfResult;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use tokio::sync::{Notify, watch};
use tracing::debug;

use crate::bee::{Bee, BeeParams, Calibration};
use crate::pool::CappedPool;
use crate::roles::{Role, Stimuli};
use crate::temperature::{
    NoThermal, TemperatureInputs, TemperatureRegulation, ThermalSensor, node_temperature,
};

/// What a Node tells its Bees, and what it does when one answers.
#[async_trait]
pub trait Duties: Send + Sync + 'static {
    /// What each role's stimulus is now, read from the Node's own state.
    fn stimuli(&self) -> Stimuli;

    /// How many Patches a Bee has claimed but not yet started (they wait on a
    /// memory reservation, say). This is part of the Node's temperature. Work
    /// that no Bee has claimed yet is a stimulus, not a heat: counting it would
    /// stop the very Bees that would drain it.
    fn claimed_waiting(&self) -> usize;

    /// A Bee that took up `role` asks what to do. Do one Patch of it and say
    /// whether there was one.
    async fn perform(&self, role: Role, bee: &BeeContext) -> bool;

    /// A calibration Patch: measure this Bee's scan rate and store latency.
    async fn calibrate(&self, _bee: &BeeContext) -> Option<Calibration> {
        None
    }
}

/// What a Patch can use of its Bee.
pub struct BeeContext {
    index: usize,
    id: String,
    budget: usize,
    pool: Arc<CappedPool>,
    reservation: Mutex<MemoryReservation>,
    clock: Arc<dyn Clock>,
}

impl BeeContext {
    /// The Bee's number.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The Bee's id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The most memory the Bee may reserve.
    pub fn budget(&self) -> usize {
        self.budget
    }

    /// The Bee's share of the Node's memory pool, to run operators under: they
    /// are refused beyond the share, and spill.
    pub fn pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.pool) as Arc<dyn MemoryPool>
    }

    /// Reserve `bytes` for the Patch, or fail if the Bee's share (or the Node's
    /// pool) cannot give them.
    pub fn try_reserve(&self, bytes: usize) -> DfResult<()> {
        self.reservation
            .lock()
            .expect("reservation lock")
            .try_grow(bytes)
    }

    /// Give back `bytes` reserved with [`try_reserve`](Self::try_reserve).
    pub fn release(&self, bytes: usize) {
        let r = self.reservation.lock().expect("reservation lock");
        let n = bytes.min(r.size());
        r.shrink(n);
    }

    /// How much the Bee holds now, by any route.
    pub fn reserved(&self) -> usize {
        self.pool.reserved()
    }

    /// The Node's clock.
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }
}

/// How a Node's colony is set up.
pub struct ColonyConfig {
    /// The Node's id (seeds each Bee's thresholds).
    pub node: String,
    /// How many Bees: one per core.
    pub bees: usize,
    /// How much memory each Bee may reserve.
    pub bee_budget: usize,
    /// The Node's memory pool.
    pub pool: Arc<dyn MemoryPool>,
    /// How Bees behave.
    pub params: BeeParams,
    /// The claimed-but-unstarted Patches the Node tolerates (temperature reads
    /// them over this).
    pub queue_limit: usize,
    /// How often a Bee looks again while something is calling.
    pub engaged_poll: Duration,
    /// How often a Bee looks again while nothing is.
    pub idle_poll: Duration,
    /// The SoC's heat.
    pub thermal: Arc<dyn ThermalSensor>,
}

impl ColonyConfig {
    /// A Node's colony with the usual pacing and no SoC sensor.
    pub fn new(node: &str, bees: usize, bee_budget: usize, pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            node: node.to_string(),
            bees,
            bee_budget,
            pool,
            params: BeeParams::default(),
            queue_limit: (bees * 2).max(2),
            engaged_poll: Duration::from_millis(10),
            idle_poll: Duration::from_millis(250),
            thermal: Arc::new(NoThermal),
        }
    }
}

/// A Bee as seen from outside.
#[derive(Clone, Debug)]
pub struct BeeSnapshot {
    /// The Bee's id.
    pub id: String,
    /// The role it holds, or last held.
    pub role: Role,
    /// Whether it is doing a Patch now.
    pub busy: bool,
    /// Whether it has stopped claiming because the Node is hot.
    pub cooling: bool,
    /// Completed Patches.
    pub age: u64,
    /// The memory it holds now.
    pub reserved: usize,
    /// The most it may hold.
    pub budget: usize,
    /// What it measured about itself.
    pub calibration: Option<Calibration>,
}

struct Slot {
    snapshot: Mutex<BeeSnapshot>,
    context: Arc<BeeContext>,
}

struct Shared {
    clock: Arc<dyn Clock>,
    duties: Arc<dyn Duties>,
    pool: Arc<dyn MemoryPool>,
    thermal: Arc<dyn ThermalSensor>,
    queue_limit: usize,
    engaged_poll: Duration,
    idle_poll: Duration,
    busy: AtomicUsize,
    foraging: AtomicUsize,
    slots: Vec<Slot>,
    wake: Notify,
    /// Counts nudges, so one that arrives while a Bee is busy is not lost.
    wake_count: AtomicU64,
    stop: watch::Sender<bool>,
}

impl Shared {
    fn temperature(&self) -> f64 {
        let bees = self.slots.len().max(1) as f64;
        let memory = match self.pool.memory_limit() {
            MemoryLimit::Finite(limit) if limit > 0 => self.pool.reserved() as f64 / limit as f64,
            _ => 0.0,
        };
        node_temperature(&TemperatureInputs {
            cpu: self.busy.load(Ordering::Relaxed) as f64 / bees,
            memory,
            queue: self.duties.claimed_waiting() as f64 / self.queue_limit.max(1) as f64,
            soc: self.thermal.soc_fraction(),
        })
    }

    fn forager_share(&self) -> f64 {
        self.foraging.load(Ordering::Relaxed) as f64 / self.slots.len().max(1) as f64
    }

    /// Wait for a nudge, the next look, or the end. `seen` is the nudge count
    /// the Bee read before it looked: a nudge since then ends the wait at once.
    async fn wait(&self, calling: bool, seen: u64, stop: &mut watch::Receiver<bool>) {
        let poll = if calling {
            self.engaged_poll
        } else {
            self.idle_poll
        };
        let nudged = self.wake.notified();
        tokio::pin!(nudged);
        nudged.as_mut().enable();
        if self.wake_count.load(Ordering::Acquire) != seen {
            return;
        }
        // The poll is the scheduler's own pacing, not the Node's notion of time, so
        // it runs on the runtime's timer: the same as the Node's clock in
        // production and in a simulation (whose virtual clock is the runtime's),
        // and still moving when a test holds the Node's clock still.
        tokio::select! {
            () = nudged => {}
            () = tokio::time::sleep(poll) => {}
            _ = stop.changed() => {}
        }
    }

    fn snapshot(&self, bee: &Bee, busy: bool) {
        let slot = &self.slots[bee.index()];
        let mut s = slot.snapshot.lock().expect("snapshot lock");
        s.role = bee.role();
        s.busy = busy;
        s.cooling = bee.is_cooling();
        s.age = bee.age();
        s.reserved = slot.context.reserved();
        s.calibration = bee.calibration();
    }
}

/// A Node's Bees.
pub struct Colony {
    shared: Arc<Shared>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Colony {
    /// Start the Bees. They run on `runtime` if given (the Node's CPU runtime),
    /// otherwise on the current one.
    pub fn start(
        config: ColonyConfig,
        env: &Env,
        duties: Arc<dyn Duties>,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        let clock = env.clock();
        let (stop, _) = watch::channel(false);
        let mut bees = Vec::new();
        let mut slots = Vec::new();
        for index in 0..config.bees {
            let bee = Bee::new(&config.node, index, env, config.params);
            let capped = Arc::new(CappedPool::new(
                bee.id().to_string(),
                Arc::clone(&config.pool),
                config.bee_budget,
            ));
            let pool: Arc<dyn MemoryPool> = Arc::clone(&capped) as Arc<dyn MemoryPool>;
            let reservation = MemoryConsumer::new(format!("{}/patch", bee.id())).register(&pool);
            let context = Arc::new(BeeContext {
                index,
                id: bee.id().to_string(),
                budget: config.bee_budget,
                pool: capped,
                reservation: Mutex::new(reservation),
                clock: Arc::clone(&clock),
            });
            slots.push(Slot {
                snapshot: Mutex::new(BeeSnapshot {
                    id: bee.id().to_string(),
                    role: bee.role(),
                    busy: false,
                    cooling: false,
                    age: 0,
                    reserved: 0,
                    budget: config.bee_budget,
                    calibration: None,
                }),
                context,
            });
            bees.push(bee);
        }
        let shared = Arc::new(Shared {
            clock,
            duties,
            pool: config.pool,
            thermal: config.thermal,
            queue_limit: config.queue_limit,
            engaged_poll: config.engaged_poll,
            idle_poll: config.idle_poll,
            busy: AtomicUsize::new(0),
            foraging: AtomicUsize::new(0),
            slots,
            wake: Notify::new(),
            wake_count: AtomicU64::new(0),
            stop,
        });
        let tasks = bees
            .into_iter()
            .map(|bee| {
                let shared = Arc::clone(&shared);
                let stop = shared.stop.subscribe();
                let work = run_bee(bee, shared, stop);
                match &runtime {
                    Some(handle) => handle.spawn(work),
                    None => tokio::spawn(work),
                }
            })
            .collect();
        Self {
            shared,
            tasks: Mutex::new(tasks),
        }
    }

    /// Tell the Bees something new is calling.
    pub fn wake(&self) {
        self.shared.wake_count.fetch_add(1, Ordering::Release);
        self.shared.wake.notify_waiters();
    }

    /// The Node's temperature now, in `[0, 1]`.
    pub fn temperature(&self) -> f64 {
        self.shared.temperature()
    }

    /// How the temperature reads against the band the Node aims for.
    pub fn regulation(&self) -> TemperatureRegulation {
        TemperatureRegulation::of(self.temperature())
    }

    /// Every Bee as seen from outside.
    pub fn bees(&self) -> Vec<BeeSnapshot> {
        self.shared
            .slots
            .iter()
            .map(|slot| {
                let mut s = slot.snapshot.lock().expect("snapshot lock").clone();
                s.reserved = slot.context.reserved();
                s
            })
            .collect()
    }

    /// How many Bees hold each role now.
    pub fn role_counts(&self) -> [(Role, usize); 7] {
        let bees = self.bees();
        Role::ALL.map(|role| (role, bees.iter().filter(|b| b.role == role).count()))
    }

    /// The Bees' contexts, for work that wants one outside a Patch (tests).
    pub fn context(&self, index: usize) -> Arc<BeeContext> {
        Arc::clone(&self.shared.slots[index].context)
    }

    /// Stop the Bees. A Patch in flight finishes first.
    pub async fn shutdown(&self) {
        let _ = self.shared.stop.send(true);
        self.wake();
        let tasks: Vec<_> = std::mem::take(&mut *self.tasks.lock().expect("task lock"));
        for task in tasks {
            let _ = task.await;
        }
    }
}

async fn run_bee(mut bee: Bee, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    let context = Arc::clone(&shared.slots[bee.index()].context);
    let mut last = shared.clock.monotonic();
    loop {
        if *stop.borrow() {
            return;
        }
        let seen = shared.wake_count.load(Ordering::Acquire);

        // A new Bee's first Patches measure itself.
        if bee.needs_calibration() {
            shared.busy.fetch_add(1, Ordering::Relaxed);
            shared.snapshot(&bee, true);
            let measured = shared.duties.calibrate(&context).await;
            shared.busy.fetch_sub(1, Ordering::Relaxed);
            bee.finish_calibration(measured);
            shared.snapshot(&bee, false);
            last = shared.clock.monotonic();
            continue;
        }

        // Too hot: stop claiming, finish nothing new, look again later.
        if !bee.may_claim(shared.temperature()) {
            shared.snapshot(&bee, false);
            shared.wait(false, seen, &mut stop).await;
            continue;
        }

        let mut stimuli = shared.duties.stimuli();
        stimuli.forager_share = shared.forager_share();
        let now = shared.clock.monotonic();
        bee.tick(None, now.saturating_sub(last));
        last = now;

        let decision = bee.reconsider(&stimuli, now);
        shared.snapshot(&bee, false);
        if !decision.engaged {
            shared
                .wait(stimuli.strongest() > 0.0, seen, &mut stop)
                .await;
            continue;
        }

        shared.busy.fetch_add(1, Ordering::Relaxed);
        if decision.role == Role::Forager {
            shared.foraging.fetch_add(1, Ordering::Relaxed);
        }
        shared.snapshot(&bee, true);
        let worked = shared.duties.perform(decision.role, &context).await;
        if decision.role == Role::Forager {
            shared.foraging.fetch_sub(1, Ordering::Relaxed);
        }
        shared.busy.fetch_sub(1, Ordering::Relaxed);

        let done = shared.clock.monotonic();
        bee.tick(worked.then_some(decision.role), done.saturating_sub(last));
        last = done;
        if worked {
            bee.finish_patch();
            debug!(bee = bee.id(), role = decision.role.name(), "Patch done");
        }
        shared.snapshot(&bee, false);
        if !worked {
            // The stimulus called but there was nothing to take: look again soon,
            // not at once.
            shared.wait(true, seen, &mut stop).await;
        }
    }
}
