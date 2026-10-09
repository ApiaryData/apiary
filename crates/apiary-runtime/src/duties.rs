//! What a Node's Bees do.
//!
//! The Node never assigns a Bee work. It reads its own state into a stimulus for
//! each role, and does what the role calls for when a Bee answers:
//!
//! | Role | Stimulus | Work |
//! |---|---|---|
//! | Forager | queries waiting | run a query under the Bee's share of memory |
//! | Ripener | the crop is full or old; nectar is ready to cap; harvest is due | deposit, cap, harvest |
//! | Undertaker | clearing is due | delete what no table version needs |
//! | Scout | the Node's picture of its comb is stale | survey the comb for nectar ready to cap |
//!
//! Followers, Receivers and Guards have no stimulus yet: dances, tremble signals
//! and a handoff wait arrive in later phases, and the entrance still validates
//! deposits inline.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::execution::memory_pool::MemoryPool;
use futures::future::BoxFuture;
use tracing::{debug, warn};

use apiary_colony::{BeeContext, Calibration, Duties, Role, Stimuli};
use apiary_comb::Crop;
use apiary_core::{ApiaryError, Clock, Result, StorageBackend};

use crate::deposit::Depositor;
use crate::upkeep::Upkeep;

/// How much louder a waiting query calls each second it is unanswered.
const FORAGE_URGENCY_PER_SECOND: f64 = 40.0;

/// A query waiting for a Forager, given the pool it must run under.
pub type QueryPatch = Box<dyn FnOnce(Arc<dyn MemoryPool>) -> BoxFuture<'static, ()> + Send>;

/// How a Node's duties are paced.
#[derive(Clone, Debug)]
pub struct DutiesSettings {
    /// A crop this full is urgent.
    pub crop_max_bytes: u64,
    /// A crop this old is urgent.
    pub deposit_interval: Duration,
    /// How often the comb is looked at for nectar ready to cap.
    pub survey_interval: Duration,
    /// How often harvest is due.
    pub harvest_interval: Duration,
    /// How often clearing is due.
    pub clear_interval: Duration,
    /// The most queries that may wait; more are refused.
    pub query_limit: usize,
}

/// What the Scout last saw, and when each kind of work last ran.
struct Board {
    last_deposit: Duration,
    /// An earlier run left rows in the crop: the first deposit is due at once.
    deposit_overdue: bool,
    last_survey: Option<Duration>,
    last_harvest: Duration,
    last_clear: Duration,
    /// Groups of nectar the capping policy would cap now (the Scout's finding).
    ready_to_cap: usize,
    /// A deposit held back by a Frame's commit budget waits until then.
    deposit_not_before: Duration,
    /// Capping held back by the budget, or a failed pass, waits until then.
    cap_not_before: Duration,
    running: Running,
}

/// Which passes a Bee is doing now, so two Bees never do the same one at once.
#[derive(Default, Clone, Copy)]
struct Running {
    deposit: bool,
    cap: bool,
    harvest: bool,
    clear: bool,
    survey: bool,
}

/// A Node's duties, as its Bees see them.
pub struct NodeDuties {
    clock: Arc<dyn Clock>,
    settings: DutiesSettings,
    storage: Arc<dyn StorageBackend>,
    crop: Arc<Crop>,
    depositor: Arc<Depositor>,
    upkeep: Arc<Upkeep>,
    crop_bytes: Arc<AtomicU64>,
    /// Waiting queries, each with the time it joined the queue.
    queries: Mutex<VecDeque<(Duration, QueryPatch)>>,
    board: Mutex<Board>,
}

/// Which Ripener work is most urgent.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ripening {
    Deposit,
    Cap,
    Harvest,
}

impl NodeDuties {
    /// The duties of a Node.
    pub fn new(
        clock: Arc<dyn Clock>,
        settings: DutiesSettings,
        storage: Arc<dyn StorageBackend>,
        crop: Arc<Crop>,
        depositor: Arc<Depositor>,
        upkeep: Arc<Upkeep>,
        crop_bytes: Arc<AtomicU64>,
    ) -> Self {
        let now = clock.monotonic();
        let board = Board {
            last_deposit: now,
            deposit_overdue: crop_bytes.load(Ordering::Relaxed) > 0,
            last_survey: None,
            last_harvest: now,
            last_clear: now,
            ready_to_cap: 0,
            deposit_not_before: Duration::ZERO,
            cap_not_before: Duration::ZERO,
            running: Running::default(),
        };
        Self {
            clock,
            settings,
            storage,
            crop,
            depositor,
            upkeep,
            crop_bytes,
            queries: Mutex::default(),
            board: Mutex::new(board),
        }
    }

    /// Queue a query for a Forager. Refused if too many already wait: a Node
    /// that cannot keep up says so rather than queueing without end.
    pub fn submit_query(&self, patch: QueryPatch) -> Result<()> {
        let mut queue = self.queries.lock().expect("query queue");
        if queue.len() >= self.settings.query_limit {
            return Err(ApiaryError::Internal {
                message: format!(
                    "This node is overloaded: {} queries are already waiting",
                    queue.len()
                ),
            });
        }
        queue.push_back((self.clock.monotonic(), patch));
        Ok(())
    }

    /// How many queries wait for a Forager.
    pub fn queries_waiting(&self) -> usize {
        self.queries.lock().expect("query queue").len()
    }

    /// The forager stimulus: queries waiting, rising the longer the oldest has
    /// waited. A call nobody answers grows louder, so no query waits long on Bees
    /// whose forager thresholds have drifted high.
    fn forage_stimulus(&self) -> f64 {
        let queue = self.queries.lock().expect("query queue");
        let Some((since, _)) = queue.front() else {
            return 0.0;
        };
        let waited = self.clock.monotonic().saturating_sub(*since).as_secs_f64();
        queue.len() as f64 + FORAGE_URGENCY_PER_SECOND * waited
    }

    fn urgencies(&self) -> (f64, f64, f64) {
        let now = self.clock.monotonic();
        let board = self.board.lock().expect("duties board");
        let s = &self.settings;
        let bytes = self.crop_bytes.load(Ordering::Relaxed);

        let deposit = if board.running.deposit || bytes == 0 || now < board.deposit_not_before {
            0.0
        } else {
            let full = bytes as f64 / s.crop_max_bytes.max(1) as f64;
            let old = if board.deposit_overdue {
                1.0
            } else {
                now.saturating_sub(board.last_deposit).as_secs_f64()
                    / s.deposit_interval.as_secs_f64().max(1e-3)
            };
            full.max(old)
        };
        let cap = if board.running.cap || board.ready_to_cap == 0 || now < board.cap_not_before {
            0.0
        } else {
            1.0 + 0.1 * (board.ready_to_cap as f64 - 1.0).min(10.0)
        };
        let harvest = if board.running.harvest || !self.upkeep.harvests() {
            0.0
        } else {
            now.saturating_sub(board.last_harvest).as_secs_f64()
                / s.harvest_interval.as_secs_f64().max(1e-3)
        };
        (deposit, cap, harvest)
    }

    fn most_urgent(&self) -> Option<(Ripening, f64)> {
        let (deposit, cap, harvest) = self.urgencies();
        [
            (Ripening::Deposit, deposit),
            (Ripening::Cap, cap),
            (Ripening::Harvest, harvest),
        ]
        .into_iter()
        .filter(|(_, u)| *u >= 1.0)
        .max_by(|a, b| a.1.total_cmp(&b.1))
    }

    fn edit<R>(&self, f: impl FnOnce(&mut Board) -> R) -> R {
        f(&mut self.board.lock().expect("duties board"))
    }

    async fn deposit(&self) {
        self.edit(|b| b.running.deposit = true);
        let outcome = self.depositor.deposit_within_budget().await;
        let now = self.clock.monotonic();
        // What is left in the crop (a deposit takes a bounded chunk, and a Frame
        // out of budget keeps all of its own).
        let crop = Arc::clone(&self.crop);
        let left = tokio::task::spawn_blocking(move || crop.pending_bytes())
            .await
            .ok()
            .and_then(|r| r.ok());
        if let Some(left) = left {
            self.crop_bytes.store(left, Ordering::Relaxed);
        }
        self.edit(|b| {
            b.running.deposit = false;
            b.last_deposit = now;
            b.deposit_overdue = false;
            match &outcome {
                Ok((report, held_back)) => {
                    Depositor::log(report);
                    if report.segments > 0 {
                        // New nectar: have the Scout look again soon.
                        b.last_survey = None;
                    }
                    if *held_back {
                        // A Frame is out of budget: leave it to the minute.
                        b.deposit_not_before = now + Duration::from_secs(1);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Crop deposit failed; will retry");
                    b.deposit_not_before = now + Duration::from_secs(1);
                }
            }
        });
    }

    async fn cap(&self) {
        self.edit(|b| b.running.cap = true);
        let outcome = self.upkeep.cap_all().await;
        let now = self.clock.monotonic();
        self.edit(|b| {
            b.running.cap = false;
            b.ready_to_cap = 0;
            b.last_survey = None;
            match &outcome {
                Ok(report) if report.commits == 0 => {
                    // Nothing could be capped: a Frame is out of budget, or users
                    // keep getting in the way. Look again later.
                    b.cap_not_before = now + Duration::from_secs(1);
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(error = %e, "Capping pass failed");
                    b.cap_not_before = now + Duration::from_secs(1);
                }
            }
        });
    }

    async fn harvest(&self) {
        self.edit(|b| b.running.harvest = true);
        if let Err(e) = self.upkeep.harvest_all().await {
            warn!(error = %e, "Harvest pass failed");
        }
        let now = self.clock.monotonic();
        self.edit(|b| {
            b.running.harvest = false;
            b.last_harvest = now;
        });
    }

    async fn clear(&self) {
        self.edit(|b| b.running.clear = true);
        if let Err(e) = self.upkeep.clear_all().await {
            warn!(error = %e, "Clearing pass failed");
        }
        let now = self.clock.monotonic();
        self.edit(|b| {
            b.running.clear = false;
            b.last_clear = now;
        });
    }

    async fn survey(&self) {
        self.edit(|b| b.running.survey = true);
        let outcome = self.upkeep.survey().await;
        let now = self.clock.monotonic();
        self.edit(|b| {
            b.running.survey = false;
            b.last_survey = Some(now);
            match &outcome {
                Ok(survey) => {
                    b.ready_to_cap = survey.ready_groups;
                    debug!(
                        nectar_cells = survey.nectar_cells,
                        ready = survey.ready_groups,
                        "Surveyed the comb"
                    );
                }
                Err(e) => warn!(error = %e, "Surveying the comb failed"),
            }
        });
    }
}

#[async_trait]
impl Duties for NodeDuties {
    fn stimuli(&self) -> Stimuli {
        let now = self.clock.monotonic();
        let (deposit, cap, harvest) = self.urgencies();
        let ripener = deposit.max(cap).max(harvest);
        let (scout, undertaker) = {
            let board = self.board.lock().expect("duties board");
            let scout = if board.running.survey {
                0.0
            } else {
                match board.last_survey {
                    None => 1.0,
                    Some(at) => {
                        now.saturating_sub(at).as_secs_f64()
                            / self.settings.survey_interval.as_secs_f64().max(1e-3)
                    }
                }
            };
            let undertaker = if board.running.clear {
                0.0
            } else {
                now.saturating_sub(board.last_clear).as_secs_f64()
                    / self.settings.clear_interval.as_secs_f64().max(1e-3)
            };
            (scout, undertaker)
        };
        Stimuli::none()
            .with(Role::Forager, self.forage_stimulus())
            .with(Role::Ripener, ripener)
            .with(Role::Scout, scout)
            .with(Role::Undertaker, undertaker)
    }

    fn claimed_waiting(&self) -> usize {
        // A Forager starts the query it takes at once, so nothing is claimed and
        // waiting. Queries still in the queue are a stimulus, not a heat.
        0
    }

    async fn perform(&self, role: Role, bee: &BeeContext) -> bool {
        match role {
            Role::Forager => {
                let patch = self.queries.lock().expect("query queue").pop_front();
                match patch.map(|(_, patch)| patch) {
                    Some(patch) => {
                        patch(bee.pool()).await;
                        true
                    }
                    None => false,
                }
            }
            Role::Ripener => match self.most_urgent() {
                Some((Ripening::Deposit, _)) => {
                    self.deposit().await;
                    true
                }
                Some((Ripening::Cap, _)) => {
                    self.cap().await;
                    true
                }
                Some((Ripening::Harvest, _)) => {
                    self.harvest().await;
                    true
                }
                None => false,
            },
            Role::Undertaker => {
                let due = {
                    let board = self.board.lock().expect("duties board");
                    !board.running.clear
                        && self.clock.monotonic().saturating_sub(board.last_clear)
                            >= self.settings.clear_interval
                };
                if due {
                    self.clear().await;
                }
                due
            }
            Role::Scout => {
                let due = {
                    let board = self.board.lock().expect("duties board");
                    let now = self.clock.monotonic();
                    !board.running.survey
                        && board.last_survey.is_none_or(|at| {
                            now.saturating_sub(at) >= self.settings.survey_interval
                        })
                };
                if due {
                    self.survey().await;
                }
                due
            }
            // No stimulus yet: dances, tremble signals and a handoff wait come later.
            Role::Follower | Role::Receiver | Role::Guard => false,
        }
    }

    async fn calibrate(&self, _bee: &BeeContext) -> Option<Calibration> {
        // How long the comb store takes to answer from here...
        let started = self.clock.monotonic();
        let _ = self.storage.list("_registry/").await;
        let store_latency = self.clock.monotonic().saturating_sub(started);

        // ...and how fast this Bee scans. A simulation's virtual clock does not
        // move while a Bee computes, so there it reads zero, and a Bee that
        // cannot measure says so.
        const ROWS: usize = 1 << 20;
        let values = arrow::array::Int64Array::from_iter_values(0..ROWS as i64);
        let scan_started = self.clock.monotonic();
        let total = arrow::compute::sum(&values).unwrap_or(0);
        let elapsed = self
            .clock
            .monotonic()
            .saturating_sub(scan_started)
            .as_secs_f64();
        std::hint::black_box(total);
        let scan_rows_per_sec = if elapsed > 0.0 {
            ROWS as f64 / elapsed
        } else {
            0.0
        };
        Some(Calibration {
            scan_rows_per_sec,
            store_latency,
        })
    }
}
