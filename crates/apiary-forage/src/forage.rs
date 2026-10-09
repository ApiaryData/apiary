//! A Node's foraging: planning queries, advertising their stages, following
//! dances, claiming and running Patches, and answering the caller.
//!
//! Four of a Bee's roles are here, each a few steps:
//!
//! - A **Scout** plans a newly submitted query and dances for its first stages;
//!   later it notices stages whose inputs are complete that nobody advertises,
//!   and Patches claimed by Bees that have gone quiet or slow, and advertises them.
//! - A **Forager** pulls the next Patch of a query this Node planned, nearest data
//!   first: stages that read rows only this Node holds go before the rest.
//! - A **Follower** samples dances from this Node's floor copy, picks one in
//!   proportion to its strength, claims a random Patch of its group and runs it.
//! - A **Receiver** runs the result stage of a query this Node planned, and
//!   answers the caller.
//!
//! Nothing here asks another Node for anything. What a Bee knows of the colony's
//! work is its Node's copy of the floor, plus the comb store, which settles who
//! finished a Patch first.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::MemoryPool;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use apiary_core::rng::hash_str;
use apiary_core::{Clock, Result, SeededRng, StdSeededRng};
use apiary_floor::{ClaimState, Dance, Entry, EntryBody, EntryKey, EntryKind, Floor, SoftClaim};
use apiary_net::NodeId;
use apiary_plan::exchange::{Completed, PatchKey};

use crate::config::{ForageConfig, Mode};
use crate::exec::Executor;
use crate::plan::{QueryPlan, StageInfo};
use crate::profit::{Profit, circuits_after, circuits_left, initial_circuits, lifetime_of};

/// What foraging has done on a Node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ForageCounts {
    /// Queries planned here.
    pub planned: u64,
    /// Patch attempts started.
    pub attempts: u64,
    /// Attempts whose output became the Patch's.
    pub won: u64,
    /// Attempts that finished second, so their work was discarded.
    pub lost: u64,
    /// Attempts that failed.
    pub failed: u64,
    /// Claims given up to a Bee with a lower id.
    pub yields: u64,
    /// Dances posted or renewed.
    pub dances: u64,
    /// Patches run again because they had been running far longer than the median.
    pub redone: u64,
    /// Results delivered.
    pub answered: u64,
    /// Queries small enough to be run whole by the Bee that planned them.
    pub whole: u64,
    /// Rows in the output of the attempts that won: the colony's useful work.
    pub rows: u64,
}

#[derive(Default)]
struct Counters {
    planned: AtomicU64,
    attempts: AtomicU64,
    won: AtomicU64,
    lost: AtomicU64,
    failed: AtomicU64,
    yields: AtomicU64,
    dances: AtomicU64,
    redone: AtomicU64,
    answered: AtomicU64,
    whole: AtomicU64,
    rows: AtomicU64,
}

/// A query's answer: its rows, and the schema they have even when there are none.
#[derive(Clone, Debug)]
pub struct Answer {
    /// The rows.
    pub batches: Vec<RecordBatch>,
    /// The schema of the result.
    pub schema: arrow::datatypes::SchemaRef,
}

type Reply = oneshot::Sender<Result<Answer>>;

struct Submitted {
    query: String,
    sql: String,
    reply: Reply,
}

/// A query this Node planned.
struct Home {
    plan: Arc<dyn QueryPlan>,
    reply: Option<Reply>,
    /// When progress was last checked against the store.
    checked: u64,
    result_started: bool,
}

/// The Patches of a stage, as a dance names them.
struct Group {
    query: String,
    stage: usize,
    patches: usize,
    pinned: Option<NodeId>,
}

/// A dance this Bee is performing for a stage.
struct Dancing {
    c0: f64,
    renewals: u32,
}

type PatchId = (String, usize, usize);

struct State {
    queue: VecDeque<Submitted>,
    homes: BTreeMap<String, Home>,
    /// Plans of queries other Nodes planned.
    plans: HashMap<String, Arc<dyn QueryPlan>>,
    /// Stages known to be complete. Completion never reverses.
    complete: HashSet<(String, usize)>,
    /// Patches known to be complete.
    done: HashSet<PatchId>,
    /// Patches a Bee on this Node is running now.
    running: HashSet<PatchId>,
    /// Patches that failed here recently, and when to try them again.
    cooldown: HashMap<PatchId, u64>,
    dancing: HashMap<(String, usize, u32), Dancing>,
    /// Groups with stragglers as of `suspects_at`, so reading the stimulus is cheap.
    suspects: usize,
    suspects_at: u64,
    profit: Profit,
    /// Share of recent attempts whose work was surplus.
    duplicates: f64,
    next_query: u64,
    next_attempt: u64,
}

/// A Node's foraging.
pub struct Forage {
    me: NodeId,
    label: String,
    floor: Arc<dyn Floor>,
    exec: Arc<dyn Executor>,
    clock: Arc<dyn Clock>,
    cfg: ForageConfig,
    state: Mutex<State>,
    counters: Counters,
}

impl Forage {
    /// Foraging for the Node `me`, called `label` in query ids and traces.
    pub fn new(
        me: NodeId,
        label: impl Into<String>,
        floor: Arc<dyn Floor>,
        exec: Arc<dyn Executor>,
        clock: Arc<dyn Clock>,
        cfg: ForageConfig,
    ) -> Arc<Self> {
        let profit = Profit::new(cfg.profit_window);
        Arc::new(Self {
            me,
            label: label.into(),
            floor,
            exec,
            clock,
            cfg,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                homes: BTreeMap::new(),
                plans: HashMap::new(),
                complete: HashSet::new(),
                done: HashSet::new(),
                running: HashSet::new(),
                cooldown: HashMap::new(),
                dancing: HashMap::new(),
                suspects: 0,
                suspects_at: 0,
                profit,
                duplicates: 0.0,
                next_query: 0,
                next_attempt: 0,
            }),
            counters: Counters::default(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("foraging state poisoned")
    }

    fn now(&self) -> u64 {
        self.floor.now()
    }

    /// What foraging has done so far.
    pub fn counts(&self) -> ForageCounts {
        let c = &self.counters;
        ForageCounts {
            planned: c.planned.load(Ordering::Relaxed),
            attempts: c.attempts.load(Ordering::Relaxed),
            won: c.won.load(Ordering::Relaxed),
            lost: c.lost.load(Ordering::Relaxed),
            failed: c.failed.load(Ordering::Relaxed),
            yields: c.yields.load(Ordering::Relaxed),
            dances: c.dances.load(Ordering::Relaxed),
            redone: c.redone.load(Ordering::Relaxed),
            answered: c.answered.load(Ordering::Relaxed),
            whole: c.whole.load(Ordering::Relaxed),
            rows: c.rows.load(Ordering::Relaxed),
        }
    }

    /// The share of this Node's recent attempts whose work was surplus.
    pub fn duplicate_rate(&self) -> f64 {
        self.lock().duplicates
    }

    /// Queries waiting for a Scout, planned here and not yet answered.
    pub fn queries_in_flight(&self) -> usize {
        let st = self.lock();
        st.queue.len() + st.homes.len()
    }

    /// Submit a query. A Scout plans it and the colony runs it; the answer comes
    /// back on the returned channel.
    pub fn submit(&self, sql: impl Into<String>) -> oneshot::Receiver<Result<Answer>> {
        let (reply, answer) = oneshot::channel();
        let mut st = self.lock();
        st.next_query += 1;
        let query = format!("{}-q{:06}", self.label, st.next_query);
        st.queue.push_back(Submitted {
            query,
            sql: sql.into(),
            reply,
        });
        answer
    }

    fn rng(&self, bee: u32) -> StdSeededRng {
        let mut st = self.lock();
        st.next_attempt += 1;
        StdSeededRng::derive(
            self.cfg.seed,
            &[hash_str(&self.label), u64::from(bee), st.next_attempt],
        )
    }

    // -----------------------------------------------------------------------
    // What is on the floor
    // -----------------------------------------------------------------------

    /// Every live dance with a body.
    fn dances(&self) -> Vec<(Entry, Dance)> {
        self.floor
            .entries(EntryKind::Dance)
            .into_iter()
            .filter_map(|e| match &e.body {
                EntryBody::Dance(d) => {
                    let d = d.clone();
                    Some((e, d))
                }
                _ => None,
            })
            .collect()
    }

    /// The live claims on the Patches of one stage, by Patch.
    fn claims(&self, query: &str, stage: usize) -> HashMap<usize, Vec<(Entry, SoftClaim)>> {
        let prefix = format!("{query}/{stage}/");
        let mut by_patch: HashMap<usize, Vec<(Entry, SoftClaim)>> = HashMap::new();
        for entry in self.floor.entries_for(EntryKind::SoftClaim, &prefix) {
            let EntryBody::SoftClaim(claim) = &entry.body else {
                continue;
            };
            let claim = claim.clone();
            if let Some(patch) = entry
                .key
                .target
                .rsplit('/')
                .next()
                .and_then(|p| p.parse::<usize>().ok())
            {
                by_patch.entry(patch).or_default().push((entry, claim));
            }
        }
        by_patch
    }

    /// Patches of a stage that a Bee here could run now: not known to be done, not
    /// running here, not recently failed, and not claimed by anyone else (unless a
    /// Scout asked for them to be run again).
    fn claimable(
        &self,
        st: &mut State,
        query: &str,
        info: &StageInfo,
        redo: &[usize],
    ) -> Vec<usize> {
        let now = self.now();
        let claims = self.claims(query, info.id);
        for (patch, entries) in &claims {
            if entries.iter().any(|(_, c)| c.state == ClaimState::Done) {
                st.done.insert((query.to_string(), info.id, *patch));
            }
        }
        st.cooldown.retain(|_, until| *until > now);
        (0..info.patches)
            .filter(|p| {
                let id = (query.to_string(), info.id, *p);
                if st.done.contains(&id)
                    || st.running.contains(&id)
                    || st.cooldown.contains_key(&id)
                {
                    return false;
                }
                match claims.get(p) {
                    None => true,
                    // A Patch with one Bee on it may be run again if a Scout asked;
                    // never a third time.
                    Some(entries) => entries.is_empty() || (redo.contains(p) && entries.len() < 2),
                }
            })
            .collect()
    }

    /// The Patch a Bee here takes from `open` (sorted): the first at or after this
    /// Node's own point on the ring of the group, wrapping round. Bees on one Node
    /// take neighbouring Patches, and Nodes start far apart, so the first wave of a
    /// group (every Bee reacting before gossip tells any Node of another's claims)
    /// seldom lands two Bees on one Patch.
    fn take_from(&self, query: &str, stage: usize, patches: usize, open: &[usize]) -> usize {
        let start =
            (hash_str(&format!("{}:{query}:{stage}", self.label)) % patches.max(1) as u64) as usize;
        open.iter()
            .copied()
            .find(|p| *p >= start)
            .unwrap_or(open[0])
    }

    /// Whether the claims on the floor say every Patch of `stage` is done. A hint
    /// only: the comb store has the final word.
    fn floor_says_done(&self, query: &str, stage: usize, patches: usize) -> bool {
        let claims = self.claims(query, stage);
        patches > 0
            && (0..patches).all(|p| {
                claims
                    .get(&p)
                    .is_some_and(|c| c.iter().any(|(_, c)| c.state == ClaimState::Done))
            })
    }

    /// Whether every stage feeding `info` is known, or looks from the floor, to be
    /// complete.
    fn inputs_look_done(&self, st: &State, plan: &Arc<dyn QueryPlan>, info: &StageInfo) -> bool {
        info.inputs.iter().all(|i| {
            st.complete.contains(&(plan.query().to_string(), *i))
                || plan
                    .stage(*i)
                    .is_some_and(|input| self.floor_says_done(plan.query(), *i, input.patches))
        })
    }

    /// Whether every stage feeding `info` is known to be complete.
    fn ready(st: &State, query: &str, info: &StageInfo) -> bool {
        info.inputs
            .iter()
            .all(|i| st.complete.contains(&(query.to_string(), *i)))
    }

    /// The groups of advertised Patches a Follower here could join: the dance
    /// entries behind each, and the Patches still to claim.
    fn candidates(&self, st: &mut State) -> Vec<(Entry, Dance, Vec<usize>)> {
        let mut out = Vec::new();
        let mut seen: HashMap<(String, usize), Vec<usize>> = HashMap::new();
        for (entry, dance) in self.dances() {
            if dance.pinned.is_some_and(|node| node != self.me) {
                continue;
            }
            let group = (dance.query.clone(), dance.stage);
            let open = match seen.get(&group) {
                Some(open) => open.clone(),
                None => {
                    let info = StageInfo {
                        id: dance.stage,
                        patches: dance.patches,
                        inputs: vec![],
                        pinned: false,
                        result: false,
                    };
                    let open = self.claimable(st, &dance.query, &info, &dance.redo);
                    seen.insert(group, open.clone());
                    open
                }
            };
            if !open.is_empty() {
                out.push((entry, dance, open));
            }
        }
        out
    }

    // -----------------------------------------------------------------------
    // Stimuli
    // -----------------------------------------------------------------------

    /// What calls a Scout: queries waiting to be planned, and queries planned here
    /// that are due a look at their progress.
    pub fn scout_stimulus(&self) -> f64 {
        let mut st = self.lock();
        let now = self.now();
        let poll = self.cfg.poll.as_millis() as u64;
        let due = st
            .homes
            .values()
            .filter(|h| !h.result_started && now.saturating_sub(h.checked) >= poll)
            .count();
        // Any Scout can see a Patch that is running far too long on the floor.
        let suspects = if now.saturating_sub(st.suspects_at) >= poll {
            st.suspects = self.stragglers().len();
            st.suspects_at = now;
            st.suspects
        } else {
            st.suspects
        };
        (st.queue.len() + due + suspects) as f64
    }

    /// What calls a Forager: Patches of queries planned here that can run now.
    pub fn forager_stimulus(&self) -> f64 {
        let mut st = self.lock();
        let homes: Vec<Arc<dyn QueryPlan>> =
            st.homes.values().map(|h| Arc::clone(&h.plan)).collect();
        let mut total = 0usize;
        for plan in homes {
            for info in plan.stages().iter().filter(|s| !s.result) {
                if Self::ready(&st, plan.query(), info) {
                    total += self.claimable(&mut st, plan.query(), info, &[]).len();
                }
            }
        }
        total as f64
    }

    /// What calls a Follower: the strength of the dances for Patches still
    /// unclaimed, a flat dance counting one.
    pub fn follower_stimulus(&self) -> f64 {
        let mut st = self.lock();
        let candidates = self.candidates(&mut st);
        let now = self.now();
        candidates
            .iter()
            .map(|(entry, _, _)| {
                (circuits_left(entry.remaining(now), &self.cfg) / self.cfg.flat_circuits)
                    .clamp(0.25, 2.0)
            })
            .sum()
    }

    /// What calls a Receiver: queries planned here whose result stage can run.
    pub fn receiver_stimulus(&self) -> f64 {
        let st = self.lock();
        st.homes
            .values()
            .filter(|h| {
                !h.result_started && self.inputs_look_done(&st, &h.plan, h.plan.result_stage())
            })
            .count() as f64
    }

    // -----------------------------------------------------------------------
    // Scouting
    // -----------------------------------------------------------------------

    /// Do a Scout's work: plan a waiting query, or look over the queries planned
    /// here and advertise what needs it.
    pub async fn scout(self: &Arc<Self>, bee: u32, pool: Option<Arc<dyn MemoryPool>>) -> bool {
        let next = self.lock().queue.pop_front();
        if let Some(sub) = next {
            self.plan_query(bee, sub, pool).await;
            return true;
        }
        self.forget_finished();
        let advertised = self.advertise_stragglers(bee);
        let homes: Vec<Arc<dyn QueryPlan>> = {
            let st = self.lock();
            st.homes
                .values()
                .filter(|h| !h.result_started)
                .map(|h| Arc::clone(&h.plan))
                .collect()
        };
        let looked = !homes.is_empty() || advertised;
        for plan in homes {
            self.look_over(bee, &plan).await;
        }
        looked
    }

    /// Forget what this Node learned about queries that are no longer advertised
    /// and were not planned here.
    fn forget_finished(&self) {
        let live: HashSet<String> = self.dances().into_iter().map(|(_, d)| d.query).collect();
        let mut st = self.lock();
        let homes: HashSet<String> = st.homes.keys().cloned().collect();
        let keep = |q: &String| live.contains(q) || homes.contains(q);
        st.plans.retain(|q, _| keep(q));
        st.complete.retain(|(q, _)| keep(q));
        st.done.retain(|(q, ..)| keep(q));
        st.cooldown.retain(|(q, ..), _| keep(q));
        st.dancing.retain(|(q, ..), _| keep(q));
    }

    async fn plan_query(
        self: &Arc<Self>,
        bee: u32,
        sub: Submitted,
        pool: Option<Arc<dyn MemoryPool>>,
    ) {
        match self.exec.plan(&sub.query, &sub.sql, pool.clone()).await {
            Ok(plan) => {
                self.counters.planned.fetch_add(1, Ordering::Relaxed);
                // A small query is finished before a dance could recruit anyone.
                if plan
                    .input_bytes()
                    .is_some_and(|bytes| bytes <= self.cfg.local_bytes)
                {
                    self.counters.whole.fetch_add(1, Ordering::Relaxed);
                    let answer = self
                        .exec
                        .run_whole(&plan, pool)
                        .await
                        .map(|batches| Answer {
                            batches,
                            schema: plan.schema(),
                        });
                    self.counters.answered.fetch_add(1, Ordering::Relaxed);
                    let _ = sub.reply.send(answer);
                    return;
                }
                if let Err(e) = self.exec.publish(&plan).await {
                    let _ = sub.reply.send(Err(e));
                    return;
                }
                {
                    let mut st = self.lock();
                    st.homes.insert(
                        sub.query.clone(),
                        Home {
                            plan: Arc::clone(&plan),
                            reply: Some(sub.reply),
                            checked: self.now(),
                            result_started: false,
                        },
                    );
                }
                self.advertise_ready(bee, &plan);
            }
            Err(e) => {
                let _ = sub.reply.send(Err(e));
            }
        }
    }

    /// Dance for each stage of `plan` that is ready, incomplete and not advertised.
    fn advertise_ready(&self, bee: u32, plan: &Arc<dyn QueryPlan>) {
        let mut st = self.lock();
        let query = plan.query();
        for info in plan.stages().iter().filter(|s| !s.result) {
            let complete = st.complete.contains(&(query.to_string(), info.id));
            if complete || !Self::ready(&st, query, info) {
                continue;
            }
            if self.has_live_dance(query, info.id) {
                continue;
            }
            if self.claimable(&mut st, query, info, &[]).is_empty() {
                continue;
            }
            self.post_dance(&mut st, bee, plan, info, self.cfg.flat_circuits, 0, vec![]);
        }
    }

    /// A Bee that touches a group keeps it advertised while Patches remain, as a
    /// forager dances after every return trip: work stays on the floor as long as
    /// there is a Bee about to run it, even if every Scout is busy.
    fn keep_advertised(&self, bee: u32, plan: &Arc<dyn QueryPlan>, stage: usize) {
        let Some(info) = plan.stage(stage).cloned() else {
            return;
        };
        if info.result || self.has_live_dance(plan.query(), stage) {
            return;
        }
        let mut st = self.lock();
        if !self.claimable(&mut st, plan.query(), &info, &[]).is_empty() {
            self.post_dance(&mut st, bee, plan, &info, self.cfg.flat_circuits, 0, vec![]);
        }
    }

    fn has_live_dance(&self, query: &str, stage: usize) -> bool {
        self.floor
            .entries_for(EntryKind::Dance, &format!("{query}/{stage}"))
            .iter()
            .any(|e| matches!(&e.body, EntryBody::Dance(d) if d.query == query && d.stage == stage))
    }

    #[allow(clippy::too_many_arguments)]
    fn post_dance(
        &self,
        _st: &mut State,
        bee: u32,
        plan: &Arc<dyn QueryPlan>,
        info: &StageInfo,
        circuits: f64,
        renewals: u32,
        redo: Vec<usize>,
    ) {
        let group = Group {
            query: plan.query().to_string(),
            stage: info.id,
            patches: info.patches,
            pinned: info.pinned.then_some(self.me),
        };
        self.dance_for(bee, &group, circuits, renewals, redo);
    }

    /// Post a dance for `group`, lasting as long as `circuits` take to run.
    fn dance_for(&self, bee: u32, group: &Group, circuits: f64, renewals: u32, redo: Vec<usize>) {
        let life = lifetime_of(circuits, &self.cfg);
        if life.is_zero() {
            return;
        }
        self.counters.dances.fetch_add(1, Ordering::Relaxed);
        self.floor.publish(
            EntryKey::dance(&group.query, group.stage, self.me, bee),
            EntryBody::Dance(Dance {
                query: group.query.clone(),
                stage: group.stage,
                patches: group.patches,
                circuits,
                renewals,
                pinned: group.pinned,
                redo,
            }),
            life,
        );
    }

    /// Check a query planned here against the store, advertise what is ready, and
    /// ask for stragglers to be run again.
    async fn look_over(self: &Arc<Self>, bee: u32, plan: &Arc<dyn QueryPlan>) {
        let query = plan.query().to_string();
        {
            let mut st = self.lock();
            if let Some(home) = st.homes.get_mut(&query) {
                home.checked = self.now();
            }
        }
        // Stages in flight: ready and not yet known complete.
        let in_flight: Vec<StageInfo> = {
            let st = self.lock();
            plan.stages()
                .iter()
                .filter(|s| !s.result)
                .filter(|s| {
                    !st.complete.contains(&(query.clone(), s.id)) && Self::ready(&st, &query, s)
                })
                .cloned()
                .collect()
        };
        for info in &in_flight {
            match self.exec.exchange().completed(&query, info.id).await {
                Ok(done) => {
                    let mut st = self.lock();
                    for p in &done {
                        st.done.insert((query.clone(), info.id, *p));
                    }
                    if done.len() >= info.patches {
                        st.complete.insert((query.clone(), info.id));
                    }
                }
                Err(e) => debug!(error = %e, "Could not list a stage's completions"),
            }
        }
        self.advertise_ready(bee, plan);
    }

    /// Groups with a Patch that one Bee has been running far longer than the
    /// median of the group, read from this Node's floor alone: the Patches such a
    /// Bee is slow at, and a second Bee could run again.
    fn stragglers(&self) -> Vec<(Group, Vec<usize>)> {
        // Every claim on the floor says which group it belongs to, so a Scout needs
        // neither the plan nor a dance to find a Bee that is taking too long.
        type Claims = HashMap<usize, Vec<(Entry, SoftClaim)>>;
        let mut groups: BTreeMap<(String, usize), (Group, Claims)> = BTreeMap::new();
        for entry in self.floor.entries(EntryKind::SoftClaim) {
            let EntryBody::SoftClaim(claim) = &entry.body else {
                continue;
            };
            let claim = claim.clone();
            let mut parts = entry.key.target.rsplitn(3, '/');
            let (Some(patch), Some(stage), Some(query)) = (
                parts.next().and_then(|p| p.parse::<usize>().ok()),
                parts.next().and_then(|p| p.parse::<usize>().ok()),
                parts.next(),
            ) else {
                continue;
            };
            let (_, claims) = groups.entry((query.to_string(), stage)).or_insert_with(|| {
                (
                    Group {
                        query: query.to_string(),
                        stage,
                        patches: claim.patches,
                        pinned: claim.pinned,
                    },
                    HashMap::new(),
                )
            });
            claims.entry(patch).or_default().push((entry, claim));
        }
        let live: Vec<Dance> = self.dances().into_iter().map(|(_, d)| d).collect();
        let mut out = Vec::new();
        for (group, claims) in groups.into_values() {
            let mut finished: Vec<u64> = claims
                .values()
                .flatten()
                .filter(|(_, c)| c.state == ClaimState::Done)
                .map(|(_, c)| c.elapsed_ms)
                .collect();
            if finished.len() < 3 {
                continue;
            }
            finished.sort_unstable();
            let limit = finished[finished.len() / 2] as f64 * self.cfg.straggler_factor;
            let mut slow: Vec<usize> = claims
                .iter()
                .filter(|(_, entries)| {
                    // One Bee on it, still running, and slow.
                    entries.len() == 1
                        && entries[0].1.state == ClaimState::Running
                        && entries[0].1.elapsed_ms as f64 > limit
                })
                .map(|(p, _)| *p)
                .collect();
            // Patches a live dance already asks to have run again need no more asking.
            let covered: HashSet<usize> = live
                .iter()
                .filter(|d| d.query == group.query && d.stage == group.stage)
                .flat_map(|d| d.redo.iter().copied())
                .collect();
            slow.retain(|p| !covered.contains(p));
            slow.sort_unstable();
            if !slow.is_empty() {
                out.push((group, slow));
            }
        }
        out
    }

    /// Advertise the stragglers any Scout can see for a second attempt. Returns
    /// whether there were any.
    fn advertise_stragglers(&self, bee: u32) -> bool {
        let slow = self.stragglers();
        for (group, patches) in &slow {
            self.dance_for(bee, group, self.cfg.flat_circuits, 0, patches.clone());
        }
        !slow.is_empty()
    }

    // -----------------------------------------------------------------------
    // Foraging and following
    // -----------------------------------------------------------------------

    /// Do a Forager's work: pull the next Patch of a query planned here and run it.
    /// Stages that read rows held only by this Node go first.
    pub async fn forage(self: &Arc<Self>, bee: u32, pool: Option<Arc<dyn MemoryPool>>) -> bool {
        let pick = {
            let mut st = self.lock();
            let homes: Vec<Arc<dyn QueryPlan>> =
                st.homes.values().map(|h| Arc::clone(&h.plan)).collect();
            let mut best: Option<(bool, Arc<dyn QueryPlan>, usize, usize)> = None;
            for plan in homes {
                for info in plan.stages().iter().filter(|s| !s.result) {
                    if !Self::ready(&st, plan.query(), info) {
                        continue;
                    }
                    let open = self.claimable(&mut st, plan.query(), info, &[]);
                    if open.is_empty() {
                        continue;
                    }
                    let patch = self.take_from(plan.query(), info.id, info.patches, &open);
                    // Pinned stages first, then the oldest query, then plan order.
                    if best
                        .as_ref()
                        .is_none_or(|(pinned, ..)| info.pinned && !pinned)
                    {
                        best = Some((info.pinned, Arc::clone(&plan), info.id, patch));
                    }
                }
            }
            if let Some((_, plan, stage, patch)) = &best {
                st.running
                    .insert((plan.query().to_string(), *stage, *patch));
            }
            best
        };
        match pick {
            Some((_, plan, stage, patch)) => {
                self.run_one(bee, plan, stage, patch, pool, false).await
            }
            None => false,
        }
    }

    /// Do a Follower's work: follow a dance, claim a Patch of its group and run it.
    pub async fn follow(self: &Arc<Self>, bee: u32, pool: Option<Arc<dyn MemoryPool>>) -> bool {
        let mut rng = self.rng(bee);
        let chosen = {
            let mut st = self.lock();
            let mut candidates = self.candidates(&mut st);
            if candidates.is_empty() {
                return false;
            }
            let now = self.now();
            let entry_index = match self.cfg.mode {
                Mode::Pull => rng.next_below(candidates.len() as u64) as usize,
                Mode::Dances => {
                    // Follow one dance, chosen at random in proportion to its
                    // strength. Nothing is sorted or compared: each dance gets its
                    // share of the draw. On a very large floor a uniform sample of
                    // `sample_k` dances stands in for the whole.
                    let mut sample: Vec<usize> = (0..candidates.len()).collect();
                    while sample.len() > self.cfg.sample_k {
                        let i = rng.next_below(sample.len() as u64) as usize;
                        sample.swap_remove(i);
                    }
                    let weights: Vec<f64> = sample
                        .iter()
                        .map(|i| circuits_left(candidates[*i].0.remaining(now), &self.cfg))
                        .collect();
                    let total: f64 = weights.iter().sum();
                    if total <= 0.0 {
                        sample[0]
                    } else {
                        let mut draw = rng.next_f64() * total;
                        let mut pick = sample[sample.len() - 1];
                        for (i, w) in sample.iter().zip(&weights) {
                            if draw < *w {
                                pick = *i;
                                break;
                            }
                            draw -= w;
                        }
                        pick
                    }
                }
            };
            let (_, dance, open) = candidates.swap_remove(entry_index);
            let patch = self.take_from(&dance.query, dance.stage, dance.patches, &open);
            let redo = dance.redo.contains(&patch);
            st.running.insert((dance.query.clone(), dance.stage, patch));
            (dance, patch, redo)
        };
        let (dance, patch, redo) = chosen;
        let plan = match self.plan_for(&dance.query).await {
            Ok(plan) => plan,
            Err(e) => {
                debug!(error = %e, query = %dance.query, "Could not read the plan of a dance");
                self.lock()
                    .running
                    .remove(&(dance.query.clone(), dance.stage, patch));
                return false;
            }
        };
        self.run_one(bee, plan, dance.stage, patch, pool, redo)
            .await
    }

    async fn plan_for(&self, query: &str) -> Result<Arc<dyn QueryPlan>> {
        {
            let st = self.lock();
            if let Some(home) = st.homes.get(query) {
                return Ok(Arc::clone(&home.plan));
            }
            if let Some(plan) = st.plans.get(query) {
                return Ok(Arc::clone(plan));
            }
        }
        let plan = self.exec.load(query).await?;
        self.lock()
            .plans
            .insert(query.to_string(), Arc::clone(&plan));
        Ok(plan)
    }

    fn post_claim(
        &self,
        group: &Group,
        patch: usize,
        bee: u32,
        state: ClaimState,
        elapsed_ms: u64,
    ) {
        let ttl = match state {
            ClaimState::Running => self.cfg.claim_ttl,
            ClaimState::Done => self.cfg.claim_ttl * 4,
        };
        self.floor.publish(
            EntryKey::claim(&group.query, group.stage, patch, self.me, bee),
            EntryBody::SoftClaim(SoftClaim {
                state,
                elapsed_ms,
                patches: group.patches,
                pinned: group.pinned,
            }),
            ttl,
        );
    }

    /// Claim and run one Patch, which is already marked as running here.
    async fn run_one(
        self: &Arc<Self>,
        bee: u32,
        plan: Arc<dyn QueryPlan>,
        stage: usize,
        patch: usize,
        pool: Option<Arc<dyn MemoryPool>>,
        redo: bool,
    ) -> bool {
        let query = plan.query().to_string();
        let id: PatchId = (query.clone(), stage, patch);
        let started = self.clock.monotonic();
        let group = Arc::new(Group {
            query: query.clone(),
            stage,
            patches: plan.stage(stage).map_or(0, |s| s.patches),
            pinned: plan
                .stage(stage)
                .is_some_and(|s| s.pinned)
                .then_some(self.me),
        });
        self.post_claim(&group, patch, bee, ClaimState::Running, 0);
        self.keep_advertised(bee, &plan, stage);

        // The comb store has the final word on whether the Patch is done. A Done
        // marker on the floor fades, and a Node forgets what it learned of a query,
        // so a Patch won long ago can look open again; the store never forgets.
        let already = self
            .exec
            .exchange()
            .completion(&PatchKey::new(&query, stage, patch))
            .await;
        if matches!(already, Ok(Some(_))) {
            self.floor
                .withdraw(&EntryKey::claim(&query, stage, patch, self.me, bee));
            let mut st = self.lock();
            st.running.remove(&id);
            st.done.insert(id);
            return false;
        }

        // A Node that has been wasting work waits a gossip round after claiming and
        // yields to a lower claimant, so duplicates thin out.
        if self.duplicate_rate() > self.cfg.yield_above {
            self.clock.sleep(self.cfg.gossip_round).await;
            if self.rival_claims(&query, stage, patch, bee) {
                self.floor
                    .withdraw(&EntryKey::claim(&query, stage, patch, self.me, bee));
                self.lock().running.remove(&id);
                self.counters.yields.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }

        // Keep the claim alive while the Patch runs.
        let refresher = {
            let forage = Arc::clone(self);
            let (group, clock) = (Arc::clone(&group), Arc::clone(&self.clock));
            tokio::spawn(async move {
                loop {
                    clock.sleep(forage.cfg.claim_refresh).await;
                    let elapsed = (clock.monotonic() - started).as_millis() as u64;
                    forage.post_claim(&group, patch, bee, ClaimState::Running, elapsed);
                }
            })
        };

        self.counters.attempts.fetch_add(1, Ordering::Relaxed);
        if redo {
            self.counters.redone.fetch_add(1, Ordering::Relaxed);
        }
        let attempt = {
            let mut st = self.lock();
            st.next_attempt += 1;
            format!("{}-{bee}-{}", self.label, st.next_attempt)
        };
        let outcome = self
            .exec
            .run_patch(&plan, stage, patch, &attempt, pool)
            .await;
        refresher.abort();
        let elapsed = self.clock.monotonic() - started;
        let elapsed_ms = elapsed.as_millis() as u64;

        match outcome {
            Ok(run) => {
                let won = matches!(run.completed, Completed::Won);
                let moot = matches!(run.completed, Completed::Moot);
                {
                    let mut st = self.lock();
                    st.running.remove(&id);
                    st.done.insert(id.clone());
                    st.duplicates = 0.9 * st.duplicates + if won { 0.0 } else { 0.1 };
                }
                if moot {
                    // The query was answered while this ran: nothing to record.
                    self.counters.lost.fetch_add(1, Ordering::Relaxed);
                    self.floor
                        .withdraw(&EntryKey::claim(&query, stage, patch, self.me, bee));
                } else if won {
                    self.counters.won.fetch_add(1, Ordering::Relaxed);
                    self.counters.rows.fetch_add(run.rows, Ordering::Relaxed);
                    self.post_claim(&group, patch, bee, ClaimState::Done, elapsed_ms);
                    self.after_win(bee, &plan, stage, run.rows, elapsed).await;
                } else {
                    // The work is surplus; the Patch is done all the same.
                    self.counters.lost.fetch_add(1, Ordering::Relaxed);
                    self.post_claim(&group, patch, bee, ClaimState::Done, elapsed_ms);
                }
            }
            Err(e) => {
                warn!(error = %e, query = %query, stage, patch, "A Patch failed");
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                self.floor
                    .withdraw(&EntryKey::claim(&query, stage, patch, self.me, bee));
                let mut st = self.lock();
                st.running.remove(&id);
                st.cooldown
                    .insert(id, self.now() + self.cfg.claim_ttl.as_millis() as u64);
            }
        }
        true
    }

    /// Whether another Bee with a lower (Node, Bee) id has a live claim on the Patch.
    fn rival_claims(&self, query: &str, stage: usize, patch: usize, bee: u32) -> bool {
        let mine = (self.me, bee);
        self.claims(query, stage)
            .remove(&patch)
            .unwrap_or_default()
            .iter()
            .any(|(entry, claim)| {
                claim.state == ClaimState::Running && (entry.key.node, entry.key.bee) < mine
            })
    }

    /// What a Bee does after her Patch completed: measure how good it was, dance
    /// for the group if that is worth it, and move the query on if its stage is done.
    async fn after_win(
        self: &Arc<Self>,
        bee: u32,
        plan: &Arc<dyn QueryPlan>,
        stage: usize,
        rows: u64,
        elapsed: Duration,
    ) {
        let Some(info) = plan.stage(stage).cloned() else {
            return;
        };
        {
            let mut st = self.lock();
            let normalised = st.profit.record(rows, elapsed);
            let dance = self.cfg.mode == Mode::Dances && st.profit.worth_dancing(&self.cfg);
            if dance {
                let key = (plan.query().to_string(), stage, bee);
                let state = st.dancing.entry(key).or_insert_with(|| Dancing {
                    c0: initial_circuits(normalised, &self.cfg),
                    renewals: 0,
                });
                let (c0, renewals) = (state.c0, state.renewals);
                state.renewals += 1;
                let circuits = circuits_after(c0, renewals, &self.cfg);
                self.post_dance(&mut st, bee, plan, &info, circuits, renewals, vec![]);
            }
        }
        // If the floor says every Patch of the stage is done, confirm against the
        // store and move the query on; otherwise the home Node's Scout will.
        let known_done = {
            let st = self.lock();
            (0..info.patches)
                .filter(|p| st.done.contains(&(plan.query().to_string(), stage, *p)))
                .count()
        };
        if known_done < info.patches {
            return;
        }
        let complete = matches!(
            self.exec.exchange().completed(plan.query(), stage).await,
            Ok(done) if done.len() >= info.patches
        );
        if complete {
            self.lock()
                .complete
                .insert((plan.query().to_string(), stage));
            self.advertise_ready(bee, plan);
        }
    }

    // -----------------------------------------------------------------------
    // Receiving
    // -----------------------------------------------------------------------

    /// Do a Receiver's work: run the result stage of a query planned here, answer
    /// the caller and tidy up.
    pub async fn receive(self: &Arc<Self>, _bee: u32, pool: Option<Arc<dyn MemoryPool>>) -> bool {
        let next = {
            let mut st = self.lock();
            let ready: Vec<String> = st
                .homes
                .iter()
                .filter(|(_, h)| {
                    !h.result_started && self.inputs_look_done(&st, &h.plan, h.plan.result_stage())
                })
                .map(|(q, _)| q.clone())
                .collect();
            let query = ready.into_iter().next();
            query.and_then(|q| {
                let home = st.homes.get_mut(&q)?;
                home.result_started = true;
                Some((q, Arc::clone(&home.plan)))
            })
        };
        let Some((query, plan)) = next else {
            return false;
        };
        // The floor says the inputs are done; the store says whether they are.
        for input in &plan.result_stage().inputs {
            let known = self.lock().complete.contains(&(query.clone(), *input));
            if known {
                continue;
            }
            let patches = plan.stage(*input).map_or(0, |s| s.patches);
            let done = match self.exec.exchange().completed(&query, *input).await {
                Ok(done) => done.len() >= patches,
                Err(_) => false,
            };
            if done {
                self.lock().complete.insert((query.clone(), *input));
            } else {
                if let Some(home) = self.lock().homes.get_mut(&query) {
                    home.result_started = false;
                }
                return false;
            }
        }
        let answer = self
            .exec
            .run_result(&plan, pool)
            .await
            .map(|batches| Answer {
                batches,
                schema: plan.schema(),
            });
        let reply = self.lock().homes.remove(&query).and_then(|h| h.reply);
        if let Err(e) = self.exec.finish(&query).await {
            debug!(error = %e, "Could not tidy up after a query");
        }
        self.counters.answered.fetch_add(1, Ordering::Relaxed);
        if let Some(reply) = reply {
            let _ = reply.send(answer);
        }
        true
    }
}
