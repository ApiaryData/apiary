//! A colony foraging on simulated time and a simulated network.
//!
//! Everything about who does what is the production code: each Node has real
//! Bees choosing roles by response thresholds, a real gossiped dance floor over
//! the simulated network, real Scouts, Foragers, Followers and Receivers, and the
//! real exchange and completion records in a simulated comb store. Only the cost
//! of a Patch is made up: a Patch takes virtual time (its cost over the Node's
//! speed) instead of CPU, which is what lets dances, claims and duplicate work be
//! measured at all, since virtual time is the only time the simulation has.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apiary_colony::{BeeContext, Colony as Bees, ColonyConfig, Duties, Role, Stimuli};
use apiary_comb::custom_store::ObjectStoreBackend;
use apiary_core::rng::hash_str;
use apiary_core::{ApiaryError, Clock, Result, StorageBackend};
use apiary_floor::{Floor, GossipConfig, GossipFloor};
use apiary_forage::{Executor, Forage, ForageConfig, PatchRun, QueryPlan, StageInfo};
use apiary_plan::exchange::{Completion, Exchange, PatchKey, StoreExchange};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::colony::Colony as MeshColony;
use crate::net::{Placement, SimNetwork};
use crate::sim::Sim;
use crate::store::SimStore;

/// The work of one stage, as the simulation models it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Work {
    /// Patches in the stage.
    pub patches: usize,
    /// Stages feeding it.
    pub inputs: Vec<usize>,
    /// Reads rows only the planning Node holds.
    pub pinned: bool,
    /// How long a Patch takes on a Node of speed 1, in milliseconds.
    pub patch_ms: u64,
    /// Rows a Patch produces.
    pub rows: u64,
    /// Patches vary by up to this fraction either way.
    pub jitter: f64,
}

/// A query's shape: stages in order, the last the result stage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shape {
    /// The stages.
    pub stages: Vec<Work>,
}

impl Shape {
    /// A scan of `patches` Patches, then the result.
    pub fn scan(patches: usize, patch_ms: u64, rows: u64) -> Self {
        Self {
            stages: vec![
                Work {
                    patches,
                    inputs: vec![],
                    pinned: false,
                    patch_ms,
                    rows,
                    jitter: 0.2,
                },
                Work {
                    patches: 1,
                    inputs: vec![0],
                    pinned: false,
                    patch_ms: 5,
                    rows: 1,
                    jitter: 0.0,
                },
            ],
        }
    }

    /// Scan, aggregate across a hash split, then the result: three stages in a chain.
    pub fn rollup(scan: usize, scan_ms: u64, merge: usize, merge_ms: u64, rows: u64) -> Self {
        Self {
            stages: vec![
                Work {
                    patches: scan,
                    inputs: vec![],
                    pinned: false,
                    patch_ms: scan_ms,
                    rows,
                    jitter: 0.2,
                },
                Work {
                    patches: merge,
                    inputs: vec![0],
                    pinned: false,
                    patch_ms: merge_ms,
                    rows: rows / 4,
                    jitter: 0.1,
                },
                Work {
                    patches: 1,
                    inputs: vec![1],
                    pinned: false,
                    patch_ms: 5,
                    rows: 1,
                    jitter: 0.0,
                },
            ],
        }
    }

    /// The same query with its first stage pinned to the Node that plans it.
    pub fn pinned(mut self) -> Self {
        self.stages[0].pinned = true;
        self
    }
}

/// The shapes queries have, by their text: what the simulated planner "plans".
#[derive(Clone, Default)]
pub struct Shapes(Arc<Mutex<HashMap<String, Shape>>>);

impl Shapes {
    /// Say what `sql` plans to.
    pub fn insert(&self, sql: &str, shape: Shape) {
        self.0
            .lock()
            .expect("shapes")
            .insert(sql.to_string(), shape);
    }

    fn get(&self, sql: &str) -> Option<Shape> {
        self.0.lock().expect("shapes").get(sql).cloned()
    }
}

struct SimPlan {
    query: String,
    info: Vec<StageInfo>,
    shape: Shape,
}

impl SimPlan {
    fn new(query: &str, shape: Shape) -> Self {
        let last = shape.stages.len() - 1;
        let info = shape
            .stages
            .iter()
            .enumerate()
            .map(|(id, w)| StageInfo {
                id,
                patches: w.patches,
                inputs: w.inputs.clone(),
                pinned: w.pinned,
                result: id == last,
            })
            .collect();
        Self {
            query: query.to_string(),
            info,
            shape,
        }
    }
}

impl QueryPlan for SimPlan {
    fn query(&self) -> &str {
        &self.query
    }

    fn stages(&self) -> &[StageInfo] {
        &self.info
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// An executor whose Patches take virtual time.
pub struct SimExecutor {
    node: String,
    speed: f64,
    clock: Arc<dyn Clock>,
    exchange: Arc<dyn Exchange>,
    store: Arc<dyn StorageBackend>,
    shapes: Shapes,
    seed: u64,
    tally: Tally,
}

/// Patches the colony committed, by the rows each produced (which tells the
/// kinds of work apart in a mixed workload).
pub type Tally = Arc<Mutex<std::collections::BTreeMap<u64, u64>>>;

fn internal(message: impl Into<String>) -> ApiaryError {
    ApiaryError::Internal {
        message: message.into(),
    }
}

impl SimExecutor {
    fn plan_of(plan: &Arc<dyn QueryPlan>) -> Result<&SimPlan> {
        plan.as_any()
            .downcast_ref::<SimPlan>()
            .ok_or_else(|| internal("a plan from another executor"))
    }

    fn cost(&self, work: &Work, query: &str, stage: usize, patch: usize) -> Duration {
        let u = (hash_str(&format!("{}:{query}:{stage}:{patch}", self.seed)) % 10_000) as f64
            / 10_000.0;
        let factor = 1.0 + work.jitter * (2.0 * u - 1.0);
        Duration::from_secs_f64(work.patch_ms as f64 / 1000.0 * factor / self.speed)
    }

    /// Reading a Patch's inputs: every stage that feeds it must be complete.
    async fn check_inputs(&self, plan: &SimPlan, stage: usize) -> Result<()> {
        for input in &plan.info[stage].inputs {
            let done = self.exchange.completed(&plan.query, *input).await?;
            if done.len() < plan.info[*input].patches {
                return Err(internal(format!(
                    "stage {input} of {} is not complete yet",
                    plan.query
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Executor for SimExecutor {
    async fn plan(&self, query: &str, sql: &str) -> Result<Arc<dyn QueryPlan>> {
        let shape = self
            .shapes
            .get(sql)
            .ok_or_else(|| internal(format!("the simulation does not know {sql:?}")))?;
        self.store
            .put(
                &format!("plans/{query}"),
                Bytes::from(serde_json::to_vec(&shape).expect("a shape serialises")),
            )
            .await?;
        Ok(Arc::new(SimPlan::new(query, shape)))
    }

    async fn load(&self, query: &str) -> Result<Arc<dyn QueryPlan>> {
        let bytes = self.store.get(&format!("plans/{query}")).await?;
        let shape: Shape =
            serde_json::from_slice(&bytes).map_err(|e| internal(format!("plan record: {e}")))?;
        Ok(Arc::new(SimPlan::new(query, shape)))
    }

    async fn run_patch(
        &self,
        plan: &Arc<dyn QueryPlan>,
        stage: usize,
        patch: usize,
        attempt: &str,
        _pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<PatchRun> {
        let plan = Self::plan_of(plan)?;
        self.check_inputs(plan, stage).await?;
        let work = &plan.shape.stages[stage];
        self.clock
            .sleep(self.cost(work, &plan.query, stage, patch))
            .await;
        let key = PatchKey::new(&plan.query, stage, patch);
        self.exchange
            .put(&key, attempt, 0, Bytes::from_static(b"patch output"))
            .await?;
        let record = Completion {
            attempt: attempt.to_string(),
            node: self.node.clone(),
            rows: vec![work.rows],
            bytes: vec![12],
        };
        let completed = self.exchange.complete(&key, &record).await?;
        if matches!(completed, apiary_plan::exchange::Completed::Won) {
            *self
                .tally
                .lock()
                .expect("tally")
                .entry(work.rows)
                .or_default() += 1;
        }
        Ok(PatchRun {
            completed,
            rows: work.rows,
        })
    }

    async fn run_result(
        &self,
        plan: &Arc<dyn QueryPlan>,
        _pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<Vec<RecordBatch>> {
        let plan = Self::plan_of(plan)?;
        let stage = plan.info.len() - 1;
        self.check_inputs(plan, stage).await?;
        let work = &plan.shape.stages[stage];
        self.clock
            .sleep(self.cost(work, &plan.query, stage, 0))
            .await;
        Ok(Vec::new())
    }

    fn exchange(&self) -> &Arc<dyn Exchange> {
        &self.exchange
    }

    async fn finish(&self, query: &str) -> Result<()> {
        self.exchange.clear(query).await?;
        self.store.delete(&format!("plans/{query}")).await
    }
}

/// What a Node's Bees do when a role calls, in terms of its [`Forage`].
struct ForageDuties {
    forage: Arc<Forage>,
}

#[async_trait]
impl Duties for ForageDuties {
    fn stimuli(&self) -> Stimuli {
        Stimuli::none()
            .with(Role::Scout, self.forage.scout_stimulus())
            .with(Role::Forager, self.forage.forager_stimulus())
            .with(Role::Follower, self.forage.follower_stimulus())
            .with(Role::Receiver, self.forage.receiver_stimulus())
    }

    fn claimed_waiting(&self) -> usize {
        0
    }

    async fn perform(&self, role: Role, bee: &BeeContext) -> bool {
        let index = bee.index() as u32;
        match role {
            Role::Scout => self.forage.scout(index).await,
            Role::Forager => self.forage.forage(index, Some(bee.pool())).await,
            Role::Follower => self.forage.follow(index, Some(bee.pool())).await,
            Role::Receiver => self.forage.receive(index, Some(bee.pool())).await,
            _ => false,
        }
    }
}

/// How one simulated Node is set up.
#[derive(Clone)]
pub struct NodeSpec {
    /// Its name in the scenario.
    pub name: &'static str,
    /// Where it sits on the network.
    pub placement: Placement,
    /// How many Bees (cores) it has.
    pub bees: usize,
    /// How fast it is, against a reference Node of 1.
    pub speed: f64,
}

impl NodeSpec {
    /// A Node.
    pub fn new(name: &'static str, placement: Placement, bees: usize, speed: f64) -> Self {
        Self {
            name,
            placement,
            bees,
            speed,
        }
    }
}

/// One Node of a [`ForageColony`].
pub struct ForageNode {
    /// Its name.
    pub name: &'static str,
    /// Its foraging.
    pub forage: Arc<Forage>,
    /// Its copy of the floor, gossiped to the rest of the colony.
    pub floor: Arc<GossipFloor>,
    /// Its Bees.
    pub bees: Bees,
}

/// A colony of Nodes foraging together.
pub struct ForageColony {
    /// The Nodes.
    pub nodes: Vec<ForageNode>,
    /// The membership layer under them.
    pub mesh: MeshColony,
    shapes: Shapes,
    tally: Tally,
}

/// How a colony is configured.
#[derive(Clone)]
pub struct ColonySpec {
    /// How foraging behaves.
    pub forage: ForageConfig,
    /// Latency of each request to the comb store.
    pub store_latency: Duration,
    /// How the Bees behave.
    pub dwell: Duration,
    /// Time between gossip rounds.
    pub gossip: Duration,
}

impl Default for ColonySpec {
    fn default() -> Self {
        Self {
            forage: ForageConfig::default(),
            store_latency: Duration::from_millis(15),
            dwell: Duration::from_millis(500),
            gossip: Duration::from_millis(250),
        }
    }
}

impl ForageColony {
    /// Start a colony of `specs` on `net`, every Node joined to every other, all
    /// sharing one comb store.
    pub async fn start(sim: &Sim, net: &SimNetwork, specs: &[NodeSpec], cfg: ColonySpec) -> Self {
        let placements: Vec<(&'static str, Placement)> = specs
            .iter()
            .map(|s| (s.name, s.placement.clone()))
            .collect();
        let mesh = MeshColony::new(sim, net, &placements);
        for a in mesh.members() {
            for b in mesh.members() {
                if a.id() < b.id() {
                    a.connect(b).await.expect("the colony's Nodes are admitted");
                }
            }
        }
        let bucket = sim.store("bucket");
        let shapes = Shapes::default();
        let tally = Tally::default();
        let mut nodes = Vec::new();
        for spec in specs {
            let member = mesh.get(spec.name);
            let view: SimStore = sim.store_view(&bucket, &format!("link-{}", spec.name));
            let latency = cfg.store_latency;
            view.set_faults(|f| f.latency = latency);
            let store: Arc<dyn StorageBackend> = Arc::new(ObjectStoreBackend::new(Arc::new(view)));
            let exchange: Arc<dyn Exchange> = Arc::new(StoreExchange::new(Arc::clone(&store)));
            let exec = Arc::new(SimExecutor {
                node: spec.name.to_string(),
                speed: spec.speed,
                clock: sim.clock(),
                exchange,
                store,
                shapes: shapes.clone(),
                seed: sim.seed(),
                tally: Arc::clone(&tally),
            });
            let mut gossip = GossipConfig::new("plant");
            gossip.seed = sim.seed();
            gossip.interval = cfg.gossip;
            let floor = Arc::new(GossipFloor::start(member.mesh.clone(), sim.clock(), gossip));
            let mut forage_cfg = cfg.forage.clone();
            forage_cfg.seed = sim.seed();
            forage_cfg.gossip_round = cfg.gossip;
            let forage = Forage::new(
                member.id(),
                spec.name,
                Arc::clone(&floor) as Arc<dyn Floor>,
                exec,
                sim.clock(),
                forage_cfg,
            );
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
            let mut colony = ColonyConfig::new(spec.name, spec.bees, 64 << 20, pool);
            colony.params.dwell = cfg.dwell;
            colony.engaged_poll = Duration::from_millis(5);
            colony.idle_poll = Duration::from_millis(40);
            let bees = Bees::start(
                colony,
                &sim.env(),
                Arc::new(ForageDuties {
                    forage: Arc::clone(&forage),
                }),
                None,
            );
            nodes.push(ForageNode {
                name: spec.name,
                forage,
                floor,
                bees,
            });
        }
        Self {
            nodes,
            mesh,
            shapes,
            tally,
        }
    }

    /// The Node called `name`.
    pub fn node(&self, name: &str) -> &ForageNode {
        self.nodes
            .iter()
            .find(|n| n.name == name)
            .unwrap_or_else(|| panic!("no Node called {name}"))
    }

    /// Patches committed so far, by the rows each produced.
    pub fn tally(&self) -> std::collections::BTreeMap<u64, u64> {
        self.tally.lock().expect("tally").clone()
    }

    /// Say what `sql` plans to.
    pub fn teach(&self, sql: &str, shape: Shape) {
        self.shapes.insert(sql, shape);
    }

    /// Submit `sql` at the Node called `at`.
    pub fn submit(&self, at: &str, sql: &str) -> oneshot::Receiver<Result<Vec<RecordBatch>>> {
        let node = self.node(at);
        let answer = node.forage.submit(sql);
        node.bees.wake();
        answer
    }

    /// Stop every Node's Bees and gossip.
    pub async fn shutdown(&self) {
        for node in &self.nodes {
            node.bees.shutdown().await;
            node.floor.stop();
        }
    }
}
