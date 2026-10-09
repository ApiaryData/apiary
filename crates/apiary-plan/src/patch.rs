//! Running one Patch.
//!
//! A Patch is one partition of one stage. Running it means executing the stage's
//! plan for that partition, splitting the rows as the stage's boundary says, and
//! handing the pieces to the [`Exchange`]. Each run is an *attempt*; the attempt
//! that completes the Patch first wins, and the rest are discarded.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::repartition::{BatchPartitioner, RepartitionExec};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::StreamExt;

use apiary_core::{ApiaryError, Result};

use crate::exchange::{Completed, Completion, Exchange, PatchKey, encode_batches};
use crate::scan::ScanSource;
use crate::stages::{Handoff, Stage};

/// What a Patch needs from the Node running it, carried in the task context.
pub struct PatchEnv {
    query: String,
    node: String,
    exchange: Arc<dyn Exchange>,
    scans: ScanSource,
    held: HashMap<String, Arc<dyn ExecutionPlan>>,
}

impl PatchEnv {
    /// The environment for running Patches of `query` on `node`.
    pub fn new(
        query: impl Into<String>,
        node: impl Into<String>,
        exchange: Arc<dyn Exchange>,
        scans: ScanSource,
        held: HashMap<String, Arc<dyn ExecutionPlan>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            query: query.into(),
            node: node.into(),
            exchange,
            scans,
            held,
        })
    }

    /// The query's id.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// The Node running the Patches.
    pub fn node(&self) -> &str {
        &self.node
    }

    /// Where stages hand batches to each other.
    pub fn exchange(&self) -> &Arc<dyn Exchange> {
        &self.exchange
    }

    /// How scans are rebuilt from their specs.
    pub fn scans(&self) -> &ScanSource {
        &self.scans
    }

    /// A leaf held in this Node's memory for the query, if this Node holds it.
    pub fn held(&self, key: &str) -> Option<Arc<dyn ExecutionPlan>> {
        self.held.get(key).cloned()
    }

    /// A task context for running a Patch, with this environment attached. With
    /// `pool` the Patch runs under that share of the Node's memory, keeping the
    /// Node's spill directory, caches and object stores.
    pub fn task_context(
        self: &Arc<Self>,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> DfResult<Arc<TaskContext>> {
        let mut state: SessionState = self.scans.state().clone();
        if let Some(pool) = pool {
            let runtime = RuntimeEnvBuilder::from_runtime_env(state.runtime_env())
                .with_memory_pool(pool)
                .build_arc()?;
            state = SessionStateBuilder::new_from_existing(state)
                .with_runtime_env(runtime)
                .build();
        }
        let config = state.config().clone().with_extension(Arc::clone(self));
        Ok(Arc::new(
            TaskContext::from(&state).with_session_config(config),
        ))
    }
}

/// What running a Patch came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchOutcome {
    /// Whether this attempt's output became the Patch's.
    pub completed: Completed,
    /// Rows this attempt produced.
    pub rows: u64,
}

fn to_apiary(e: DataFusionError) -> ApiaryError {
    ApiaryError::Internal {
        message: format!("Patch failed: {e}"),
    }
}

/// Run Patch `index` of `stage` as attempt `attempt`: execute it, split its rows
/// as the stage's boundary says, store the pieces, and try to complete the Patch.
pub async fn run_patch(
    stage: &Stage,
    index: usize,
    env: &Arc<PatchEnv>,
    ctx: Arc<TaskContext>,
    attempt: &str,
) -> Result<PatchOutcome> {
    let key = PatchKey::new(env.query(), stage.id, index);
    let body = stage.body();
    let schema = body.schema();
    let outputs = match stage.handoff {
        Handoff::Split(n) => n,
        Handoff::Whole => 1,
        Handoff::Result => {
            return Err(ApiaryError::Internal {
                message: "the result stage hands rows to the caller, not the exchange".into(),
            });
        }
    };

    let mut partitioner = match stage.handoff {
        Handoff::Split(_) => {
            let repartition = stage
                .boundary
                .downcast_ref::<RepartitionExec>()
                .ok_or_else(|| ApiaryError::Internal {
                    message: "a split stage must end in a repartition".into(),
                })?;
            let timer = MetricBuilder::new(&ExecutionPlanMetricsSet::new())
                .subset_time("repartition_time", index);
            Some(match repartition.partitioning() {
                Partitioning::Hash(exprs, n) => {
                    BatchPartitioner::new_hash_partitioner(exprs.clone(), *n, timer)
                        .map_err(to_apiary)?
                }
                Partitioning::RoundRobinBatch(n) => {
                    // Start each Patch at a different output, so small outputs spread.
                    BatchPartitioner::new_round_robin_partitioner(*n, timer, index, stage.patches())
                }
                other => {
                    return Err(ApiaryError::Internal {
                        message: format!("cannot split by {other}"),
                    });
                }
            })
        }
        _ => None,
    };

    let reservation =
        MemoryConsumer::new(format!("Patch[{}/{}]", stage.id, index)).register(ctx.memory_pool());
    let mut parts: Vec<Vec<RecordBatch>> = vec![Vec::new(); outputs];
    let mut stream = stage
        .fresh_body()
        .and_then(|plan| plan.execute(index, Arc::clone(&ctx)))
        .map_err(to_apiary)?;
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(to_apiary)?;
        rows += batch.num_rows() as u64;
        match partitioner.as_mut() {
            Some(partitioner) => {
                let mut grow = 0;
                partitioner
                    .partition(batch, |part, piece| {
                        grow += piece.get_array_memory_size();
                        parts[part].push(piece);
                        Ok(())
                    })
                    .map_err(to_apiary)?;
                reservation.try_grow(grow).map_err(to_apiary)?;
            }
            None => {
                reservation
                    .try_grow(batch.get_array_memory_size())
                    .map_err(to_apiary)?;
                parts[0].push(batch);
            }
        }
    }

    let exchange = env.exchange();
    let mut record = Completion {
        attempt: attempt.to_string(),
        node: env.node().to_string(),
        rows: Vec::with_capacity(outputs),
        bytes: Vec::with_capacity(outputs),
    };
    for (output, batches) in parts.iter().enumerate() {
        let data = encode_batches(&schema, batches).map_err(to_apiary)?;
        record
            .rows
            .push(batches.iter().map(|b| b.num_rows() as u64).sum());
        record.bytes.push(data.len() as u64);
        exchange.put(&key, attempt, output, data).await?;
    }
    drop(parts);
    drop(reservation);
    let completed = exchange.complete(&key, &record).await?;
    Ok(PatchOutcome { completed, rows })
}

/// Run the result stage: its one Patch's rows are the query's answer.
pub async fn run_result(stage: &Stage, ctx: Arc<TaskContext>) -> Result<Vec<RecordBatch>> {
    let stream = stage
        .fresh_body()
        .and_then(|plan| plan.execute(0, ctx))
        .map_err(to_apiary)?;
    datafusion::physical_plan::common::collect(stream)
        .await
        .map_err(to_apiary)
}

/// Run every Patch of `stage` on this Node, a few at a time.
pub async fn run_stage(
    stage: &Stage,
    env: &Arc<PatchEnv>,
    ctx: &Arc<TaskContext>,
    attempt_prefix: &str,
    concurrency: usize,
) -> Result<Vec<PatchOutcome>> {
    use futures::TryStreamExt;
    futures::stream::iter(0..stage.patches())
        .map(|index| {
            let (env, ctx) = (Arc::clone(env), Arc::clone(ctx));
            let attempt = format!("{attempt_prefix}-{index}");
            async move { run_patch(stage, index, &env, ctx, &attempt).await }
        })
        .buffered(concurrency.max(1))
        .try_collect()
        .await
}

/// A mistake the caller can recognise: a Patch could not run yet.
pub fn is_input_not_ready(e: &ApiaryError) -> bool {
    e.to_string().contains("is not complete yet")
}
