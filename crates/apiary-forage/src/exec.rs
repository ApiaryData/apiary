//! Planning and running, behind a trait.
//!
//! The colony decides who runs which Patch; an [`Executor`] runs it. In a Node the
//! executor is [`PlanExecutor`](crate::PlanExecutor), which plans with DataFusion
//! and runs Patches under a Bee's share of memory. The simulator's executor takes
//! virtual time instead of CPU, which is how dances, claims and duplicate work can
//! be measured on a virtual clock.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::memory_pool::MemoryPool;

use apiary_core::Result;
use apiary_plan::exchange::{Completed, Exchange};

use crate::plan::QueryPlan;

/// What running a Patch came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchRun {
    /// Whether this attempt's output became the Patch's.
    pub completed: Completed,
    /// Rows the attempt produced.
    pub rows: u64,
}

/// Plans queries and runs their Patches.
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    /// Plan `sql` as query `query`, writing the plan where other Nodes can read it.
    async fn plan(&self, query: &str, sql: &str) -> Result<Arc<dyn QueryPlan>>;

    /// Read the plan another Node wrote for `query`.
    async fn load(&self, query: &str) -> Result<Arc<dyn QueryPlan>>;

    /// Run Patch `patch` of `stage` as `attempt`, under `pool` if given.
    async fn run_patch(
        &self,
        plan: &Arc<dyn QueryPlan>,
        stage: usize,
        patch: usize,
        attempt: &str,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<PatchRun>;

    /// Run the result stage and return the query's rows.
    async fn run_result(
        &self,
        plan: &Arc<dyn QueryPlan>,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<Vec<RecordBatch>>;

    /// Where stages hand batches to each other.
    fn exchange(&self) -> &Arc<dyn Exchange>;

    /// Remove what a finished query left behind.
    async fn finish(&self, query: &str) -> Result<()>;
}
