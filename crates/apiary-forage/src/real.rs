//! The executor a Node uses: DataFusion plans, comb-backed exchange.

use std::any::Any;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::memory_pool::MemoryPool;

use apiary_core::{ApiaryError, Result, StorageBackend};
use apiary_plan::ApiaryQueryContext;
use apiary_plan::codec::{decode_plan, encode_plan};
use apiary_plan::exchange::Exchange;
use apiary_plan::patch::{PatchEnv, run_patch, run_result};
use apiary_plan::stages::{self, StagePlan};

use crate::exec::{Executor, PatchRun};
use crate::plan::{QueryPlan, StageInfo};

/// A query cut into stages, with what a Node needs to run its Patches.
pub struct StagedQuery {
    query: String,
    info: Vec<StageInfo>,
    staged: StagePlan,
    env: Arc<PatchEnv>,
}

impl QueryPlan for StagedQuery {
    fn query(&self) -> &str {
        &self.query
    }

    fn stages(&self) -> &[StageInfo] {
        &self.info
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn describe(staged: &StagePlan) -> Vec<StageInfo> {
    let root = staged.root().id;
    staged
        .stages
        .iter()
        .map(|s| StageInfo {
            id: s.id,
            patches: s.patches(),
            inputs: s.inputs(),
            pinned: s.is_pinned(),
            result: s.id == root,
        })
        .collect()
}

/// Plans with a Node's query context and runs Patches under a Bee's share of memory.
pub struct PlanExecutor {
    ctx: Arc<ApiaryQueryContext>,
    exchange: Arc<dyn Exchange>,
    store: Arc<dyn StorageBackend>,
    node: String,
}

impl PlanExecutor {
    /// An executor for `node`, planning with `ctx` and keeping plans in `store`.
    pub fn new(
        node: impl Into<String>,
        ctx: Arc<ApiaryQueryContext>,
        exchange: Arc<dyn Exchange>,
        store: Arc<dyn StorageBackend>,
    ) -> Self {
        Self {
            ctx,
            exchange,
            store,
            node: node.into(),
        }
    }

    fn record_key(query: &str) -> String {
        format!("plans/{query}")
    }

    fn env(
        &self,
        query: &str,
        held: std::collections::HashMap<String, Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> Result<Arc<PatchEnv>> {
        Ok(PatchEnv::new(
            query,
            self.node.clone(),
            Arc::clone(&self.exchange),
            self.ctx.scan_source(None)?,
            held,
        ))
    }
}

fn staged_of(plan: &Arc<dyn QueryPlan>) -> Result<&StagedQuery> {
    plan.as_any()
        .downcast_ref::<StagedQuery>()
        .ok_or_else(|| ApiaryError::Internal {
            message: "a plan from another executor".into(),
        })
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiaryError {
    ApiaryError::Internal {
        message: format!("{context}: {e}"),
    }
}

#[async_trait]
impl Executor for PlanExecutor {
    async fn plan(&self, query: &str, sql: &str) -> Result<Arc<dyn QueryPlan>> {
        let planned = self.ctx.plan_sql(sql, None).await?;
        let mut staged = stages::cut(Arc::clone(&planned.physical))
            .map_err(|e| internal("Cannot cut the query into stages", e))?;
        let held = std::mem::take(&mut staged.held);
        let record = encode_plan(query, &self.node, &staged)
            .map_err(|e| internal("Cannot write the plan", e))?;
        self.store
            .put(&Self::record_key(query), Bytes::from(record))
            .await?;
        Ok(Arc::new(StagedQuery {
            query: query.to_string(),
            info: describe(&staged),
            env: self.env(query, held)?,
            staged,
        }))
    }

    async fn load(&self, query: &str) -> Result<Arc<dyn QueryPlan>> {
        let bytes = self.store.get(&Self::record_key(query)).await?;
        let task = self.ctx.scan_source(None)?.state().task_ctx();
        let (_, staged) =
            decode_plan(&bytes, &task).map_err(|e| internal("Cannot read the plan", e))?;
        Ok(Arc::new(StagedQuery {
            query: query.to_string(),
            info: describe(&staged),
            env: self.env(query, Default::default())?,
            staged,
        }))
    }

    async fn run_patch(
        &self,
        plan: &Arc<dyn QueryPlan>,
        stage: usize,
        patch: usize,
        attempt: &str,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<PatchRun> {
        let q = staged_of(plan)?;
        let stage = q
            .staged
            .stages
            .get(stage)
            .ok_or_else(|| internal("No such stage", stage))?;
        let task = q
            .env
            .task_context(pool)
            .map_err(|e| internal("Cannot build the Patch context", e))?;
        let outcome = run_patch(stage, patch, &q.env, task, attempt).await?;
        Ok(PatchRun {
            completed: outcome.completed,
            rows: outcome.rows,
        })
    }

    async fn run_result(
        &self,
        plan: &Arc<dyn QueryPlan>,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<Vec<RecordBatch>> {
        let q = staged_of(plan)?;
        let task = q
            .env
            .task_context(pool)
            .map_err(|e| internal("Cannot build the Patch context", e))?;
        run_result(q.staged.root(), task).await
    }

    fn exchange(&self) -> &Arc<dyn Exchange> {
        &self.exchange
    }

    async fn finish(&self, query: &str) -> Result<()> {
        self.exchange.clear(query).await?;
        self.store.delete(&Self::record_key(query)).await
    }
}
