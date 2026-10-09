//! Cutting a query into stages, and stages into Patches.
//!
//! DataFusion plans a query as a tree of operators over partitions. Everywhere
//! the tree moves rows between partitions (a hash or round-robin repartition, or
//! a merge of partitions into one) the plan can be cut: the part below the cut
//! becomes a **stage**, and the part above reads the stage's output through a
//! [`StageInputExec`]. Every operator left inside a stage maps partition *i* of
//! its inputs to partition *i* of its output, so each partition of a stage is an
//! independent unit of work, a **Patch**, that any Node can run.
//!
//! A stage is kept as the plan node at its top edge (its *boundary*): the
//! repartition or merge itself. The Patches run the boundary's input and then do
//! what the boundary does, splitting rows by the repartition's partitioning, or
//! passing them on whole for a merge. Because the boundary is an ordinary plan
//! node, `datafusion-proto` serialises it with its expressions, and the plan can
//! be written to the comb store and run by Nodes that never saw the query.
//!
//! Leaves that cannot travel (rows held in this Node's memory, such as a crop
//! read) are wrapped in a [`HeldExec`]; the stages that contain them are pinned
//! to the Node that planned the query.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use futures::{StreamExt, TryStreamExt};

use crate::exchange::{PatchKey, decode_batches};
use crate::patch::PatchEnv;

/// The plan nodes that survive a trip through `datafusion-proto` unaided and
/// have no state of their own, which may stay as leaves of a stage.
const PORTABLE_LEAVES: [&str; 4] = [
    "FrameScanExec",
    "StageInputExec",
    "EmptyExec",
    "PlaceholderRowExec",
];

/// How a stage's Patches hand over their output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Handoff {
    /// Rows are split by hash or round robin into this many output partitions,
    /// and the stage above reads output *q* of every Patch for its partition *q*.
    Split(usize),
    /// Each Patch's output is passed on whole, as one output, and the stage above
    /// reads Patch *i*'s output for its partition *i*.
    Whole,
    /// The stage is the root: its one Patch's rows are the query's result.
    Result,
}

/// A stage of a query.
#[derive(Clone, Debug)]
pub struct Stage {
    /// Its position: stages that feed it have lower ids.
    pub id: usize,
    /// The plan node at its top edge: a repartition or merge for a stage that
    /// feeds another, the root of the plan for the result stage.
    pub boundary: Arc<dyn ExecutionPlan>,
    /// How its Patches hand over their output.
    pub handoff: Handoff,
}

impl Stage {
    /// The plan each Patch runs: the boundary's input, or for the result stage
    /// the root itself.
    pub fn body(&self) -> Arc<dyn ExecutionPlan> {
        match self.handoff {
            Handoff::Result => Arc::clone(&self.boundary),
            _ => Arc::clone(self.boundary.children()[0]),
        }
    }

    /// The plan a Patch attempt runs: a copy of the body with its execution state
    /// reset. Operators share state between the partitions of one instance (a
    /// scan hands files to whichever partition asks, a hash join builds once), so
    /// each attempt runs its own copy and sees the same rows however many times
    /// the Patch is run.
    pub fn fresh_body(&self) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.body()
            .transform_up(|node| Ok(Transformed::yes(node.reset_state()?)))
            .map(|t| t.data)
    }

    /// How many Patches the stage has: the partitions of the plan it runs.
    pub fn patches(&self) -> usize {
        self.body().output_partitioning().partition_count()
    }

    /// The stages whose output this stage reads.
    pub fn inputs(&self) -> Vec<usize> {
        let mut inputs = Vec::new();
        visit(&self.body(), &mut |node| {
            if let Some(input) = node.downcast_ref::<StageInputExec>() {
                inputs.push(input.stage);
            }
        });
        inputs.sort_unstable();
        inputs.dedup();
        inputs
    }

    /// Whether the stage reads rows that exist only in the planning Node's memory.
    pub fn is_pinned(&self) -> bool {
        let mut pinned = false;
        visit(&self.body(), &mut |node| {
            pinned |= node.downcast_ref::<HeldExec>().is_some();
        });
        pinned
    }

    /// The output schema of the stage's Patches.
    pub fn schema(&self) -> SchemaRef {
        self.body().schema()
    }
}

/// A query cut into stages, in an order that runs each after those it reads.
#[derive(Clone, Debug)]
pub struct StagePlan {
    /// The stages; the last is the result stage.
    pub stages: Vec<Stage>,
    /// Leaves that cannot travel, by key, for the Node that planned the query.
    pub held: HashMap<String, Arc<dyn ExecutionPlan>>,
}

impl StagePlan {
    /// The result stage.
    pub fn root(&self) -> &Stage {
        self.stages.last().expect("a plan has a result stage")
    }
}

/// `plan` over `children`, its properties worked out afresh.
fn rebuild(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    plan.replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

fn visit(plan: &Arc<dyn ExecutionPlan>, f: &mut dyn FnMut(&Arc<dyn ExecutionPlan>)) {
    f(plan);
    for child in plan.children() {
        visit(child, f);
    }
}

/// Cut `plan` into stages.
pub fn cut(plan: Arc<dyn ExecutionPlan>) -> DfResult<StagePlan> {
    // The result is one stream: merge whatever the root has left unmerged.
    let root = if plan.output_partitioning().partition_count() > 1 {
        Arc::new(CoalescePartitionsExec::new(plan)) as Arc<dyn ExecutionPlan>
    } else {
        plan
    };
    let mut cutter = Cutter::default();
    let root = cutter.cut(root)?;
    let id = cutter.stages.len();
    cutter.stages.push(Stage {
        id,
        boundary: root,
        handoff: Handoff::Result,
    });
    Ok(StagePlan {
        stages: cutter.stages,
        held: cutter.held,
    })
}

#[derive(Default)]
struct Cutter {
    stages: Vec<Stage>,
    held: HashMap<String, Arc<dyn ExecutionPlan>>,
}

impl Cutter {
    fn cut(&mut self, plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let children: Vec<Arc<dyn ExecutionPlan>> =
            plan.children().into_iter().map(Arc::clone).collect();
        if children.is_empty() {
            return Ok(self.leaf(plan));
        }
        let cut_children = children
            .iter()
            .map(|c| self.cut(Arc::clone(c)))
            .collect::<DfResult<Vec<_>>>()?;
        let changed = cut_children
            .iter()
            .zip(&children)
            .any(|(new, old)| !Arc::ptr_eq(new, old));
        let node = if changed {
            rebuild(plan, cut_children)?
        } else {
            plan
        };

        if let Some(repartition) = node.downcast_ref::<RepartitionExec>() {
            if repartition.preserve_order() {
                return Err(DataFusionError::NotImplemented(
                    "an order-preserving repartition cannot be cut into Patches".into(),
                ));
            }
            let partitions = match repartition.partitioning() {
                Partitioning::Hash(_, n) | Partitioning::RoundRobinBatch(n) => *n,
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "cannot cut a repartition by {other}"
                    )));
                }
            };
            let producer = self.stage(Arc::clone(&node), Handoff::Split(partitions));
            return Ok(Arc::new(StageInputExec::new(
                producer.id,
                node.schema(),
                producer.patches(),
                partitions,
                false,
                None,
            )));
        }
        let merges = node.downcast_ref::<CoalescePartitionsExec>().is_some()
            || node.downcast_ref::<SortPreservingMergeExec>().is_some();
        if merges {
            let input = Arc::clone(node.children()[0]);
            if input.output_partitioning().partition_count() <= 1 {
                return Ok(node);
            }
            let producer = self.stage(Arc::clone(&node), Handoff::Whole);
            let placeholder = Arc::new(StageInputExec::new(
                producer.id,
                input.schema(),
                producer.patches(),
                producer.patches(),
                true,
                Some(Arc::clone(input.properties())),
            ));
            return rebuild(node, vec![placeholder]);
        }
        Ok(node)
    }

    fn stage(&mut self, boundary: Arc<dyn ExecutionPlan>, handoff: Handoff) -> Stage {
        let stage = Stage {
            id: self.stages.len(),
            boundary,
            handoff,
        };
        self.stages.push(stage.clone());
        stage
    }

    fn leaf(&mut self, plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        if PORTABLE_LEAVES.contains(&plan.name()) {
            return plan;
        }
        let key = format!("held{}", self.held.len());
        self.held.insert(key.clone(), Arc::clone(&plan));
        Arc::new(HeldExec::new(key, plan))
    }
}

// ---------------------------------------------------------------------------
// StageInputExec
// ---------------------------------------------------------------------------

/// The output of a stage, read from the exchange.
pub struct StageInputExec {
    /// The stage it reads.
    pub stage: usize,
    schema: SchemaRef,
    /// How many Patches the stage has.
    pub patches: usize,
    /// How many partitions this reads: output partitions of a split, or Patches
    /// for a whole handoff.
    pub partitions: usize,
    /// Whether each partition is one Patch's whole output (rather than a share
    /// of every Patch's).
    pub whole: bool,
    properties: Arc<PlanProperties>,
}

impl StageInputExec {
    /// A reader of `stage`'s output. `ordering` is the ordering the stage's Patches
    /// keep, for a whole handoff.
    pub fn new(
        stage: usize,
        schema: SchemaRef,
        patches: usize,
        partitions: usize,
        whole: bool,
        ordering: Option<Arc<PlanProperties>>,
    ) -> Self {
        let equivalence = match &ordering {
            Some(p) => p.eq_properties.clone(),
            None => EquivalenceProperties::new(Arc::clone(&schema)),
        };
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            stage,
            schema,
            patches,
            partitions,
            whole,
            properties,
        }
    }
}

impl fmt::Debug for StageInputExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageInputExec")
            .field("stage", &self.stage)
            .field("partitions", &self.partitions)
            .finish()
    }
}

impl DisplayAs for StageInputExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "StageInputExec: stage={}, patches={}, partitions={}",
                self.stage, self.patches, self.partitions
            ),
            DisplayFormatType::TreeRender => write!(f, "stage={}", self.stage),
        }
    }
}

/// A stage input was read before the stage had finished.
#[derive(Debug)]
pub struct InputNotReady(pub PatchKey);

impl fmt::Display for InputNotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the output of {:?} is not complete yet", self.0)
    }
}

impl std::error::Error for InputNotReady {}

impl ExecutionPlan for StageInputExec {
    fn name(&self) -> &'static str {
        "StageInputExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition >= self.partitions {
            return Err(DataFusionError::Internal(format!(
                "stage input {} has no partition {partition}",
                self.stage
            )));
        }
        let (stage, whole, patches) = (self.stage, self.whole, self.patches);
        let patch_indices: Vec<usize> = if whole {
            vec![partition]
        } else {
            (0..patches).collect()
        };
        let output = if whole { 0 } else { partition };
        let schema = Arc::clone(&self.schema);
        let stream = futures::stream::iter(patch_indices)
            .then(move |index| {
                let context = Arc::clone(&context);
                async move {
                    let env = context
                        .session_config()
                        .get_extension::<PatchEnv>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "a stage input needs the Patch environment".into(),
                            )
                        })?;
                    let key = PatchKey::new(env.query(), stage, index);
                    let exchange = env.exchange();
                    let done = exchange
                        .completion(&key)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .ok_or_else(|| {
                            DataFusionError::External(Box::new(InputNotReady(key.clone())))
                        })?;
                    let bytes = exchange
                        .get(&key, &done, output)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    decode_batches(bytes)
                }
            })
            .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

// ---------------------------------------------------------------------------
// HeldExec
// ---------------------------------------------------------------------------

/// A leaf whose rows exist only in the memory of the Node that planned the query.
pub struct HeldExec {
    /// How the planning Node finds it again.
    pub key: String,
    inner: Option<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}

impl HeldExec {
    /// Wrap `inner` as the leaf called `key`.
    pub fn new(key: String, inner: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(inner.properties());
        Self {
            key,
            inner: Some(inner),
            properties,
        }
    }

    /// A held leaf decoded from a plan record, to be found by key where it runs.
    pub fn decoded(key: String, schema: SchemaRef, partitions: usize) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            key,
            inner: None,
            properties,
        }
    }
}

impl fmt::Debug for HeldExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldExec").field("key", &self.key).finish()
    }
}

impl DisplayAs for HeldExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "HeldExec: {}", self.key)
            }
            DisplayFormatType::TreeRender => write!(f, "held={}", self.key),
        }
    }
}

impl ExecutionPlan for HeldExec {
    fn name(&self) -> &'static str {
        "HeldExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let inner = match &self.inner {
            Some(inner) => Arc::clone(inner),
            None => context
                .session_config()
                .get_extension::<PatchEnv>()
                .and_then(|env| env.held(&self.key))
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "input {} is held in the memory of the Node that planned the query",
                        self.key
                    ))
                })?,
        };
        inner.execute(partition, context)
    }
}
