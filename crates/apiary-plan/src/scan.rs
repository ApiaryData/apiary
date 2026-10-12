//! Scans that can travel.
//!
//! A query's plan has to run on Nodes other than the one that planned it, so every
//! leaf of it must be describable in a few bytes. A Delta scan is not: it carries
//! the snapshot's file metadata. A [`FrameScanExec`] instead carries a
//! [`ScanSpec`]: which Frame, at which table version, which columns and filters,
//! and which files each of its partitions reads. The Node that runs a Patch
//! rebuilds the scan from the spec through the comb, so two Nodes given the same
//! spec read the same rows.
//!
//! On the Node that planned the query, a `FrameScanExec` simply delegates to the
//! scan it was made from, so planning and running a query on one Node costs only
//! one more node in the plan.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::TableType;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, ReplaceChildrenOptions,
};
use datafusion_proto::bytes::Serializeable;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use apiary_comb::Comb;

use crate::exchange::{schema_from_bytes, schema_to_bytes};
use crate::models::Models;
use crate::patch::PatchEnv;

/// Everything needed to rebuild a Frame scan on any Node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanSpec {
    /// The Frame.
    pub hive: String,
    /// The Frame's Box.
    pub box_name: String,
    /// The Frame's name.
    pub frame: String,
    /// The table version the query reads.
    pub version: i64,
    /// Columns read, as indices into the Frame's schema.
    pub projection: Option<Vec<usize>>,
    /// Filters the scan was given, as serialised logical expressions.
    pub filters: Vec<Vec<u8>>,
    /// A row limit pushed into the scan.
    pub limit: Option<usize>,
    /// The files each partition reads, relative to the table's root.
    pub groups: Vec<Vec<String>>,
    /// The scan's output schema, as an Arrow IPC schema.
    pub schema: Vec<u8>,
}

impl ScanSpec {
    /// The scan's output schema.
    pub fn output_schema(&self) -> DfResult<SchemaRef> {
        schema_from_bytes(&self.schema)
    }
}

/// What a Node must provide to rebuild scans from specs: the comb and a session
/// state to build scans in.
#[derive(Clone)]
pub struct ScanSource {
    comb: Arc<Comb>,
    state: SessionState,
    models: Arc<Models>,
}

impl ScanSource {
    /// A source over `comb`, building scans in `state`.
    pub fn new(comb: Arc<Comb>, state: SessionState, models: Arc<Models>) -> Self {
        Self {
            comb,
            state,
            models,
        }
    }

    /// The models a Patch's `predict` calls use.
    pub fn models(&self) -> &Arc<Models> {
        &self.models
    }

    /// The session state scans are built in; a Patch runs under its runtime.
    pub fn state(&self) -> &SessionState {
        &self.state
    }

    /// A scan of one partition of `spec`: its files, at its version.
    async fn scan_partition(
        &self,
        spec: &ScanSpec,
        partition: usize,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let files = spec.groups.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "scan of {}.{}.{} has no partition {partition}",
                spec.hive, spec.box_name, spec.frame
            ))
        })?;
        // No files for this partition (pruning left none): there is nothing to scan,
        // and an empty selection must not be mistaken for "every file".
        if files.is_empty() {
            return Ok(Arc::new(EmptyExec::new(spec.output_schema()?)));
        }
        let provider = self
            .comb
            .table_provider_at(
                &self.state,
                &spec.hive,
                &spec.box_name,
                &spec.frame,
                spec.version,
                files,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let registry = self.state.task_ctx();
        let filters = spec
            .filters
            .iter()
            .map(|bytes| Expr::from_bytes_with_ctx(bytes, registry.as_ref()))
            .collect::<DfResult<Vec<_>>>()?;
        let plan = provider
            .scan(&self.state, spec.projection.as_ref(), &filters, spec.limit)
            .await?;
        // One Patch reads its files in sequence.
        Ok(if plan.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        })
    }
}

/// A Frame scan whose spec travels with it.
pub struct FrameScanExec {
    spec: Arc<ScanSpec>,
    schema: SchemaRef,
    /// The scan this was made from, on the Node that planned the query. A scan
    /// decoded elsewhere has none and is rebuilt from the spec.
    local: Option<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for FrameScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameScanExec")
            .field("frame", &self.frame_name())
            .field("version", &self.spec.version)
            .field("partitions", &self.spec.groups.len())
            .finish()
    }
}

impl FrameScanExec {
    /// A scan made from `local`, described by `spec`.
    pub fn from_local(spec: ScanSpec, local: Arc<dyn ExecutionPlan>) -> DfResult<Self> {
        let schema = local.schema();
        let properties = Arc::clone(local.properties());
        Ok(Self {
            spec: Arc::new(spec),
            schema,
            local: Some(local),
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// A scan decoded from `spec`, to be rebuilt where it runs.
    pub fn from_spec(spec: ScanSpec) -> DfResult<Self> {
        let schema = spec.output_schema()?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(spec.groups.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            spec: Arc::new(spec),
            schema,
            local: None,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The spec.
    pub fn spec(&self) -> &ScanSpec {
        &self.spec
    }

    fn frame_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.spec.hive, self.spec.box_name, self.spec.frame
        )
    }

    /// The same scan, keeping only what it needs to travel.
    pub fn detached(&self) -> Self {
        Self {
            spec: Arc::clone(&self.spec),
            schema: Arc::clone(&self.schema),
            local: None,
            properties: Arc::clone(&self.properties),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for FrameScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "FrameScanExec: {} at version {}, {} files in {} partitions",
                self.frame_name(),
                self.spec.version,
                self.spec.groups.iter().map(Vec::len).sum::<usize>(),
                self.spec.groups.len()
            ),
            DisplayFormatType::TreeRender => write!(f, "frame={}", self.frame_name()),
        }
    }
}

impl ExecutionPlan for FrameScanExec {
    fn name(&self) -> &'static str {
        "FrameScanExec"
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
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        // Inside a Patch the scan is always rebuilt from its spec, so every attempt
        // at a Patch reads the same files whatever else is running. A query run
        // whole uses the scan it was planned with.
        let in_patch = context
            .session_config()
            .get_extension::<PatchEnv>()
            .is_some();
        let stream: SendableRecordBatchStream = match (&self.local, in_patch) {
            (Some(local), false) => local.execute(partition, context)?,
            _ => {
                let spec = Arc::clone(&self.spec);
                let schema = Arc::clone(&self.schema);
                let built = async move {
                    let env = context
                        .session_config()
                        .get_extension::<PatchEnv>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "a Patch's scan needs the Patch environment".into(),
                            )
                        })?;
                    let plan = env.scans().scan_partition(&spec, partition).await?;
                    plan.execute(0, context)
                };
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::once(built).try_flatten(),
                ))
            }
        };
        // Count what the scan produced, so a query can report rows read.
        let counted = stream.inspect_ok(move |batch| {
            baseline.record_output(batch.num_rows());
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            counted,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> DfResult<Arc<Statistics>> {
        // The scan this was made from knows its files' statistics; planning
        // (join order, build sides) depends on them.
        match &self.local {
            Some(local) => StatisticsContext::new().compute(local.as_ref(), args),
            None => Ok(Arc::new(Statistics::new_unknown(&self.schema))),
        }
    }
}

/// A Frame's comb table as a provider whose scans carry specs.
#[derive(Debug)]
pub(crate) struct FramePatchProvider {
    inner: Arc<dyn TableProvider>,
    hive: String,
    box_name: String,
    frame: String,
    version: i64,
    /// The table's root within its object store, without a leading slash.
    root: String,
}

impl FramePatchProvider {
    pub(crate) fn new(
        inner: Arc<dyn TableProvider>,
        frame: (&str, &str, &str),
        version: i64,
        root: &str,
    ) -> Self {
        Self {
            inner,
            hive: frame.0.to_string(),
            box_name: frame.1.to_string(),
            frame: frame.2.to_string(),
            version,
            root: root.trim_matches('/').to_string(),
        }
    }

    /// The files each partition of `plan` reads, if it is a Delta scan over
    /// plain Parquet files, as paths relative to the table root.
    fn groups_of(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<Vec<String>>> {
        let child = plan.children().into_iter().next()?;
        // Pruning removed every file: nothing to read, in as many partitions as the
        // scan has.
        if child.name() == "EmptyExec" {
            return Some(vec![
                Vec::new();
                plan.output_partitioning().partition_count().max(1)
            ]);
        }
        let source = child.downcast_ref::<DataSourceExec>()?;
        let config = source.data_source().downcast_ref::<FileScanConfig>()?;
        let prefix = format!("{}/", self.root);
        config
            .file_groups
            .iter()
            .map(|group| {
                group
                    .files()
                    .iter()
                    .map(|file| {
                        let path = file.object_meta.location.as_ref();
                        path.strip_prefix(&prefix).map(str::to_string)
                    })
                    .collect::<Option<Vec<String>>>()
            })
            .collect()
    }
}

#[async_trait]
impl TableProvider for FramePatchProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        // A scan we cannot describe still runs here; it just cannot travel.
        let Some(groups) = self.groups_of(&plan) else {
            return Ok(plan);
        };
        if groups.len() != plan.output_partitioning().partition_count() {
            return Ok(plan);
        }
        let filters = filters
            .iter()
            .map(|f| f.to_bytes().map(|b| b.to_vec()))
            .collect::<DfResult<Vec<_>>>();
        let Ok(filters) = filters else {
            return Ok(plan);
        };
        let spec = ScanSpec {
            hive: self.hive.clone(),
            box_name: self.box_name.clone(),
            frame: self.frame.clone(),
            version: self.version,
            projection: projection.cloned(),
            filters,
            limit,
            groups,
            schema: schema_to_bytes(&plan.schema())?,
        };
        Ok(Arc::new(FrameScanExec::from_local(spec, plan)?))
    }
}
