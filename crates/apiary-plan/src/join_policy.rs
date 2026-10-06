//! Joins are planned to fit a Bee.
//!
//! DataFusion's hash join keeps its whole build side in memory and cannot
//! spill, so a build side larger than a Bee's memory fails the query. A
//! Raspberry Pi Bee has well under a gigabyte.
//!
//! [`FitJoinsToBee`] runs right after DataFusion's own join selection. It
//! estimates each hash join's build side from the plan's statistics (which, for
//! Frames, come from the Delta log) and keeps the hash join only when that
//! estimate is known and fits. Otherwise it replaces the join with a sort-merge
//! join, which sorts through the memory pool and spills. A spilling hash join,
//! which DataFusion contributors are working on, would retire this rule.
//!
//! The replacement happens before DataFusion decides where to repartition and
//! sort, so it adds whatever the sort-merge join then requires.

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use tracing::debug;

/// A build side may use at most this share of one Bee's memory, as a
/// fraction of 1/`BUILD_SHARE_DENOMINATOR`. The rest is for the probe side,
/// aggregation state and the Bee's other operators.
const BUILD_SHARE_DENOMINATOR: u64 = 2;

/// The hash table costs about this many bytes per build row, on top of the
/// rows themselves (a hash and a chain link per row). It dominates for narrow
/// rows: a join on a single 8-byte key takes about 27 bytes per row.
const HASH_TABLE_BYTES_PER_ROW: u64 = 24;

/// Slack on the estimate, as numerator and denominator (25%).
const SLACK: (u64, u64) = (5, 4);

/// Width assumed for a variable-length value when a plan reports row counts
/// but not byte sizes.
const VARIABLE_WIDTH_GUESS: u64 = 32;

/// Replaces hash joins whose build side may not fit a Bee with sort-merge
/// joins.
#[derive(Debug)]
pub struct FitJoinsToBee {
    memory_per_bee: u64,
    partitions: usize,
}

/// What to do with one hash join.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// The build side is known and fits.
    Fits,
    /// The build side is larger than a Bee can hold.
    TooBig,
    /// The plan gives no usable size estimate.
    Unknown,
}

impl FitJoinsToBee {
    /// A rule for Bees with `memory_per_bee` bytes, on a Node that runs
    /// queries over `partitions` partitions.
    pub fn new(memory_per_bee: u64, partitions: usize) -> Self {
        Self {
            memory_per_bee,
            partitions: partitions.max(1),
        }
    }

    fn judge(&self, join: &HashJoinExec) -> Verdict {
        let Some(size) = estimated_size(join.left()) else {
            return Verdict::Unknown;
        };
        let footprint = size
            .bytes
            .saturating_add(size.rows.saturating_mul(HASH_TABLE_BYTES_PER_ROW))
            .saturating_mul(SLACK.0)
            / SLACK.1;
        // A collect-left join builds the whole side in one place; a
        // partitioned join builds one share per partition.
        let per_build = match join.partition_mode() {
            PartitionMode::CollectLeft => footprint,
            _ => footprint / self.partitions as u64,
        };
        let allowed = self.memory_per_bee / BUILD_SHARE_DENOMINATOR;
        if per_build <= allowed {
            Verdict::Fits
        } else {
            Verdict::TooBig
        }
    }
}

impl PhysicalOptimizerRule for FitJoinsToBee {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            let verdict = self.judge(join);
            if verdict == Verdict::Fits {
                return Ok(Transformed::no(node));
            }
            let sort_options = vec![SortOptions::default(); join.on().len()];
            let replacement = SortMergeJoinExec::try_new(
                Arc::clone(join.left()),
                Arc::clone(join.right()),
                join.on().to_vec(),
                join.filter().cloned(),
                *join.join_type(),
                sort_options,
                join.null_equality(),
            );
            match replacement {
                Ok(smj) => {
                    debug!(?verdict, "Hash join replaced by sort-merge join");
                    let smj: Arc<dyn ExecutionPlan> = Arc::new(smj);
                    // A hash join may already carry a column projection (an
                    // empty one for count(*)); the sort-merge join outputs
                    // every column, so project the same ones.
                    let replaced = match &join.projection {
                        None => smj,
                        Some(columns) => {
                            let schema = smj.schema();
                            let exprs: Vec<ProjectionExpr> = columns
                                .iter()
                                .map(|&i| {
                                    let name = schema.field(i).name().clone();
                                    ProjectionExpr {
                                        expr: Arc::new(Column::new(&name, i)),
                                        alias: name,
                                    }
                                })
                                .collect();
                            Arc::new(ProjectionExec::try_new(exprs, smj)?)
                        }
                    };
                    Ok(Transformed::yes(replaced))
                }
                // A join type the sort-merge join cannot do stays a hash join.
                Err(_) => Ok(Transformed::no(node)),
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "fit_joins_to_bee"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// How big a plan's output is expected to be.
struct Size {
    rows: u64,
    bytes: u64,
}

/// The estimated size of a plan's output, if it can be known.
fn estimated_size(plan: &Arc<dyn ExecutionPlan>) -> Option<Size> {
    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .ok()?;
    let width = row_width(&plan.schema());
    let rows = stats.num_rows.get_value().map(|r| *r as u64);
    let bytes = stats.total_byte_size.get_value().map(|b| *b as u64);
    match (rows, bytes) {
        (Some(rows), Some(bytes)) => Some(Size { rows, bytes }),
        (Some(rows), None) => Some(Size {
            rows,
            bytes: rows.saturating_mul(width),
        }),
        (None, Some(bytes)) => Some(Size {
            rows: bytes / width,
            bytes,
        }),
        (None, None) => None,
    }
}

/// Rough bytes per row for a schema.
fn row_width(schema: &SchemaRef) -> u64 {
    schema
        .fields()
        .iter()
        .map(|f| fixed_width(f.data_type()).unwrap_or(VARIABLE_WIDTH_GUESS))
        .sum::<u64>()
        .max(1)
}

fn fixed_width(dt: &DataType) -> Option<u64> {
    dt.primitive_width().map(|w| w as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn table(rows: i64) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("value-{i}")),
                )),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    /// A session with the policy installed after join selection, and two
    /// tables: `small` (known size) and `big`.
    fn session(memory_per_bee: u64) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(4);
        let mut rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
        let at = rules
            .iter()
            .position(|r| r.name() == "join_selection")
            .expect("join_selection rule");
        rules.insert(at + 1, Arc::new(FitJoinsToBee::new(memory_per_bee, 4)));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("small", table(100)).unwrap();
        ctx.register_table("big", table(10_000)).unwrap();
        ctx
    }

    async fn plan_text(ctx: &SessionContext, sql: &str) -> String {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        displayable(plan.as_ref()).indent(true).to_string()
    }

    const JOIN: &str = "SELECT a.k, b.v FROM small a JOIN big b ON a.k = b.k";

    #[tokio::test]
    async fn a_build_side_that_fits_keeps_its_hash_join() {
        let ctx = session(1024 * 1024 * 1024);
        let plan = plan_text(&ctx, JOIN).await;
        assert!(plan.contains("HashJoinExec"), "{plan}");
        assert!(!plan.contains("SortMergeJoinExec"), "{plan}");
    }

    #[tokio::test]
    async fn a_build_side_too_big_for_a_bee_becomes_a_sort_merge_join() {
        // The small side is a few KB; a 1 KB Bee cannot hold it.
        let ctx = session(1024);
        let plan = plan_text(&ctx, JOIN).await;
        assert!(plan.contains("SortMergeJoinExec"), "{plan}");
        assert!(!plan.contains("HashJoinExec"), "{plan}");
    }

    #[tokio::test]
    async fn a_join_with_an_empty_projection_is_replaced_too() {
        // count(*) needs no columns, so the hash join carries an empty
        // projection. It must not escape the policy.
        let ctx = session(1024);
        let plan = plan_text(
            &ctx,
            "SELECT count(*) AS n FROM small a JOIN big b ON a.k = b.k",
        )
        .await;
        assert!(plan.contains("SortMergeJoinExec"), "{plan}");
        assert!(!plan.contains("HashJoinExec"), "{plan}");
    }

    #[tokio::test]
    async fn both_plans_give_the_same_answer() {
        let sql = "SELECT count(*) AS n, sum(a.k) AS s FROM small a JOIN big b ON a.k = b.k";
        let roomy = session(1024 * 1024 * 1024);
        let cramped = session(1024);
        let a = roomy.sql(sql).await.unwrap().collect().await.unwrap();
        let b = cramped.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&a)
                .unwrap()
                .to_string(),
            arrow::util::pretty::pretty_format_batches(&b)
                .unwrap()
                .to_string()
        );
    }

    /// A session whose memory pool is far smaller than the join's build
    /// side, with the policy believing Bees hold `memory_per_bee` bytes.
    fn cramped_session(memory_per_bee: u64) -> SessionContext {
        use datafusion::execution::memory_pool::FairSpillPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;

        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(24 * 1024 * 1024)))
                .build()
                .unwrap(),
        );
        let mut rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
        let at = rules
            .iter()
            .position(|r| r.name() == "join_selection")
            .unwrap();
        rules.insert(at + 1, Arc::new(FitJoinsToBee::new(memory_per_bee, 4)));
        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_target_partitions(4)
                    // Sorts reserve memory to spill; scale that to the pool.
                    .set_usize(
                        "datafusion.execution.sort_spill_reservation_bytes",
                        256 * 1024,
                    ),
            )
            .with_runtime_env(runtime)
            .with_default_features()
            .with_physical_optimizer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("left_t", table(1_000_000)).unwrap();
        ctx.register_table("right_t", table(1_000_000)).unwrap();
        ctx
    }

    const BIG_JOIN: &str = "SELECT count(*) AS n FROM left_t a JOIN right_t b ON a.k = b.k";

    async fn count(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<i64> {
        let rows = ctx.sql(sql).await?.collect().await?;
        Ok(rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0))
    }

    #[tokio::test]
    async fn without_the_policy_a_build_side_larger_than_the_pool_fails() {
        // Control: a policy that thinks Bees are enormous keeps the hash
        // join, which cannot spill, and the query runs out of memory.
        let ctx = cramped_session(u64::MAX / 4);
        let err = count(&ctx, BIG_JOIN).await.unwrap_err();
        assert!(
            err.to_string().contains("exhausted") || err.to_string().contains("memory"),
            "expected a memory failure, got: {err}"
        );
    }

    #[tokio::test]
    async fn with_the_policy_the_same_join_completes_by_spilling() {
        // Bees hold 8 MB, far less than the build side, so the join becomes a
        // sort-merge join and the same query succeeds in the same pool.
        let ctx = cramped_session(8 * 1024 * 1024);
        assert_eq!(count(&ctx, BIG_JOIN).await.unwrap(), 1_000_000);
    }

    #[test]
    fn row_width_counts_fixed_and_guessed_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        assert_eq!(row_width(&schema), 8 + VARIABLE_WIDTH_GUESS);
    }
}
