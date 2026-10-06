//! The Node's query session: memory pool, spill directory, catalogue and
//! join policy, built once and shared by every query.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::prelude::{SessionConfig, SessionContext};

use apiary_comb::{Comb, Crop};
use apiary_core::config::NodeConfig;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::{ApiaryError, Result};

use crate::catalog::{ApiaryCatalogList, CatalogShared, NO_BOX, NO_HIVE};
use crate::join_policy::FitJoinsToBee;

/// The share of a Node's memory given to queries; the rest is left for the
/// operating system, the crop and the other parts of the Node. Expressed as
/// numerator and denominator.
const POOL_SHARE: (u64, u64) = (3, 4);

/// The smallest memory pool a Node will run queries with.
const MIN_POOL_BYTES: u64 = 64 * 1024 * 1024;

/// DataFusion reserves this much memory for each sort so it can always spill
/// and merge. The default is 10 MB per sorter, which a small pool cannot
/// afford across several partitions.
const MAX_SORT_RESERVATION: u64 = 10 * 1024 * 1024;
const MIN_SORT_RESERVATION: u64 = 256 * 1024;

/// How a Node runs queries.
#[derive(Clone, Debug)]
pub struct QueryOptions {
    /// Size of the memory pool all queries share, in bytes. Zero means
    /// unbounded.
    pub memory_pool_bytes: u64,
    /// Memory one Bee may use, which the join policy plans build sides to fit.
    pub memory_per_bee: u64,
    /// Partitions queries run over: one per Bee.
    pub target_partitions: usize,
    /// Where operators that exceed the pool spill to. `None` disables
    /// spilling to a chosen directory (DataFusion then uses the system
    /// temporary directory).
    pub spill_dir: Option<PathBuf>,
    /// This Node's crop. When set, every Frame includes the rows ingested to it
    /// and not yet deposited into the comb.
    pub crop: Option<Arc<Crop>>,
}

impl Default for QueryOptions {
    /// Unbounded memory, one partition per core: suitable for tests and tools.
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        Self {
            memory_pool_bytes: 0,
            memory_per_bee: u64::MAX / 4,
            target_partitions: cores,
            spill_dir: None,
            crop: None,
        }
    }
}

impl QueryOptions {
    /// Options for a Node: a pool of three quarters of its memory (at least
    /// 64 MB), one partition per Bee, and a spill directory under the Node's
    /// cache directory.
    pub fn from_node(config: &NodeConfig) -> Self {
        let pool = (config.memory_bytes / POOL_SHARE.1 * POOL_SHARE.0).max(MIN_POOL_BYTES);
        Self {
            memory_pool_bytes: pool,
            memory_per_bee: config.memory_per_bee,
            target_partitions: config.cores.max(1),
            spill_dir: Some(config.cache_dir.join("spill")),
            crop: None,
        }
    }
}

/// Build the Node's long-lived query session.
pub(crate) fn build_session(
    options: &QueryOptions,
    registry: Arc<RegistryManager>,
    comb: Arc<Comb>,
) -> Result<SessionContext> {
    let mut runtime = RuntimeEnvBuilder::new();
    if options.memory_pool_bytes > 0 {
        runtime = runtime.with_memory_pool(Arc::new(FairSpillPool::new(
            options.memory_pool_bytes as usize,
        )));
    }
    if let Some(dir) = &options.spill_dir {
        std::fs::create_dir_all(dir).map_err(|e| {
            ApiaryError::storage(format!("Failed to create spill directory {dir:?}"), e)
        })?;
        runtime = runtime.with_disk_manager_builder(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![dir.clone()])),
        );
    }
    let runtime = Arc::new(runtime.build().map_err(|e| ApiaryError::Internal {
        message: format!("Failed to build the query runtime: {e}"),
    })?);

    // Each partition's sorts keep a reservation for spilling; size it so all
    // partitions together take about an eighth of the pool.
    let sort_reservation = if options.memory_pool_bytes > 0 {
        (options.memory_pool_bytes / (options.target_partitions.max(1) as u64 * 8))
            .clamp(MIN_SORT_RESERVATION, MAX_SORT_RESERVATION)
    } else {
        MAX_SORT_RESERVATION
    };

    // Results leave the Node as Arrow IPC, where view types are not
    // universally readable, so scans return plain Utf8 and Binary.
    let mut config = SessionConfig::new()
        .with_target_partitions(options.target_partitions)
        .with_default_catalog_and_schema(NO_HIVE, NO_BOX)
        .set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        )
        .set_usize(
            "datafusion.execution.sort_spill_reservation_bytes",
            sort_reservation as usize,
        );
    config
        .options_mut()
        .catalog
        .create_default_catalog_and_schema = false;

    // A state with the Node's runtime and settings, used by the catalogue to
    // build table scans.
    let scan_state = SessionStateBuilder::new()
        .with_config(config.clone())
        .with_runtime_env(Arc::clone(&runtime))
        .with_default_features()
        .build();

    let catalogs = Arc::new(ApiaryCatalogList::new(Arc::new(CatalogShared {
        registry,
        comb,
        crop: options.crop.clone(),
        scan_state,
    })));

    // Join policy goes right after DataFusion's join selection.
    let mut rules = PhysicalOptimizer::new().rules;
    let after_selection = rules
        .iter()
        .position(|r| r.name() == "join_selection")
        .map(|i| i + 1)
        .ok_or_else(|| ApiaryError::Internal {
            message: "DataFusion's join selection rule was not found".into(),
        })?;
    rules.insert(
        after_selection,
        Arc::new(FitJoinsToBee::new(
            options.memory_per_bee,
            options.target_partitions,
        )),
    );

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_catalog_list(catalogs)
        .with_physical_optimizer_rules(rules)
        .build();
    Ok(SessionContext::new_with_state(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_config(memory: u64, cores: usize) -> NodeConfig {
        let mut config = NodeConfig::detect("local://test");
        config.memory_bytes = memory;
        config.memory_per_bee = memory / cores as u64;
        config.cores = cores;
        config.cache_dir = PathBuf::from("/var/cache/apiary");
        config
    }

    #[test]
    fn node_options_leave_a_quarter_of_memory_free() {
        let options = QueryOptions::from_node(&node_config(4 * 1024 * 1024 * 1024, 4));
        assert_eq!(options.memory_pool_bytes, 3 * 1024 * 1024 * 1024);
        assert_eq!(options.target_partitions, 4);
        assert_eq!(options.memory_per_bee, 1024 * 1024 * 1024);
        assert_eq!(
            options.spill_dir,
            Some(PathBuf::from("/var/cache/apiary/spill"))
        );
    }

    #[tokio::test]
    async fn a_small_pool_still_sorts_and_spills() {
        // The reservation scales with the pool, so even a 64 MB pool running
        // four partitions can sort data larger than itself.
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;

        let dir = tempfile::tempdir().unwrap();
        let options = QueryOptions {
            memory_pool_bytes: 64 * 1024 * 1024,
            memory_per_bee: 16 * 1024 * 1024,
            target_partitions: 4,
            spill_dir: Some(dir.path().to_path_buf()),
            crop: None,
        };
        let registry_dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn apiary_core::StorageBackend> = Arc::new(
            apiary_comb::LocalBackend::new(registry_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let registry = Arc::new(RegistryManager::new(backend));
        let comb = Arc::new(Comb::from_local_path(registry_dir.path()).unwrap());
        let ctx = build_session(&options, registry, comb).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        // Many modest batches, as a Parquet scan produces: a sort can spill
        // between batches but never within one.
        let rows = 1_500_000i64;
        let per_batch = 25_000i64;
        let batches: Vec<RecordBatch> = (0..rows / per_batch)
            .map(|b| {
                let range = b * per_batch..(b + 1) * per_batch;
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(range.clone().rev())),
                        Arc::new(StringArray::from_iter_values(
                            range.map(|i| format!("a-reasonably-long-string-value-{i}")),
                        )),
                    ],
                )
                .unwrap()
            })
            .collect();

        // Sort every row by a string key, streaming the result so only the
        // sort itself counts against the pool.
        let df = ctx
            .read_table(Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()))
            .unwrap()
            .sort(vec![datafusion::prelude::col("v").sort(true, true)])
            .unwrap();
        let mut stream = df.execute_stream().await.unwrap();
        let mut sorted = 0usize;
        while let Some(batch) = futures::StreamExt::next(&mut stream).await {
            sorted += batch.unwrap().num_rows();
        }
        assert_eq!(sorted, rows as usize);
    }

    #[test]
    fn tiny_nodes_still_get_a_usable_pool() {
        let options = QueryOptions::from_node(&node_config(16 * 1024 * 1024, 1));
        assert_eq!(options.memory_pool_bytes, MIN_POOL_BYTES);
    }
}
