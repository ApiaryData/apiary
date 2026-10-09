//! DataFusion-based SQL query engine for Apiary.
//!
//! [`ApiaryQueryContext`] owns one long-lived DataFusion session per Node. The
//! session resolves `hive.box.frame` natively, through a catalogue backed by
//! the registry and the comb (see the `catalog` module), and scans each Frame's
//! Delta table lazily. All queries share one memory pool with a spill
//! directory, and hash joins that might not fit a Bee become sort-merge joins
//! ([`join_policy`]). Custom SQL commands (USE, SHOW, DESCRIBE) are
//! intercepted before they reach DataFusion.

mod catalog;
pub mod join_policy;
mod session;
mod staged;
pub mod timing;

pub use join_policy::FitJoinsToBee;
pub use session::QueryOptions;
pub use staged::{ROWS_FROM_COMB, ROWS_FROM_CROP, StageRows};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Column, TableReference};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{Expr, cast, lit};
use datafusion::prelude::SessionContext;

use apiary_comb::Comb;
use apiary_comb::{FrameStats, STAGE_COLUMN};
use apiary_core::Result;
use apiary_core::error::ApiaryError;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::types::NodeId;

use crate::catalog::{NO_BOX, NO_HIVE, TableIo, into_apiary_error, record_table_io};

/// The result of a query: its rows, and how many rows each stage gave it.
#[derive(Debug)]
pub struct QueryOutput {
    /// The result batches. For a query over Frames, each batch's schema metadata
    /// carries the stage counts under [`ROWS_FROM_CROP`] and [`ROWS_FROM_COMB`].
    pub batches: Vec<RecordBatch>,
    /// Rows the query read from the crop and from the comb.
    pub stages: StageRows,
    /// The result's schema, which is known even when there are no batches.
    pub schema: SchemaRef,
}

/// The hive and box chosen with `USE HIVE` and `USE BOX`.
#[derive(Clone, Debug, Default)]
struct Selection {
    hive: Option<String>,
    box_name: Option<String>,
}

/// The Apiary query context: a Node's long-lived DataFusion session with
/// Apiary namespace resolution.
///
/// Shared by every caller on the Node, so `sql` takes `&self`. The current
/// hive and box are shared too.
pub struct ApiaryQueryContext {
    session: SessionContext,
    comb: Arc<Comb>,
    registry: Arc<RegistryManager>,
    selection: Mutex<Selection>,
    #[allow(dead_code)] // Node identity, used once Nodes exchange work directly
    node_id: NodeId,
}

impl ApiaryQueryContext {
    /// Create a query context with default options (unbounded memory, one
    /// partition per core), for tests and tools.
    pub fn new(comb: Arc<Comb>, registry: Arc<RegistryManager>) -> Self {
        Self::with_options(
            comb,
            registry,
            NodeId::from("local"),
            QueryOptions::default(),
        )
        .expect("default query options are valid")
    }

    /// Create a query context for a Node: its memory pool, spill directory
    /// and Bee budget come from `options` (see [`QueryOptions::from_node`]).
    pub fn with_options(
        comb: Arc<Comb>,
        registry: Arc<RegistryManager>,
        node_id: NodeId,
        options: QueryOptions,
    ) -> Result<Self> {
        let session = session::build_session(&options, Arc::clone(&registry), Arc::clone(&comb))?;
        Ok(Self {
            session,
            comb,
            registry,
            selection: Mutex::new(Selection::default()),
            node_id,
        })
    }

    /// The Node's memory pool, which Bees take shares of.
    pub fn memory_pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.session.runtime_env().memory_pool)
    }

    fn selection(&self) -> Selection {
        self.selection.lock().expect("selection poisoned").clone()
    }

    fn select_hive(&self, name: String) {
        self.selection.lock().expect("selection poisoned").hive = Some(name);
    }

    fn select_box(&self, name: String) {
        self.selection.lock().expect("selection poisoned").box_name = Some(name);
    }

    /// A context for one query: the shared runtime and catalogue, with this
    /// moment's hive and box as the default namespace.
    fn query_session(&self, pool: Option<Arc<dyn MemoryPool>>) -> Result<SessionContext> {
        let selection = self.selection();
        let mut state = self.session.state();
        let catalog = &mut state.config_mut().options_mut().catalog;
        catalog.default_catalog = selection.hive.unwrap_or_else(|| NO_HIVE.to_string());
        catalog.default_schema = selection.box_name.unwrap_or_else(|| NO_BOX.to_string());
        if let Some(pool) = pool {
            // Run this query under the pool it was given (a Bee's share of the
            // Node's), keeping the Node's spill directory, caches and stores.
            let runtime = RuntimeEnvBuilder::from_runtime_env(state.runtime_env())
                .with_memory_pool(pool)
                .build_arc()
                .map_err(|e| ApiaryError::Internal {
                    message: format!("Failed to build the query's runtime: {e}"),
                })?;
            state = SessionStateBuilder::new_from_existing(state)
                .with_runtime_env(runtime)
                .build();
        }
        Ok(SessionContext::new_with_state(state))
    }

    /// Execute a SQL query and return results as RecordBatches.
    ///
    /// For a query over Frames, every batch carries the rows read from each
    /// stage in its schema metadata. Use [`sql_with_stages`](Self::sql_with_stages)
    /// to get the counts directly.
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.sql_with_stages(query)
            .await
            .map(|output| output.batches)
    }

    /// Execute a SQL query and report how many rows each stage gave it.
    pub async fn sql_with_stages(&self, query: &str) -> Result<QueryOutput> {
        self.sql_with_stages_in(query, None).await
    }

    /// Execute a SQL query under `pool` (a Bee's share of the Node's memory
    /// pool): operators that would take more than the share are refused, and
    /// spill.
    pub async fn sql_in(
        &self,
        query: &str,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<Vec<RecordBatch>> {
        self.sql_with_stages_in(query, pool)
            .await
            .map(|output| output.batches)
    }

    /// [`sql_with_stages`](Self::sql_with_stages) under `pool`.
    pub async fn sql_with_stages_in(
        &self,
        query: &str,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<QueryOutput> {
        let trimmed = query.trim();

        // Detect and block unsupported DML
        if let Some(err) = check_unsupported_dml(trimmed) {
            return Err(err);
        }

        // Handle custom commands
        if let Some(batches) = self.handle_custom_command(trimmed).await? {
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
            return Ok(QueryOutput {
                batches,
                stages: StageRows::default(),
                schema,
            });
        }

        // Standard SQL: resolve frame references, register tables, execute
        self.execute_standard_sql(trimmed, pool).await
    }

    /// Handle custom SQL commands (USE, SHOW, DESCRIBE).
    async fn handle_custom_command(&self, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
        let upper = sql.to_uppercase();
        let upper = upper.trim_end_matches(';').trim();

        // USE HIVE <name>
        if let Some(name) = upper.strip_prefix("USE HIVE ") {
            let name = name.trim().to_lowercase();
            // Verify hive exists
            let hives = self.registry.list_hives().await?;
            if !hives.iter().any(|h| h.to_lowercase() == name) {
                return Err(ApiaryError::EntityNotFound {
                    entity_type: "Hive".into(),
                    name: name.clone(),
                });
            }
            self.select_hive(name.clone());
            let batch = single_message_batch(&format!("Current hive set to '{name}'"));
            return Ok(Some(vec![batch]));
        }

        // USE BOX <name>
        if let Some(name) = upper.strip_prefix("USE BOX ") {
            let name = name.trim().to_lowercase();
            let hive = self.selection().hive.ok_or_else(|| ApiaryError::Config {
                message: "No hive selected. Run USE HIVE <name> first.".into(),
            })?;
            // Verify box exists
            let boxes = self.registry.list_boxes(&hive).await?;
            if !boxes.iter().any(|b| b.to_lowercase() == name) {
                return Err(ApiaryError::EntityNotFound {
                    entity_type: "Box".into(),
                    name: name.clone(),
                });
            }
            self.select_box(name.clone());
            let batch = single_message_batch(&format!("Current box set to '{name}'"));
            return Ok(Some(vec![batch]));
        }

        // SHOW HIVES
        if upper == "SHOW HIVES" {
            let hives = self.registry.list_hives().await?;
            let batch = string_list_batch("hive", &hives);
            return Ok(Some(vec![batch]));
        }

        // SHOW BOXES IN <hive>
        if let Some(rest) = upper.strip_prefix("SHOW BOXES IN ") {
            let hive = rest.trim().to_lowercase();
            let boxes = self.registry.list_boxes(&hive).await?;
            let batch = string_list_batch("box", &boxes);
            return Ok(Some(vec![batch]));
        }

        // SHOW BOXES (using current hive context)
        if upper == "SHOW BOXES" {
            let hive = self.selection().hive.ok_or_else(|| ApiaryError::Config {
                message:
                    "No hive selected. Run USE HIVE <name> first, or use SHOW BOXES IN <hive>."
                        .into(),
            })?;
            let boxes = self.registry.list_boxes(&hive).await?;
            let batch = string_list_batch("box", &boxes);
            return Ok(Some(vec![batch]));
        }

        // SHOW FRAMES IN <hive>.<box>
        if let Some(rest) = upper.strip_prefix("SHOW FRAMES IN ") {
            let parts: Vec<&str> = rest.trim().split('.').collect();
            if parts.len() != 2 {
                return Err(ApiaryError::Config {
                    message: "SHOW FRAMES IN requires hive.box format".into(),
                });
            }
            let hive = parts[0].trim().to_lowercase();
            let box_name = parts[1].trim().to_lowercase();
            let frames = self.registry.list_frames(&hive, &box_name).await?;
            let batch = string_list_batch("frame", &frames);
            return Ok(Some(vec![batch]));
        }

        // SHOW FRAMES (using current hive and box context)
        if upper == "SHOW FRAMES" {
            let selection = self.selection();
            let hive = selection.hive.ok_or_else(|| ApiaryError::Config {
                message: "No hive selected. Run USE HIVE <name> first, or use SHOW FRAMES IN <hive>.<box>.".into(),
            })?;
            let box_name = selection.box_name.ok_or_else(|| ApiaryError::Config {
                message:
                    "No box selected. Run USE BOX <name> first, or use SHOW FRAMES IN <hive>.<box>."
                        .into(),
            })?;
            let frames = self.registry.list_frames(&hive, &box_name).await?;
            let batch = string_list_batch("frame", &frames);
            return Ok(Some(vec![batch]));
        }

        // DESCRIBE <hive>.<box>.<frame>
        if let Some(rest) = upper.strip_prefix("DESCRIBE ") {
            let raw_rest = sql.trim_end_matches(';').trim();
            let raw_rest = &raw_rest[raw_rest.len() - rest.len()..];
            let parts: Vec<&str> = raw_rest.trim().split('.').collect();
            if parts.len() != 3 {
                return Err(ApiaryError::Config {
                    message: "DESCRIBE requires hive.box.frame format".into(),
                });
            }
            let hive = parts[0].trim();
            let box_name = parts[1].trim();
            let frame_name = parts[2].trim();
            return Ok(Some(vec![
                self.describe_frame(hive, box_name, frame_name).await?,
            ]));
        }

        Ok(None)
    }

    /// Produce a DESCRIBE result for a frame.
    async fn describe_frame(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
    ) -> Result<RecordBatch> {
        let frame = self.registry.get_frame(hive, box_name, frame_name).await?;
        // Cell count and total size come from the Delta log
        let stats = match self
            .comb
            .open_frame_table(hive, box_name, frame_name)
            .await?
        {
            Some(table) => self.comb.frame_stats(&table)?,
            None => FrameStats::default(),
        };
        let (cell_count, total_rows, total_bytes) = (stats.cells, stats.rows, stats.bytes);

        let schema_json = serde_json::to_string(&frame.schema).unwrap_or_else(|_| "{}".into());

        let schema = Arc::new(Schema::new(vec![
            Field::new("property", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let partition_str = if frame.partition_by.is_empty() {
            "(none)".to_string()
        } else {
            frame.partition_by.join(", ")
        };

        let properties = vec![
            "schema",
            "partition_by",
            "cells",
            "total_rows",
            "total_bytes",
        ];
        let values = vec![
            schema_json,
            partition_str,
            cell_count.to_string(),
            total_rows.to_string(),
            total_bytes.to_string(),
        ];

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    properties
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(values)),
            ],
        )
        .map_err(|e| ApiaryError::Internal {
            message: format!("Failed to create DESCRIBE result: {e}"),
        })
    }

    /// Execute standard SQL on the Node's session. DataFusion resolves
    /// `hive.box.frame` (or a shorter name, after USE HIVE / USE BOX) through
    /// the catalogue and scans each Frame's Delta table lazily.
    async fn execute_standard_sql(
        &self,
        sql: &str,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<QueryOutput> {
        let mut timings = timing::QueryTimings::begin_from_sql(sql);

        // --- parse phase ---
        let parse_start = timings.as_ref().map(|t| t.start_phase());
        let session = self.query_session(pool)?;
        let state = session.state();
        let dialect = state.config_options().sql_parser.dialect;
        let statement = state
            .sql_to_statement(sql, &dialect)
            .map_err(|e| into_apiary_error(e, "SQL parse error"))?;
        if let (Some(t), Some(s)) = (timings.as_mut(), parse_start) {
            t.end_phase("parse", s);
        }

        // --- plan phase: resolve names (this opens each table) and build the
        // logical plan. Table-open time is reported as its own phases.
        let plan_start = std::time::Instant::now();
        let (plan, io) =
            record_table_io(TableIo::default(), state.statement_to_plan(statement)).await;
        let plan = plan.map_err(|e| into_apiary_error(e, "DataFusion query error"))?;
        let file_discovery = io.file_discovery.get();
        let metadata_read = io.metadata_read.get();
        let plan_only = plan_start
            .elapsed()
            .saturating_sub(file_discovery + metadata_read);

        if let Some(t) = timings.as_mut() {
            t.add_accumulated_phase("plan", plan_only);
            t.add_accumulated_phase("file_discovery", file_discovery);
            t.add_accumulated_phase("metadata_read", metadata_read);
            // Scans are lazy: reading data is part of the execute phase.
            t.add_accumulated_phase("data_read", std::time::Duration::ZERO);
        }

        // --- execute phase (DataFusion planning + execution) ---
        let exec_start = timings.as_ref().map(|t| t.start_phase());
        let df = session
            .execute_logical_plan(plan)
            .await
            .map_err(|e| into_apiary_error(e, "DataFusion query error"))?;
        let task_ctx = Arc::new(df.task_ctx());
        let physical = df
            .create_physical_plan()
            .await
            .map_err(|e| into_apiary_error(e, "DataFusion query error"))?;
        let results = datafusion::physical_plan::collect(Arc::clone(&physical), task_ctx)
            .await
            .map_err(|e| into_apiary_error(e, "DataFusion execution error"))?;
        let stages = staged::stage_rows(&physical);
        if let (Some(t), Some(s)) = (timings.as_mut(), exec_start) {
            t.end_phase("execute", s);
        }

        if let Some(t) = timings {
            t.finish();
        }

        let schema = staged::schema_with_stage_metadata(physical.schema(), stages);
        Ok(QueryOutput {
            batches: staged::with_stage_metadata(results, stages),
            stages,
            schema,
        })
    }

    /// Read a Frame into one batch, optionally keeping only rows whose
    /// partition columns equal the given values. Rows still in the crop are
    /// included; the `_stage` column is not. `None` if no rows match.
    pub async fn read_frame(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
        partition_filter: Option<&HashMap<String, String>>,
    ) -> Result<Option<RecordBatch>> {
        let session = self.query_session(None)?;
        let to_error = |e| into_apiary_error(e, "Failed to read frame");
        let mut df = session
            .table(TableReference::full(hive, box_name, frame))
            .await
            .map_err(to_error)?;

        if let Some(filter) = partition_filter {
            for (column, value) in filter {
                // Filter values arrive as strings, whatever the column type.
                let column = Expr::Column(Column::new_unqualified(column));
                df = df
                    .filter(cast(column, DataType::Utf8).eq(lit(value.as_str())))
                    .map_err(to_error)?;
            }
        }

        let df = df.drop_columns(&[STAGE_COLUMN]).map_err(to_error)?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await.map_err(to_error)?;
        if batches.iter().map(RecordBatch::num_rows).sum::<usize>() == 0 {
            return Ok(None);
        }
        arrow::compute::concat_batches(&schema, &batches)
            .map(Some)
            .map_err(|e| ApiaryError::Internal {
                message: format!("Failed to merge result batches: {e}"),
            })
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Check for unsupported DML and return an error if detected.
fn check_unsupported_dml(sql: &str) -> Option<ApiaryError> {
    let upper = sql.to_uppercase();
    let first_word = upper.split_whitespace().next().unwrap_or("");

    match first_word {
        "DELETE" => Some(ApiaryError::Unsupported {
            message: "DELETE is not supported. Apiary uses append-only writes. Use overwrite_frame() to replace all data in a frame.".into(),
        }),
        "UPDATE" => Some(ApiaryError::Unsupported {
            message: "UPDATE is not supported. Apiary uses append-only writes. Rewrite the frame with corrected data using overwrite_frame().".into(),
        }),
        "INSERT" => Some(ApiaryError::Unsupported {
            message: "INSERT is not supported via SQL. Use write_to_frame() to add data.".into(),
        }),
        "DROP" => Some(ApiaryError::Unsupported {
            message: "DROP is not supported via SQL. Use the registry API for DDL operations.".into(),
        }),
        "CREATE" => Some(ApiaryError::Unsupported {
            message: "CREATE is not supported via SQL. Use create_frame() for DDL operations.".into(),
        }),
        "ALTER" => Some(ApiaryError::Unsupported {
            message: "ALTER is not supported via SQL. Use the registry API for DDL operations.".into(),
        }),
        "COPY" => Some(ApiaryError::Unsupported {
            message: "COPY is not supported via SQL. Query results are returned to the caller.".into(),
        }),
        "SET" | "RESET" => Some(ApiaryError::Unsupported {
            message: "SET and RESET are not supported via SQL. The Node configures its own query session.".into(),
        }),
        _ => None,
    }
}

/// Create a single-row batch with a message.
fn single_message_batch(message: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "message",
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![message.to_string()]))],
    )
    .unwrap()
}

/// Create a batch from a list of strings.
fn string_list_batch(column_name: &str, values: &[String]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(
            values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ))],
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use apiary_comb::local::LocalBackend;
    use apiary_comb::{CellState, Comb};
    use apiary_core::{FieldDef, FrameSchema, StorageBackend};
    use arrow::array::{Float64Array, Int64Array};

    async fn make_test_env() -> (Arc<Comb>, Arc<RegistryManager>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_path_buf()).await.unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(backend);
        let registry = Arc::new(RegistryManager::new(Arc::clone(&storage)));
        let _ = registry.load_or_create().await.unwrap();
        let comb = Arc::new(Comb::from_local_path(dir.path()).unwrap());
        (comb, registry, dir)
    }

    fn test_schema() -> serde_json::Value {
        serde_json::json!({
            "region": "string",
            "temp": "float64",
            "humidity": "int64"
        })
    }

    async fn setup_frame(comb: &Arc<Comb>, registry: &Arc<RegistryManager>) {
        registry.create_hive("test_hive").await.unwrap();
        registry.create_box("test_hive", "test_box").await.unwrap();
        registry
            .create_frame(
                "test_hive",
                "test_box",
                "sensors",
                test_schema(),
                vec!["region".into()],
            )
            .await
            .unwrap();

        // Create ledger and write data
        let frame_schema = FrameSchema {
            fields: vec![
                FieldDef {
                    name: "region".into(),
                    data_type: "string".into(),
                    nullable: false,
                },
                FieldDef {
                    name: "temp".into(),
                    data_type: "float64".into(),
                    nullable: true,
                },
                FieldDef {
                    name: "humidity".into(),
                    data_type: "int64".into(),
                    nullable: true,
                },
            ],
        };

        let table = comb
            .create_frame_table(
                "test_hive",
                "test_box",
                "sensors",
                &frame_schema,
                &["region".to_string()],
            )
            .await
            .unwrap();

        // Write test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, true),
            Field::new("humidity", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["north", "north", "south", "south"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
                Arc::new(Int64Array::from(vec![50, 60, 70, 80])),
            ],
        )
        .unwrap();

        comb.append(&table, &batch, 256 * 1024 * 1024, CellState::Nectar)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_select_all() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx
            .sql("SELECT * FROM test_hive.test_box.sensors")
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn test_aggregation() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx
            .sql("SELECT region, AVG(temp) as avg_temp FROM test_hive.test_box.sensors GROUP BY region ORDER BY region")
            .await
            .unwrap();

        assert!(!results.is_empty());
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // north, south
    }

    #[tokio::test]
    async fn test_use_hive_and_box() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        ctx.sql("USE HIVE test_hive").await.unwrap();
        ctx.sql("USE BOX test_box").await.unwrap();
        let results = ctx.sql("SELECT * FROM sensors").await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn test_show_hives() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx.sql("SHOW HIVES").await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_show_frames() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx.sql("SHOW FRAMES IN test_hive.test_box").await.unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].num_rows() >= 1);
    }

    #[tokio::test]
    async fn test_describe() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx
            .sql("DESCRIBE test_hive.test_box.sensors")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].num_rows() >= 3);
    }

    #[tokio::test]
    async fn test_delete_blocked() {
        let (comb, registry, _dir) = make_test_env().await;
        let ctx = ApiaryQueryContext::new(comb, registry);

        let result = ctx.sql("DELETE FROM test_hive.test_box.sensors").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not supported"));
    }

    #[tokio::test]
    async fn test_update_blocked() {
        let (comb, registry, _dir) = make_test_env().await;
        let ctx = ApiaryQueryContext::new(comb, registry);

        let result = ctx
            .sql("UPDATE test_hive.test_box.sensors SET temp = 0")
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not supported"));
    }

    #[tokio::test]
    async fn test_where_filter() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx
            .sql("SELECT * FROM test_hive.test_box.sensors WHERE region = 'north'")
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_projection() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx
            .sql("SELECT temp FROM test_hive.test_box.sensors")
            .await
            .unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].num_columns(), 1);
        assert_eq!(results[0].schema().field(0).name(), "temp");
    }

    #[test]
    fn test_check_unsupported_dml() {
        assert!(check_unsupported_dml("DELETE FROM t").is_some());
        assert!(check_unsupported_dml("UPDATE t SET x = 1").is_some());
        assert!(check_unsupported_dml("SELECT * FROM t").is_none());
    }

    #[tokio::test]
    async fn test_show_boxes_without_qualifier() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        ctx.sql("USE HIVE test_hive").await.unwrap();

        let results = ctx.sql("SHOW BOXES").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].num_rows() >= 1);
        assert_eq!(results[0].schema().field(0).name(), "box");
    }

    #[tokio::test]
    async fn test_show_frames_without_qualifier() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let ctx = ApiaryQueryContext::new(comb, registry);
        ctx.sql("USE HIVE test_hive").await.unwrap();
        ctx.sql("USE BOX test_box").await.unwrap();

        let results = ctx.sql("SHOW FRAMES").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].num_rows() >= 1);
        assert_eq!(results[0].schema().field(0).name(), "frame");
    }
}

#[cfg(test)]
mod catalog_tests;

#[cfg(test)]
mod staged_tests;
