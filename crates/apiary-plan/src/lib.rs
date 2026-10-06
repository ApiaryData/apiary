//! DataFusion-based SQL query engine for Apiary.
//!
//! [`ApiaryQueryContext`] wraps a DataFusion `SessionContext` and resolves
//! Apiary frame references (hive.box.frame) to in-memory tables built from
//! the frame's active Parquet cells.  Custom SQL commands (USE, SHOW,
//! DESCRIBE) are intercepted before they reach DataFusion.

pub mod timing;

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tracing::info;

use apiary_comb::Comb;
use apiary_comb::FrameStats;
use apiary_comb::schema::delta_schema;
use apiary_core::Result;
use apiary_core::error::ApiaryError;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::types::NodeId;

/// The Apiary query context — wraps DataFusion with Apiary namespace resolution.
pub struct ApiaryQueryContext {
    comb: Arc<Comb>,
    registry: Arc<RegistryManager>,
    current_hive: Option<String>,
    current_box: Option<String>,
    #[allow(dead_code)] // Node identity, used once Nodes exchange work directly
    node_id: NodeId,
}

impl ApiaryQueryContext {
    /// Create a new query context.
    pub fn new(comb: Arc<Comb>, registry: Arc<RegistryManager>) -> Self {
        Self::with_node_id(comb, registry, NodeId::from("local"))
    }

    /// Create a new query context with a specific node ID.
    pub fn with_node_id(comb: Arc<Comb>, registry: Arc<RegistryManager>, node_id: NodeId) -> Self {
        Self {
            comb,
            registry,
            current_hive: None,
            current_box: None,
            node_id,
        }
    }

    /// Execute a SQL query and return results as RecordBatches.
    pub async fn sql(&mut self, query: &str) -> Result<Vec<RecordBatch>> {
        let trimmed = query.trim();

        // Detect and block unsupported DML
        if let Some(err) = check_unsupported_dml(trimmed) {
            return Err(err);
        }

        // Handle custom commands
        if let Some(result) = self.handle_custom_command(trimmed).await? {
            return Ok(result);
        }

        // Standard SQL: resolve frame references, register tables, execute
        self.execute_standard_sql(trimmed).await
    }

    /// Handle custom SQL commands (USE, SHOW, DESCRIBE).
    async fn handle_custom_command(&mut self, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
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
            self.current_hive = Some(name.clone());
            let batch = single_message_batch(&format!("Current hive set to '{name}'"));
            return Ok(Some(vec![batch]));
        }

        // USE BOX <name>
        if let Some(name) = upper.strip_prefix("USE BOX ") {
            let name = name.trim().to_lowercase();
            let hive = self
                .current_hive
                .as_ref()
                .ok_or_else(|| ApiaryError::Config {
                    message: "No hive selected. Run USE HIVE <name> first.".into(),
                })?;
            // Verify box exists
            let boxes = self.registry.list_boxes(hive).await?;
            if !boxes.iter().any(|b| b.to_lowercase() == name) {
                return Err(ApiaryError::EntityNotFound {
                    entity_type: "Box".into(),
                    name: name.clone(),
                });
            }
            self.current_box = Some(name.clone());
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
            let hive = self
                .current_hive
                .as_ref()
                .ok_or_else(|| ApiaryError::Config {
                    message:
                        "No hive selected. Run USE HIVE <name> first, or use SHOW BOXES IN <hive>."
                            .into(),
                })?;
            let boxes = self.registry.list_boxes(hive).await?;
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
            let hive = self.current_hive.as_ref().ok_or_else(|| ApiaryError::Config {
                message: "No hive selected. Run USE HIVE <name> first, or use SHOW FRAMES IN <hive>.<box>.".into(),
            })?;
            let box_name = self.current_box.as_ref().ok_or_else(|| ApiaryError::Config {
                message: "No box selected. Run USE BOX <name> first, or use SHOW FRAMES IN <hive>.<box>.".into(),
            })?;
            let frames = self.registry.list_frames(hive, box_name).await?;
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

    /// Execute standard SQL by resolving frame references and delegating to DataFusion.
    async fn execute_standard_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut timings = timing::QueryTimings::begin_from_sql(sql);

        // --- query_parse phase ---
        let parse_start = timings.as_ref().map(|t| t.start_phase());

        // Extract table references from SQL
        let table_refs = extract_table_references(sql);

        if table_refs.is_empty() {
            return Err(ApiaryError::Config {
                message: "No table references found in query".into(),
            });
        }

        if let (Some(t), Some(s)) = (timings.as_mut(), parse_start) {
            t.end_phase("parse", s);
        }

        // --- query_plan phase ---
        let plan_start = timings.as_ref().map(|t| t.start_phase());

        // Create a fresh session for this query (avoids stale table registrations)
        let session = apiary_comb::query_session();

        if let (Some(t), Some(s)) = (timings.as_mut(), plan_start) {
            t.end_phase("plan", s);
        }

        // Resolve and register each table. Tables are scanned lazily: DataFusion
        // prunes partitions and skips files from Delta statistics, so the time
        // to read data is part of the execute phase.
        let mut file_discovery_total = std::time::Duration::ZERO;
        let mut metadata_read_total = std::time::Duration::ZERO;

        for table_ref in &table_refs {
            let (hive, box_name, frame_name, register_name) = self.resolve_table_ref(table_ref)?;

            // --- file_discovery phase (open the Delta table: read its log) ---
            let fd_start = timings.as_ref().map(|t| t.start_phase());
            let table = self
                .comb
                .open_frame_table(&hive, &box_name, &frame_name)
                .await?;
            if let Some(s) = fd_start {
                file_discovery_total += s.elapsed();
            }

            // --- metadata_read phase (build the scan: file index and statistics) ---
            let mr_start = timings.as_ref().map(|t| t.start_phase());
            match table {
                Some(table) => {
                    self.comb
                        .register_table(&session, &register_name, &table)
                        .await?;
                    info!(
                        frame = %format!("{hive}/{box_name}/{frame_name}"),
                        version = ?table.version(),
                        "Frame table registered"
                    );
                }
                None => {
                    // A registered frame that has never been written is empty,
                    // with the schema it was created with.
                    let frame = self
                        .registry
                        .get_frame(&hive, &box_name, &frame_name)
                        .await?;
                    let schema =
                        delta_schema(&apiary_core::FrameSchema::from_json_value(&frame.schema)?);
                    let empty_batch = RecordBatch::new_empty(schema);
                    let mem_table = datafusion::datasource::MemTable::try_new(
                        empty_batch.schema(),
                        vec![vec![empty_batch]],
                    )
                    .map_err(|e| ApiaryError::Internal {
                        message: format!("Failed to create empty MemTable: {e}"),
                    })?;
                    session
                        .register_table(&register_name, Arc::new(mem_table))
                        .map_err(|e| ApiaryError::Internal {
                            message: format!("Failed to register table: {e}"),
                        })?;
                }
            }
            if let Some(s) = mr_start {
                metadata_read_total += s.elapsed();
            }
        }

        // Record accumulated I/O phase timings
        if let Some(t) = timings.as_mut() {
            t.add_accumulated_phase("file_discovery", file_discovery_total);
            t.add_accumulated_phase("metadata_read", metadata_read_total);
            t.add_accumulated_phase("data_read", std::time::Duration::ZERO);
        }

        // Rewrite the SQL to use the registered table names
        let rewritten =
            rewrite_sql_table_refs(sql, &table_refs, &self.current_hive, &self.current_box);

        // --- query_execute phase (DataFusion planning + execution) ---
        let exec_start = timings.as_ref().map(|t| t.start_phase());

        let df = session
            .sql(&rewritten)
            .await
            .map_err(|e| ApiaryError::Internal {
                message: format!("DataFusion query error: {e}"),
            })?;

        let results = df.collect().await.map_err(|e| ApiaryError::Internal {
            message: format!("DataFusion execution error: {e}"),
        })?;

        if let (Some(t), Some(s)) = (timings.as_mut(), exec_start) {
            t.end_phase("execute", s);
        }

        if let Some(t) = timings {
            t.finish();
        }

        Ok(results)
    }

    /// Resolve a table reference to (hive, box, frame, register_name).
    fn resolve_table_ref(&self, table_ref: &str) -> Result<(String, String, String, String)> {
        let parts: Vec<&str> = table_ref.split('.').collect();

        match parts.len() {
            3 => {
                let hive = parts[0].to_string();
                let box_name = parts[1].to_string();
                let frame_name = parts[2].to_string();
                // Register with just the frame name to simplify SQL rewriting
                let register_name = frame_name.clone();
                Ok((hive, box_name, frame_name, register_name))
            }
            2 => {
                let hive = self.current_hive.as_ref().ok_or_else(|| {
                    ApiaryError::Resolution {
                        path: table_ref.into(),
                        reason: "No hive selected. Use 3-part name (hive.box.frame) or run USE HIVE first.".into(),
                    }
                })?;
                let box_name = parts[0].to_string();
                let frame_name = parts[1].to_string();
                let register_name = frame_name.clone();
                Ok((hive.clone(), box_name, frame_name, register_name))
            }
            1 => {
                let hive = self
                    .current_hive
                    .as_ref()
                    .ok_or_else(|| ApiaryError::Resolution {
                        path: table_ref.into(),
                        reason: "No hive selected. Use 3-part name or run USE HIVE first.".into(),
                    })?;
                let box_name =
                    self.current_box
                        .as_ref()
                        .ok_or_else(|| ApiaryError::Resolution {
                            path: table_ref.into(),
                            reason: "No box selected. Use 3-part name or run USE BOX first.".into(),
                        })?;
                let frame_name = parts[0].to_string();
                let register_name = frame_name.clone();
                Ok((hive.clone(), box_name.clone(), frame_name, register_name))
            }
            _ => Err(ApiaryError::Resolution {
                path: table_ref.into(),
                reason: "Invalid table reference. Use hive.box.frame format.".into(),
            }),
        }
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
        _ => None,
    }
}

/// Extract table references from SQL.
///
/// Finds patterns like `FROM table` and `JOIN table`, where table can be
/// `hive.box.frame`, `box.frame`, or `frame`.
fn extract_table_references(sql: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    for i in 0..tokens.len() {
        let upper = tokens[i].to_uppercase();
        if (upper == "FROM" || upper == "JOIN") && i + 1 < tokens.len() {
            let table_name = tokens[i + 1]
                .trim_end_matches(',')
                .trim_end_matches(')')
                .trim_end_matches(';');
            // Skip subqueries
            if table_name.starts_with('(') || table_name.is_empty() {
                continue;
            }
            // Skip SQL keywords that follow FROM (e.g., FROM (SELECT ...))
            let table_upper = table_name.to_uppercase();
            if matches!(
                table_upper.as_str(),
                "SELECT" | "WHERE" | "GROUP" | "ORDER" | "LIMIT" | "HAVING"
            ) {
                continue;
            }
            if !refs.contains(&table_name.to_string()) {
                refs.push(table_name.to_string());
            }
        }
    }

    refs
}

/// Rewrite SQL to replace 3-part or 2-part table references with the registered names.
fn rewrite_sql_table_refs(
    sql: &str,
    table_refs: &[String],
    _current_hive: &Option<String>,
    _current_box: &Option<String>,
) -> String {
    let mut result = sql.to_string();

    for table_ref in table_refs {
        let parts: Vec<&str> = table_ref.split('.').collect();
        let register_name = parts.last().unwrap_or(&table_ref.as_str()).to_string();
        if parts.len() > 1 {
            // Replace full reference with just the frame name
            result = result.replace(table_ref, &register_name);
        }
    }

    result
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

        let mut ctx = ApiaryQueryContext::new(comb, registry);
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

        let mut ctx = ApiaryQueryContext::new(comb, registry);
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

        let mut ctx = ApiaryQueryContext::new(comb, registry);
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

        let mut ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx.sql("SHOW HIVES").await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_show_frames() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let mut ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx.sql("SHOW FRAMES IN test_hive.test_box").await.unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].num_rows() >= 1);
    }

    #[tokio::test]
    async fn test_describe() {
        let (comb, registry, _dir) = make_test_env().await;
        setup_frame(&comb, &registry).await;

        let mut ctx = ApiaryQueryContext::new(comb, registry);
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
        let mut ctx = ApiaryQueryContext::new(comb, registry);

        let result = ctx.sql("DELETE FROM test_hive.test_box.sensors").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not supported"));
    }

    #[tokio::test]
    async fn test_update_blocked() {
        let (comb, registry, _dir) = make_test_env().await;
        let mut ctx = ApiaryQueryContext::new(comb, registry);

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

        let mut ctx = ApiaryQueryContext::new(comb, registry);
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

        let mut ctx = ApiaryQueryContext::new(comb, registry);
        let results = ctx
            .sql("SELECT temp FROM test_hive.test_box.sensors")
            .await
            .unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].num_columns(), 1);
        assert_eq!(results[0].schema().field(0).name(), "temp");
    }

    #[test]
    fn test_extract_table_references() {
        let refs = extract_table_references("SELECT * FROM hive.box.frame WHERE x = 1");
        assert_eq!(refs, vec!["hive.box.frame"]);

        let refs = extract_table_references("SELECT * FROM frame1 JOIN frame2 ON x = y");
        assert_eq!(refs, vec!["frame1", "frame2"]);
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

        let mut ctx = ApiaryQueryContext::new(comb, registry);
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

        let mut ctx = ApiaryQueryContext::new(comb, registry);
        ctx.sql("USE HIVE test_hive").await.unwrap();
        ctx.sql("USE BOX test_box").await.unwrap();

        let results = ctx.sql("SHOW FRAMES").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].num_rows() >= 1);
        assert_eq!(results[0].schema().field(0).name(), "frame");
    }
}
