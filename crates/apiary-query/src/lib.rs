//! DataFusion-based SQL query engine for Apiary.
//!
//! [`ApiaryQueryContext`] wraps a DataFusion `SessionContext` and resolves
//! Apiary frame references (hive.box.frame) to in-memory tables built from
//! the frame's active Parquet cells.  Custom SQL commands (USE, SHOW,
//! DESCRIBE) are intercepted before they reach DataFusion.

pub mod distributed;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use tracing::{info, warn};

use apiary_core::error::ApiaryError;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::storage::StorageBackend;
use apiary_core::types::NodeId;
use apiary_core::Result;
use apiary_storage::cell_reader::CellReader;
use apiary_storage::ledger::Ledger;

/// The Apiary query context — wraps DataFusion with Apiary namespace resolution.
pub struct ApiaryQueryContext {
    storage: Arc<dyn StorageBackend>,
    registry: Arc<RegistryManager>,
    current_hive: Option<String>,
    current_box: Option<String>,
    #[allow(dead_code)] // Will be used for distributed execution
    node_id: NodeId,
}

impl ApiaryQueryContext {
    /// Create a new query context.
    pub fn new(storage: Arc<dyn StorageBackend>, registry: Arc<RegistryManager>) -> Self {
        Self::with_node_id(storage, registry, NodeId::from("local"))
    }
    
    /// Create a new query context with a specific node ID.
    pub fn with_node_id(
        storage: Arc<dyn StorageBackend>,
        registry: Arc<RegistryManager>,
        node_id: NodeId,
    ) -> Self {
        Self {
            storage,
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
    async fn handle_custom_command(
        &mut self,
        sql: &str,
    ) -> Result<Option<Vec<RecordBatch>>> {
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
            let hive = self.current_hive.as_ref().ok_or_else(|| ApiaryError::Config {
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
            return Ok(Some(vec![self.describe_frame(hive, box_name, frame_name).await?]));
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
        let frame_path = format!("{hive}/{box_name}/{frame_name}");

        // Get cell count and total size from ledger
        let (cell_count, total_rows, total_bytes) =
            match Ledger::open(Arc::clone(&self.storage), &frame_path).await {
                Ok(ledger) => {
                    let cells = ledger.active_cells();
                    let rows: u64 = cells.iter().map(|c| c.rows).sum();
                    let bytes: u64 = cells.iter().map(|c| c.bytes).sum();
                    (cells.len() as u64, rows, bytes)
                }
                Err(_) => (0, 0, 0),
            };

        let schema_json = serde_json::to_string(&frame.schema)
            .unwrap_or_else(|_| "{}".into());

        let schema = Arc::new(Schema::new(vec![
            Field::new("property", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let partition_str = if frame.partition_by.is_empty() {
            "(none)".to_string()
        } else {
            frame.partition_by.join(", ")
        };

        let properties = vec!["schema", "partition_by", "cells", "total_rows", "total_bytes"];
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
                    properties.into_iter().map(|s| s.to_string()).collect::<Vec<_>>(),
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
        // Extract table references from SQL
        let table_refs = extract_table_references(sql);

        if table_refs.is_empty() {
            return Err(ApiaryError::Config {
                message: "No table references found in query".into(),
            });
        }

        // Extract simple WHERE predicates for pruning
        let predicates = extract_where_predicates(sql);

        // Create a fresh session for this query (avoids stale table registrations)
        let session = SessionContext::new();

        // Resolve and register each table
        for table_ref in &table_refs {
            let (hive, box_name, frame_name, register_name) =
                self.resolve_table_ref(table_ref)?;

            // Open ledger and prune cells
            let frame_path = format!("{hive}/{box_name}/{frame_name}");
            let ledger = Ledger::open(Arc::clone(&self.storage), &frame_path).await?;

            // Build partition and stat filters from predicates
            let partition_by: Vec<String> = ledger.partition_by().to_vec();
            let (partition_filters, stat_filters) =
                build_filters(&predicates, &partition_by);

            let cells = if partition_filters.is_empty() && stat_filters.is_empty() {
                ledger.active_cells().iter().collect::<Vec<_>>()
            } else {
                ledger.prune_cells(&partition_filters, &stat_filters)
            };

            info!(
                frame = %frame_path,
                total_cells = ledger.active_cells().len(),
                surviving_cells = cells.len(),
                "Cell pruning complete"
            );

            if cells.is_empty() {
                // Register an empty table with the correct schema
                let arrow_schema = frame_schema_to_arrow(ledger.schema())?;
                let empty_batch =
                    RecordBatch::new_empty(Arc::new(arrow_schema));
                let mem_table =
                    datafusion::datasource::MemTable::try_new(
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
                continue;
            }

            // Read surviving cells
            let reader = CellReader::new(Arc::clone(&self.storage), frame_path);
            let merged = reader.read_cells_merged(&cells, None).await?;

            let batches = match merged {
                Some(batch) => vec![vec![batch]],
                None => {
                    let arrow_schema = frame_schema_to_arrow(ledger.schema())?;
                    vec![vec![RecordBatch::new_empty(Arc::new(arrow_schema))]]
                }
            };

            let schema = batches[0][0].schema();
            let mem_table =
                datafusion::datasource::MemTable::try_new(schema, batches).map_err(|e| {
                    ApiaryError::Internal {
                        message: format!("Failed to create MemTable: {e}"),
                    }
                })?;

            session
                .register_table(&register_name, Arc::new(mem_table))
                .map_err(|e| ApiaryError::Internal {
                    message: format!("Failed to register table '{register_name}': {e}"),
                })?;
        }

        // Rewrite the SQL to use the registered table names
        let rewritten = rewrite_sql_table_refs(sql, &table_refs, &self.current_hive, &self.current_box);

        // Execute via DataFusion
        let df = session.sql(&rewritten).await.map_err(|e| ApiaryError::Internal {
            message: format!("DataFusion query error: {e}"),
        })?;

        df.collect().await.map_err(|e| ApiaryError::Internal {
            message: format!("DataFusion execution error: {e}"),
        })
    }

    /// Resolve a table reference to (hive, box, frame, register_name).
    fn resolve_table_ref(
        &self,
        table_ref: &str,
    ) -> Result<(String, String, String, String)> {
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
                let hive = self.current_hive.as_ref().ok_or_else(|| {
                    ApiaryError::Resolution {
                        path: table_ref.into(),
                        reason: "No hive selected. Use 3-part name or run USE HIVE first.".into(),
                    }
                })?;
                let box_name = self.current_box.as_ref().ok_or_else(|| {
                    ApiaryError::Resolution {
                        path: table_ref.into(),
                        reason: "No box selected. Use 3-part name or run USE BOX first.".into(),
                    }
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
    
    /// Execute a query using distributed execution (stub for Step 7).
    /// 
    /// For v1, this is a simplified implementation that:
    /// - Creates tasks from cell assignments
    /// - Generates SQL fragments (simple pass-through)
    /// - Writes the query manifest
    /// - Executes local tasks
    /// - Polls for partial results from other nodes
    /// - Merges results (simple concatenation)
    /// - Cleans up query files
    pub async fn execute_distributed(
        &self,
        sql: &str,
        assignments: HashMap<NodeId, Vec<distributed::CellInfo>>,
    ) -> Result<Vec<RecordBatch>> {
        use distributed::*;
        
        // 1. Create tasks from assignments
        let mut tasks = Vec::new();
        for (node_id, cells) in &assignments {
            let task_id = format!("{}_{}", node_id.as_str(), uuid::Uuid::new_v4());
            let cell_keys: Vec<String> = cells.iter()
                .map(|c| c.storage_key.clone())
                .collect();
            
            tasks.push(PlannedTask {
                task_id,
                node_id: node_id.clone(),
                cells: cell_keys,
                sql_fragment: sql.to_string(), // For v1, use original SQL
            });
        }
        
        // 2. Create and write manifest
        let manifest = create_manifest(sql, tasks.clone(), None, 60);
        write_manifest(&self.storage, &manifest).await?;
        
        info!(
            query_id = %manifest.query_id,
            tasks = tasks.len(),
            "Distributed query manifest written"
        );
        
        // 3. Execute local tasks
        let local_tasks: Vec<_> = tasks.iter()
            .filter(|t| t.node_id == self.node_id)
            .collect();
        
        let mut local_results = Vec::new();
        for task in local_tasks {
            match self.execute_task(&task.sql_fragment, &task.cells).await {
                Ok(batches) => {
                    if !batches.is_empty() {
                        local_results.extend(batches);
                    }
                }
                Err(e) => {
                    warn!(task_id = %task.task_id, error = %e, "Local task failed");
                }
            }
        }
        
        // Write local partial result
        if !local_results.is_empty() {
            write_partial_result(
                &self.storage,
                &manifest.query_id,
                &self.node_id,
                &local_results,
            ).await?;
        }
        
        // 4. Poll for partial results from other nodes
        let remote_nodes: Vec<_> = tasks.iter()
            .filter(|t| t.node_id != self.node_id)
            .map(|t| t.node_id.clone())
            .collect();
        
        let timeout = std::time::Duration::from_secs(manifest.timeout_secs);
        let start = std::time::Instant::now();
        let mut collected_results = local_results;
        
        for remote_node in &remote_nodes {
            let deadline = timeout.saturating_sub(start.elapsed());
            if start.elapsed() >= timeout {
                warn!(query_id = %manifest.query_id, "Query timeout reached");
                break;
            }
            
            // Poll for partial result
            let poll_interval = std::time::Duration::from_millis(500);
            let mut attempts = 0;
            let max_attempts = (deadline.as_millis() / poll_interval.as_millis()) as usize;
            
            while attempts < max_attempts {
                match read_partial_result(&self.storage, &manifest.query_id, remote_node).await {
                    Ok(batches) => {
                        info!(
                            query_id = %manifest.query_id,
                            node_id = %remote_node,
                            "Partial result received"
                        );
                        collected_results.extend(batches);
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                        attempts += 1;
                    }
                }
            }
        }
        
        // 5. Cleanup
        if let Err(e) = cleanup_query(&self.storage, &manifest.query_id).await {
            warn!(query_id = %manifest.query_id, error = %e, "Failed to cleanup query");
        }
        
        Ok(collected_results)
    }
    
    /// Execute a task on a specific set of cells.
    /// 
    /// For v1, this executes the SQL normally without filtering to specific cells.
    /// In a future version, this should register only the specified cells as tables.
    #[allow(unused_variables)]
    pub async fn execute_task(&self, sql: &str, cell_keys: &[String]) -> Result<Vec<RecordBatch>> {
        // For v1, we execute the SQL and filter cells
        // This is a simplified implementation - in reality, we'd need to
        // register only the specified cells as tables
        
        // For now, just execute the SQL normally
        self.execute_standard_sql(sql).await
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

/// A simple WHERE predicate extracted from SQL.
#[derive(Debug, Clone)]
struct Predicate {
    column: String,
    op: PredicateOp,
    value: String,
}

#[derive(Debug, Clone)]
enum PredicateOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
}

/// Extract simple WHERE predicates from SQL for pruning.
///
/// Handles patterns like:
/// - `column = 'value'` or `column = value`
/// - `column > N`
/// - `column < N`
/// - `column >= N`
/// - `column <= N`
fn extract_where_predicates(sql: &str) -> Vec<Predicate> {
    let mut predicates = Vec::new();

    // Find WHERE clause
    let upper = sql.to_uppercase();
    let where_pos = match upper.find(" WHERE ") {
        Some(pos) => pos + 7,
        None => return predicates,
    };

    let where_clause = &sql[where_pos..];
    // Truncate at GROUP BY, ORDER BY, LIMIT, HAVING
    let end_keywords = [" GROUP ", " ORDER ", " LIMIT ", " HAVING ", ";"];
    let end_pos = end_keywords
        .iter()
        .filter_map(|kw| where_clause.to_uppercase().find(kw))
        .min()
        .unwrap_or(where_clause.len());
    let where_clause = &where_clause[..end_pos];

    // Split by AND (simple approach — doesn't handle OR or nested parens)
    let parts: Vec<&str> = split_on_and(where_clause);

    for part in parts {
        let part = part.trim();
        if let Some(pred) = parse_predicate(part) {
            predicates.push(pred);
        }
    }

    predicates
}

/// Split a WHERE clause on AND keywords (case-insensitive).
fn split_on_and(clause: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let upper = clause.to_uppercase();
    let mut last = 0;

    for (i, _) in upper.match_indices(" AND ") {
        parts.push(&clause[last..i]);
        last = i + 5; // " AND ".len()
    }
    parts.push(&clause[last..]);
    parts
}

/// Parse a single predicate condition.
fn parse_predicate(condition: &str) -> Option<Predicate> {
    let condition = condition.trim();

    // Try >=, <=, >, <, = operators
    let ops = [
        (">=", PredicateOp::Gte),
        ("<=", PredicateOp::Lte),
        (">", PredicateOp::Gt),
        ("<", PredicateOp::Lt),
        ("=", PredicateOp::Eq),
    ];

    for (op_str, op) in &ops {
        if let Some(pos) = condition.find(op_str) {
            let col = condition[..pos].trim();
            let val = condition[pos + op_str.len()..].trim();

            // Clean the value: strip quotes
            let val = val
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();

            if !col.is_empty() && !val.is_empty() {
                return Some(Predicate {
                    column: col.to_string(),
                    op: op.clone(),
                    value: val,
                });
            }
        }
    }

    None
}

/// Stat filter: maps column name → (min bound, max bound).
type StatFilters = HashMap<String, (Option<serde_json::Value>, Option<serde_json::Value>)>;

/// Build partition and stat filters from predicates.
fn build_filters(
    predicates: &[Predicate],
    partition_columns: &[String],
) -> (
    HashMap<String, String>,
    StatFilters,
) {
    let mut partition_filters = HashMap::new();
    let mut stat_filters: StatFilters = HashMap::new();

    for pred in predicates {
        if partition_columns.contains(&pred.column) {
            // Partition filter: only support equality
            if matches!(pred.op, PredicateOp::Eq) {
                partition_filters.insert(pred.column.clone(), pred.value.clone());
            }
        }

        // Stat filter: convert numeric predicates
        if let Ok(num) = pred.value.parse::<f64>() {
            let json_val = serde_json::json!(num);
            let entry = stat_filters
                .entry(pred.column.clone())
                .or_insert((None, None));
            match pred.op {
                PredicateOp::Gt | PredicateOp::Gte => {
                    // min_filter: skip cells where max < this value
                    entry.0 = Some(json_val);
                }
                PredicateOp::Lt | PredicateOp::Lte => {
                    // max_filter: skip cells where min > this value
                    entry.1 = Some(json_val);
                }
                PredicateOp::Eq => {
                    // Both bounds
                    entry.0 = Some(json_val.clone());
                    entry.1 = Some(json_val);
                }
            }
        }
    }

    (partition_filters, stat_filters)
}

/// Convert a FrameSchema to an Arrow Schema.
fn frame_schema_to_arrow(schema: &apiary_core::FrameSchema) -> Result<Schema> {
    let fields: Vec<Field> = schema
        .fields
        .iter()
        .map(|f| {
            let dt = match f.data_type.as_str() {
                "int8" => DataType::Int8,
                "int16" => DataType::Int16,
                "int32" => DataType::Int32,
                "int64" => DataType::Int64,
                "uint8" => DataType::UInt8,
                "uint16" => DataType::UInt16,
                "uint32" => DataType::UInt32,
                "uint64" => DataType::UInt64,
                "float32" | "float" => DataType::Float32,
                "float64" | "double" => DataType::Float64,
                "string" | "utf8" => DataType::Utf8,
                "boolean" | "bool" => DataType::Boolean,
                "datetime" | "timestamp" => {
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                }
                _ => DataType::Utf8,
            };
            Field::new(&f.name, dt, f.nullable)
        })
        .collect();

    Ok(Schema::new(fields))
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
    use apiary_core::{CellSizingPolicy, FieldDef, FrameSchema, NodeId};
    use apiary_storage::cell_writer::CellWriter;
    use apiary_storage::ledger::Ledger;
    use apiary_storage::local::LocalBackend;
    use arrow::array::{Float64Array, Int64Array};

    async fn make_test_env() -> (Arc<dyn StorageBackend>, Arc<RegistryManager>, tempfile::TempDir)
    {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_path_buf()).await.unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(backend);
        let registry = Arc::new(RegistryManager::new(Arc::clone(&storage)));
        let _ = registry.load_or_create().await.unwrap();
        (storage, registry, dir)
    }

    fn test_schema() -> serde_json::Value {
        serde_json::json!({
            "region": "string",
            "temp": "float64",
            "humidity": "int64"
        })
    }

    async fn setup_frame(
        storage: &Arc<dyn StorageBackend>,
        registry: &Arc<RegistryManager>,
    ) {
        registry.create_hive("test_hive").await.unwrap();
        registry
            .create_box("test_hive", "test_box")
            .await
            .unwrap();
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

        let node_id = NodeId::new("test_node");
        let frame_path = "test_hive/test_box/sensors";

        let mut ledger = Ledger::create(
            Arc::clone(storage),
            frame_path,
            frame_schema.clone(),
            vec!["region".into()],
            &node_id,
        )
        .await
        .unwrap();

        let sizing = CellSizingPolicy::new(
            256 * 1024 * 1024,
            512 * 1024 * 1024,
            16 * 1024 * 1024,
        );

        let writer = CellWriter::new(
            Arc::clone(storage),
            frame_path.into(),
            frame_schema,
            vec!["region".into()],
            sizing,
        );

        // Write test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, true),
            Field::new("humidity", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "north", "north", "south", "south",
                ])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
                Arc::new(Int64Array::from(vec![50, 60, 70, 80])),
            ],
        )
        .unwrap();

        let cells = writer.write(&batch).await.unwrap();
        ledger
            .commit(
                apiary_core::LedgerAction::AddCells { cells },
                &node_id,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_select_all() {
        let (storage, registry, _dir) = make_test_env().await;
        setup_frame(&storage, &registry).await;

        let mut ctx = ApiaryQueryContext::new(storage, registry);
        let results = ctx
            .sql("SELECT * FROM test_hive.test_box.sensors")
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn test_aggregation() {
        let (storage, registry, _dir) = make_test_env().await;
        setup_frame(&storage, &registry).await;

        let mut ctx = ApiaryQueryContext::new(storage, registry);
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
        let (storage, registry, _dir) = make_test_env().await;
        setup_frame(&storage, &registry).await;

        let mut ctx = ApiaryQueryContext::new(storage, registry);
        ctx.sql("USE HIVE test_hive").await.unwrap();
        ctx.sql("USE BOX test_box").await.unwrap();
        let results = ctx.sql("SELECT * FROM sensors").await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn test_show_hives() {
        let (storage, registry, _dir) = make_test_env().await;
        setup_frame(&storage, &registry).await;

        let mut ctx = ApiaryQueryContext::new(storage, registry);
        let results = ctx.sql("SHOW HIVES").await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_show_frames() {
        let (storage, registry, _dir) = make_test_env().await;
        setup_frame(&storage, &registry).await;

        let mut ctx = ApiaryQueryContext::new(storage, registry);
        let results = ctx.sql("SHOW FRAMES IN test_hive.test_box").await.unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].num_rows() >= 1);
    }

    #[tokio::test]
    async fn test_describe() {
        let (storage, registry, _dir) = make_test_env().await;
        setup_frame(&storage, &registry).await;

        let mut ctx = ApiaryQueryContext::new(storage, registry);
        let results = ctx
            .sql("DESCRIBE test_hive.test_box.sensors")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].num_rows() >= 3);
    }

    #[tokio::test]
    async fn test_delete_blocked() {
        let (storage, registry, _dir) = make_test_env().await;
        let mut ctx = ApiaryQueryContext::new(storage, registry);

        let result = ctx.sql("DELETE FROM test_hive.test_box.sensors").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not supported"));
    }

    #[tokio::test]
    async fn test_update_blocked() {
        let (storage, registry, _dir) = make_test_env().await;
        let mut ctx = ApiaryQueryContext::new(storage, registry);

        let result = ctx
            .sql("UPDATE test_hive.test_box.sensors SET temp = 0")
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not supported"));
    }

    #[tokio::test]
    async fn test_where_filter() {
        let (storage, registry, _dir) = make_test_env().await;
        setup_frame(&storage, &registry).await;

        let mut ctx = ApiaryQueryContext::new(storage, registry);
        let results = ctx
            .sql("SELECT * FROM test_hive.test_box.sensors WHERE region = 'north'")
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_projection() {
        let (storage, registry, _dir) = make_test_env().await;
        setup_frame(&storage, &registry).await;

        let mut ctx = ApiaryQueryContext::new(storage, registry);
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
    fn test_extract_predicates() {
        let preds = extract_where_predicates("SELECT * FROM t WHERE region = 'north' AND temp > 25");
        assert_eq!(preds.len(), 2);
        assert_eq!(preds[0].column, "region");
        assert_eq!(preds[0].value, "north");
        assert_eq!(preds[1].column, "temp");
        assert_eq!(preds[1].value, "25");
    }

    #[test]
    fn test_check_unsupported_dml() {
        assert!(check_unsupported_dml("DELETE FROM t").is_some());
        assert!(check_unsupported_dml("UPDATE t SET x = 1").is_some());
        assert!(check_unsupported_dml("SELECT * FROM t").is_none());
    }
}
