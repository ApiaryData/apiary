//! Transaction ledger for frame-level ACID operations.
//!
//! Each frame has a ledger — an ordered sequence of JSON files describing
//! every mutation. The ledger is the source of truth for which cells are
//! active, the schema, and version history.
//!
//! Commits use conditional writes (`put_if_not_exists`) for optimistic
//! concurrency control — no consensus protocol needed.

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use tracing::{info, warn};

use apiary_core::{
    ApiaryError, CellMetadata, FrameSchema, LedgerAction, LedgerCheckpoint, LedgerEntry, NodeId,
    Result, StorageBackend,
};

/// Maximum number of commit retries on conflict.
const MAX_RETRIES: usize = 10;

/// Checkpoint interval — write a checkpoint every N versions.
const CHECKPOINT_INTERVAL: u64 = 100;

/// The transaction ledger for a single frame.
pub struct Ledger {
    /// Frame path in storage (e.g., "my_hive/my_box/my_frame").
    frame_path: String,
    /// Storage backend.
    storage: Arc<dyn StorageBackend>,
    /// Current ledger version.
    current_version: u64,
    /// Currently active cells (derived from replay).
    active_cells: Vec<CellMetadata>,
    /// Frame schema.
    schema: FrameSchema,
    /// Partition columns.
    partition_by: Vec<String>,
}

impl Ledger {
    /// Create a new ledger for a frame, writing version 0 (CreateFrame entry).
    pub async fn create(
        storage: Arc<dyn StorageBackend>,
        frame_path: &str,
        schema: FrameSchema,
        partition_by: Vec<String>,
        node_id: &NodeId,
    ) -> Result<Self> {
        let entry = LedgerEntry {
            version: 0,
            timestamp: Utc::now(),
            node_id: node_id.clone(),
            action: LedgerAction::CreateFrame {
                schema: schema.clone(),
                partition_by: partition_by.clone(),
            },
        };

        let key = ledger_entry_key(frame_path, 0);
        let data = serde_json::to_vec_pretty(&entry)
            .map_err(|e| ApiaryError::Serialization(e.to_string()))?;

        let wrote = storage
            .put_if_not_exists(&key, Bytes::from(data))
            .await?;

        if !wrote {
            // Ledger already exists — open it instead
            return Self::open(storage, frame_path).await;
        }

        info!(frame_path, "Created ledger version 0");

        Ok(Self {
            frame_path: frame_path.to_string(),
            storage,
            current_version: 0,
            active_cells: Vec::new(),
            schema,
            partition_by,
        })
    }

    /// Open an existing ledger by loading the latest checkpoint and replaying entries.
    pub async fn open(
        storage: Arc<dyn StorageBackend>,
        frame_path: &str,
    ) -> Result<Self> {
        let (version, schema, partition_by, active_cells) =
            Self::load_state(&storage, frame_path).await?;

        Ok(Self {
            frame_path: frame_path.to_string(),
            storage,
            current_version: version,
            active_cells,
            schema,
            partition_by,
        })
    }

    /// Load the current state by finding the latest checkpoint and replaying entries after it.
    async fn load_state(
        storage: &Arc<dyn StorageBackend>,
        frame_path: &str,
    ) -> Result<(u64, FrameSchema, Vec<String>, Vec<CellMetadata>)> {
        // Try to load the latest checkpoint
        let checkpoint_prefix = format!("{frame_path}/_ledger/_checkpoint/");
        let checkpoint_files = storage.list(&checkpoint_prefix).await?;

        let mut start_version = 0u64;
        let mut schema = FrameSchema {
            fields: Vec::new(),
        };
        let mut partition_by = Vec::new();
        let mut active_cells: Vec<CellMetadata> = Vec::new();

        if let Some(latest_cp) = checkpoint_files
            .iter()
            .filter(|f| f.ends_with(".json"))
            .max()
        {
            let cp_data = storage.get(latest_cp).await?;
            let checkpoint: LedgerCheckpoint = serde_json::from_slice(&cp_data)
                .map_err(|e| ApiaryError::Serialization(e.to_string()))?;
            start_version = checkpoint.version;
            schema = checkpoint.schema;
            partition_by = checkpoint.partition_by;
            active_cells = checkpoint.active_cells;
        }

        // List all ledger entries and replay from start_version
        let ledger_prefix = format!("{frame_path}/_ledger/");
        let all_entries = storage.list(&ledger_prefix).await?;

        let mut entry_files: Vec<(u64, String)> = all_entries
            .iter()
            .filter(|f| {
                !f.contains("_checkpoint") && f.ends_with(".json")
            })
            .filter_map(|f| {
                let filename = f.rsplit('/').next()?;
                let version_str = filename.strip_suffix(".json")?;
                let version: u64 = version_str.parse().ok()?;
                Some((version, f.clone()))
            })
            .filter(|(v, _)| *v >= start_version)
            .collect();

        entry_files.sort_by_key(|(v, _)| *v);

        let mut current_version = start_version;

        for (version, path) in &entry_files {
            let data = storage.get(path).await?;
            let entry: LedgerEntry = serde_json::from_slice(&data)
                .map_err(|e| ApiaryError::Serialization(e.to_string()))?;

            match &entry.action {
                LedgerAction::CreateFrame {
                    schema: s,
                    partition_by: pb,
                } => {
                    schema = s.clone();
                    partition_by = pb.clone();
                    active_cells.clear();
                }
                LedgerAction::AddCells { cells } => {
                    active_cells.extend(cells.iter().cloned());
                }
                LedgerAction::RewriteCells { removed, added } => {
                    active_cells.retain(|c| !removed.contains(&c.id));
                    active_cells.extend(added.iter().cloned());
                }
            }

            current_version = *version;
        }

        Ok((current_version, schema, partition_by, active_cells))
    }

    /// Reload the ledger state from storage.
    async fn reload(&mut self) -> Result<()> {
        let (version, schema, partition_by, active_cells) =
            Self::load_state(&self.storage, &self.frame_path).await?;
        self.current_version = version;
        self.schema = schema;
        self.partition_by = partition_by;
        self.active_cells = active_cells;
        Ok(())
    }

    /// Commit an action to the ledger with optimistic concurrency.
    /// Returns the committed version number.
    pub async fn commit(&mut self, action: LedgerAction, node_id: &NodeId) -> Result<u64> {
        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                // Reload state on retry to get fresh version
                self.reload().await?;
            }

            let next_version = self.current_version + 1;
            let entry = LedgerEntry {
                version: next_version,
                timestamp: Utc::now(),
                node_id: node_id.clone(),
                action: action.clone(),
            };

            let key = ledger_entry_key(&self.frame_path, next_version);
            let data = serde_json::to_vec_pretty(&entry)
                .map_err(|e| ApiaryError::Serialization(e.to_string()))?;

            let wrote = self
                .storage
                .put_if_not_exists(&key, Bytes::from(data))
                .await?;

            if wrote {
                // Apply the action to our in-memory state
                match &action {
                    LedgerAction::CreateFrame {
                        schema,
                        partition_by,
                    } => {
                        self.schema = schema.clone();
                        self.partition_by = partition_by.clone();
                        self.active_cells.clear();
                    }
                    LedgerAction::AddCells { cells } => {
                        self.active_cells.extend(cells.iter().cloned());
                    }
                    LedgerAction::RewriteCells { removed, added } => {
                        self.active_cells.retain(|c| !removed.contains(&c.id));
                        self.active_cells.extend(added.iter().cloned());
                    }
                }

                self.current_version = next_version;

                info!(
                    frame_path = %self.frame_path,
                    version = next_version,
                    "Committed ledger entry"
                );

                // Write checkpoint every CHECKPOINT_INTERVAL versions
                if next_version % CHECKPOINT_INTERVAL == 0 {
                    self.write_checkpoint().await?;
                }

                return Ok(next_version);
            }

            warn!(
                frame_path = %self.frame_path,
                attempt,
                version = next_version,
                "Ledger commit conflict, retrying"
            );
        }

        Err(ApiaryError::Conflict {
            message: format!(
                "Failed to commit ledger entry for {} after {} retries",
                self.frame_path, MAX_RETRIES
            ),
        })
    }

    /// Write a checkpoint with the current state.
    async fn write_checkpoint(&self) -> Result<()> {
        let checkpoint = LedgerCheckpoint {
            version: self.current_version,
            schema: self.schema.clone(),
            partition_by: self.partition_by.clone(),
            active_cells: self.active_cells.clone(),
        };

        let key = format!(
            "{}/_ledger/_checkpoint/checkpoint_{:06}.json",
            self.frame_path, self.current_version
        );
        let data = serde_json::to_vec_pretty(&checkpoint)
            .map_err(|e| ApiaryError::Serialization(e.to_string()))?;

        self.storage.put(&key, Bytes::from(data)).await?;
        info!(
            frame_path = %self.frame_path,
            version = self.current_version,
            "Wrote ledger checkpoint"
        );
        Ok(())
    }

    /// Get the currently active cells.
    pub fn active_cells(&self) -> &[CellMetadata] {
        &self.active_cells
    }

    /// Get the current version.
    pub fn current_version(&self) -> u64 {
        self.current_version
    }

    /// Get the frame schema.
    pub fn schema(&self) -> &FrameSchema {
        &self.schema
    }

    /// Get the partition columns.
    pub fn partition_by(&self) -> &[String] {
        &self.partition_by
    }

    /// Get the frame path.
    pub fn frame_path(&self) -> &str {
        &self.frame_path
    }

    /// Prune cells based on partition filters and column stat filters.
    /// Returns references to cells that match the given filters.
    pub fn prune_cells(
        &self,
        partition_filters: &std::collections::HashMap<String, String>,
        stat_filters: &std::collections::HashMap<String, (Option<serde_json::Value>, Option<serde_json::Value>)>,
    ) -> Vec<&CellMetadata> {
        self.active_cells
            .iter()
            .filter(|cell| {
                // Partition pruning: skip cells whose partition values don't match
                for (col, val) in partition_filters {
                    if let Some(cell_val) = cell.partition_values.get(col) {
                        if cell_val != val {
                            return false;
                        }
                    }
                }

                // Stat pruning: skip cells whose min/max don't overlap the filter range
                for (col, (min_filter, max_filter)) in stat_filters {
                    if let Some(col_stats) = cell.stats.get(col) {
                        // If filter has a minimum and cell's max is less, skip
                        if let (Some(filter_min), Some(cell_max)) =
                            (min_filter, &col_stats.max)
                        {
                            if json_value_lt(cell_max, filter_min) {
                                return false;
                            }
                        }
                        // If filter has a maximum and cell's min is greater, skip
                        if let (Some(filter_max), Some(cell_min)) =
                            (max_filter, &col_stats.min)
                        {
                            if json_value_lt(filter_max, cell_min) {
                                return false;
                            }
                        }
                    }
                }

                true
            })
            .collect()
    }
}

/// Compare two JSON values as numbers (for stat pruning).
fn json_value_lt(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(a_f), Some(b_f)) => a_f < b_f,
        _ => {
            // Fall back to string comparison
            match (a.as_str(), b.as_str()) {
                (Some(a_s), Some(b_s)) => a_s < b_s,
                _ => false,
            }
        }
    }
}

/// Generate the storage key for a ledger entry.
fn ledger_entry_key(frame_path: &str, version: u64) -> String {
    format!("{frame_path}/_ledger/{version:06}.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use apiary_core::{CellId, ColumnStats, FieldDef};
    use crate::local::LocalBackend;
    use std::collections::HashMap;

    async fn make_storage() -> Arc<dyn StorageBackend> {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.into_path()).await.unwrap();
        Arc::new(backend)
    }

    fn test_schema() -> FrameSchema {
        FrameSchema {
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
            ],
        }
    }

    #[tokio::test]
    async fn test_ledger_create() {
        let storage = make_storage().await;
        let node_id = NodeId::new("test_node");
        let schema = test_schema();

        let ledger = Ledger::create(
            storage.clone(),
            "hive/box/frame",
            schema,
            vec!["region".into()],
            &node_id,
        )
        .await
        .unwrap();

        assert_eq!(ledger.current_version(), 0);
        assert!(ledger.active_cells().is_empty());
        assert_eq!(ledger.partition_by(), &["region"]);
    }

    #[tokio::test]
    async fn test_ledger_create_idempotent() {
        let storage = make_storage().await;
        let node_id = NodeId::new("test_node");
        let schema = test_schema();

        let _ledger1 = Ledger::create(
            storage.clone(),
            "hive/box/frame",
            schema.clone(),
            vec!["region".into()],
            &node_id,
        )
        .await
        .unwrap();

        // Creating again should open the existing ledger
        let ledger2 = Ledger::create(
            storage.clone(),
            "hive/box/frame",
            schema,
            vec!["region".into()],
            &node_id,
        )
        .await
        .unwrap();

        assert_eq!(ledger2.current_version(), 0);
    }

    #[tokio::test]
    async fn test_ledger_commit_add_cells() {
        let storage = make_storage().await;
        let node_id = NodeId::new("test_node");
        let schema = test_schema();

        let mut ledger = Ledger::create(
            storage.clone(),
            "hive/box/frame",
            schema,
            vec!["region".into()],
            &node_id,
        )
        .await
        .unwrap();

        let cells = vec![CellMetadata {
            id: CellId::new("cell_001"),
            path: "region=north/cell_001.parquet".into(),
            format: "parquet".into(),
            partition_values: HashMap::from([("region".into(), "north".into())]),
            rows: 100,
            bytes: 2048,
            stats: HashMap::new(),
        }];

        let version = ledger
            .commit(LedgerAction::AddCells { cells }, &node_id)
            .await
            .unwrap();

        assert_eq!(version, 1);
        assert_eq!(ledger.active_cells().len(), 1);
        assert_eq!(ledger.active_cells()[0].rows, 100);
    }

    #[tokio::test]
    async fn test_ledger_open_replays() {
        let storage = make_storage().await;
        let node_id = NodeId::new("test_node");
        let schema = test_schema();

        {
            let mut ledger = Ledger::create(
                storage.clone(),
                "hive/box/frame",
                schema,
                vec!["region".into()],
                &node_id,
            )
            .await
            .unwrap();

            let cells = vec![CellMetadata {
                id: CellId::new("cell_001"),
                path: "region=north/cell_001.parquet".into(),
                format: "parquet".into(),
                partition_values: HashMap::from([("region".into(), "north".into())]),
                rows: 100,
                bytes: 2048,
                stats: HashMap::new(),
            }];

            ledger
                .commit(LedgerAction::AddCells { cells }, &node_id)
                .await
                .unwrap();
        }

        // Open and verify replay
        let ledger = Ledger::open(storage.clone(), "hive/box/frame")
            .await
            .unwrap();

        assert_eq!(ledger.current_version(), 1);
        assert_eq!(ledger.active_cells().len(), 1);
    }

    #[tokio::test]
    async fn test_ledger_rewrite_cells() {
        let storage = make_storage().await;
        let node_id = NodeId::new("test_node");
        let schema = test_schema();

        let mut ledger = Ledger::create(
            storage.clone(),
            "hive/box/frame",
            schema,
            vec!["region".into()],
            &node_id,
        )
        .await
        .unwrap();

        // Add cells
        let cell1 = CellMetadata {
            id: CellId::new("cell_001"),
            path: "cell_001.parquet".into(),
            format: "parquet".into(),
            partition_values: HashMap::new(),
            rows: 100,
            bytes: 2048,
            stats: HashMap::new(),
        };
        let cell2 = CellMetadata {
            id: CellId::new("cell_002"),
            path: "cell_002.parquet".into(),
            format: "parquet".into(),
            partition_values: HashMap::new(),
            rows: 200,
            bytes: 4096,
            stats: HashMap::new(),
        };

        ledger
            .commit(
                LedgerAction::AddCells {
                    cells: vec![cell1, cell2],
                },
                &node_id,
            )
            .await
            .unwrap();

        assert_eq!(ledger.active_cells().len(), 2);

        // Rewrite: remove cell_001, add cell_003
        let cell3 = CellMetadata {
            id: CellId::new("cell_003"),
            path: "cell_003.parquet".into(),
            format: "parquet".into(),
            partition_values: HashMap::new(),
            rows: 300,
            bytes: 6144,
            stats: HashMap::new(),
        };

        ledger
            .commit(
                LedgerAction::RewriteCells {
                    removed: vec![CellId::new("cell_001")],
                    added: vec![cell3],
                },
                &node_id,
            )
            .await
            .unwrap();

        assert_eq!(ledger.active_cells().len(), 2);
        let ids: Vec<&str> = ledger
            .active_cells()
            .iter()
            .map(|c| c.id.as_str())
            .collect();
        assert!(ids.contains(&"cell_002"));
        assert!(ids.contains(&"cell_003"));
    }

    #[tokio::test]
    async fn test_prune_cells_by_partition() {
        let storage = make_storage().await;
        let node_id = NodeId::new("test_node");
        let schema = test_schema();

        let mut ledger = Ledger::create(
            storage.clone(),
            "hive/box/frame",
            schema,
            vec!["region".into()],
            &node_id,
        )
        .await
        .unwrap();

        let cells = vec![
            CellMetadata {
                id: CellId::new("cell_north"),
                path: "region=north/cell_north.parquet".into(),
                format: "parquet".into(),
                partition_values: HashMap::from([("region".into(), "north".into())]),
                rows: 100,
                bytes: 2048,
                stats: HashMap::new(),
            },
            CellMetadata {
                id: CellId::new("cell_south"),
                path: "region=south/cell_south.parquet".into(),
                format: "parquet".into(),
                partition_values: HashMap::from([("region".into(), "south".into())]),
                rows: 200,
                bytes: 4096,
                stats: HashMap::new(),
            },
        ];

        ledger
            .commit(LedgerAction::AddCells { cells }, &node_id)
            .await
            .unwrap();

        // Filter for region=north
        let filters = HashMap::from([("region".into(), "north".into())]);
        let pruned = ledger.prune_cells(&filters, &HashMap::new());
        assert_eq!(pruned.len(), 1);
        assert_eq!(pruned[0].id.as_str(), "cell_north");
    }

    #[tokio::test]
    async fn test_prune_cells_by_stats() {
        let storage = make_storage().await;
        let node_id = NodeId::new("test_node");
        let schema = test_schema();

        let mut ledger = Ledger::create(
            storage.clone(),
            "hive/box/frame",
            schema,
            vec![],
            &node_id,
        )
        .await
        .unwrap();

        let cells = vec![
            CellMetadata {
                id: CellId::new("cell_low"),
                path: "cell_low.parquet".into(),
                format: "parquet".into(),
                partition_values: HashMap::new(),
                rows: 100,
                bytes: 2048,
                stats: HashMap::from([(
                    "temp".into(),
                    ColumnStats {
                        min: Some(serde_json::json!(10.0)),
                        max: Some(serde_json::json!(20.0)),
                        null_count: 0,
                        distinct_count: None,
                    },
                )]),
            },
            CellMetadata {
                id: CellId::new("cell_high"),
                path: "cell_high.parquet".into(),
                format: "parquet".into(),
                partition_values: HashMap::new(),
                rows: 100,
                bytes: 2048,
                stats: HashMap::from([(
                    "temp".into(),
                    ColumnStats {
                        min: Some(serde_json::json!(30.0)),
                        max: Some(serde_json::json!(40.0)),
                        null_count: 0,
                        distinct_count: None,
                    },
                )]),
            },
        ];

        ledger
            .commit(LedgerAction::AddCells { cells }, &node_id)
            .await
            .unwrap();

        // Filter: temp > 25 (min_filter=25, no max_filter)
        let stat_filters: HashMap<String, (Option<serde_json::Value>, Option<serde_json::Value>)> =
            HashMap::from([(
                "temp".into(),
                (Some(serde_json::json!(25.0)), None),
            )]);
        let pruned = ledger.prune_cells(&HashMap::new(), &stat_filters);
        assert_eq!(pruned.len(), 1);
        assert_eq!(pruned[0].id.as_str(), "cell_high");
    }
}
