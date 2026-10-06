//! Types for the transaction ledger and cell metadata.
//!
//! Each frame has a ledger — an ordered sequence of JSON entries describing
//! every mutation to the frame's state. These types define the ledger entries,
//! cell metadata, column statistics, and the write result.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::{CellId, NodeId};

/// A single entry in a frame's transaction ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    /// Monotonically increasing version number.
    pub version: u64,
    /// When this entry was committed.
    pub timestamp: DateTime<Utc>,
    /// The node that committed this entry.
    pub node_id: NodeId,
    /// The action performed.
    pub action: LedgerAction,
}

/// The action recorded in a ledger entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LedgerAction {
    /// Frame was created with the given schema and partition columns.
    CreateFrame {
        schema: FrameSchema,
        partition_by: Vec<String>,
    },
    /// New cells were added to the frame.
    AddCells { cells: Vec<CellMetadata> },
    /// Cells were rewritten (removed old + added new). Used by overwrite and compaction.
    RewriteCells {
        removed: Vec<CellId>,
        added: Vec<CellMetadata>,
    },
}

/// Schema definition for a frame, stored as field name → type string mappings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameSchema {
    /// Ordered list of field definitions.
    pub fields: Vec<FieldDef>,
}

/// A single field in a frame schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    /// Field name.
    pub name: String,
    /// Type string (e.g., "int64", "float64", "string", "boolean", "datetime").
    pub data_type: String,
    /// Whether the field can contain null values.
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

fn default_nullable() -> bool {
    true
}

/// Metadata about a single Parquet cell file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellMetadata {
    /// Unique cell identifier.
    pub id: CellId,
    /// Relative path within frame directory (e.g., "region=north/cell_abc.parquet").
    pub path: String,
    /// Storage format — always "parquet" for v1.
    pub format: String,
    /// Partition column values for this cell.
    pub partition_values: HashMap<String, String>,
    /// Number of rows in this cell.
    pub rows: u64,
    /// Size of the cell file in bytes.
    pub bytes: u64,
    /// Per-column statistics for query pruning.
    pub stats: HashMap<String, ColumnStats>,
}

/// Per-column statistics used for cell-level pruning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Minimum value in the column (as JSON value).
    pub min: Option<serde_json::Value>,
    /// Maximum value in the column (as JSON value).
    pub max: Option<serde_json::Value>,
    /// Number of null values.
    pub null_count: u64,
    /// Distinct value count (optional, expensive to compute).
    pub distinct_count: Option<u64>,
}

/// Policy for cell sizing, inspired by leafcutter bees.
#[derive(Debug, Clone)]
pub struct CellSizingPolicy {
    /// Target cell size in bytes (memory_per_bee / 4).
    pub target_cell_size: u64,
    /// Maximum cell size in bytes (target * 2).
    pub max_cell_size: u64,
    /// Minimum cell size in bytes (16 MB floor for S3 efficiency).
    pub min_cell_size: u64,
}

impl CellSizingPolicy {
    /// Create a sizing policy from a NodeConfig's parameters.
    pub fn new(target: u64, max: u64, min: u64) -> Self {
        Self {
            target_cell_size: target,
            max_cell_size: max,
            min_cell_size: min,
        }
    }

    /// Create default sizing policy from memory per bee.
    pub fn from_memory_per_bee(memory_per_bee: u64) -> Self {
        let target = memory_per_bee / 4;
        Self {
            target_cell_size: target,
            max_cell_size: target * 2,
            min_cell_size: 16 * 1024 * 1024, // 16 MB
        }
    }
}

/// Result returned from a write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    /// Ledger version after the write was committed.
    pub version: u64,
    /// Number of cells written.
    pub cells_written: usize,
    /// Total rows written.
    pub rows_written: u64,
    /// Total bytes written.
    pub bytes_written: u64,
    /// Duration of the write in milliseconds.
    pub duration_ms: u64,
    /// Colony temperature at write time (0.0 to 1.0).
    #[serde(default)]
    pub temperature: f64,
}

/// A checkpoint captures the full active cell set at a given version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerCheckpoint {
    /// The version this checkpoint represents.
    pub version: u64,
    /// The frame schema at this version.
    pub schema: FrameSchema,
    /// Partition columns.
    pub partition_by: Vec<String>,
    /// All active cells at this version.
    pub active_cells: Vec<CellMetadata>,
}

impl FrameSchema {
    /// Create a FrameSchema from a JSON schema definition (dict of name → type).
    pub fn from_json_value(value: &serde_json::Value) -> crate::Result<Self> {
        match value {
            serde_json::Value::Object(map) => {
                let fields = map
                    .iter()
                    .map(|(name, type_val)| {
                        let data_type = type_val.as_str().unwrap_or("string").to_string();
                        FieldDef {
                            name: name.clone(),
                            data_type,
                            nullable: true,
                        }
                    })
                    .collect();
                Ok(FrameSchema { fields })
            }
            _ => Err(crate::ApiaryError::Schema {
                message: "Schema must be a JSON object mapping field names to types".into(),
            }),
        }
    }

    /// Get field names.
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    /// Find a field by name.
    pub fn field(&self, name: &str) -> Option<&FieldDef> {
        self.fields.iter().find(|f| f.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_schema_from_json() {
        let json = serde_json::json!({
            "timestamp": "datetime",
            "region": "string",
            "temp": "float64"
        });
        let schema = FrameSchema::from_json_value(&json).unwrap();
        assert_eq!(schema.fields.len(), 3);
    }

    #[test]
    fn test_cell_metadata_serialization() {
        let cell = CellMetadata {
            id: CellId::new("cell_001"),
            path: "region=north/cell_001.parquet".into(),
            format: "parquet".into(),
            partition_values: HashMap::from([("region".into(), "north".into())]),
            rows: 1000,
            bytes: 4096,
            stats: HashMap::new(),
        };
        let json = serde_json::to_string(&cell).unwrap();
        let cell2: CellMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(cell2.rows, 1000);
    }

    #[test]
    fn test_ledger_entry_serialization() {
        let entry = LedgerEntry {
            version: 1,
            timestamp: Utc::now(),
            node_id: NodeId::new("node_1"),
            action: LedgerAction::AddCells {
                cells: vec![CellMetadata {
                    id: CellId::new("cell_001"),
                    path: "cell_001.parquet".into(),
                    format: "parquet".into(),
                    partition_values: HashMap::new(),
                    rows: 100,
                    bytes: 2048,
                    stats: HashMap::new(),
                }],
            },
        };
        let json = serde_json::to_string(&entry).unwrap();
        let entry2: LedgerEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry2.version, 1);
    }

    #[test]
    fn test_cell_sizing_policy() {
        let policy = CellSizingPolicy::from_memory_per_bee(1024 * 1024 * 1024); // 1 GB
        assert_eq!(policy.target_cell_size, 256 * 1024 * 1024); // 256 MB
        assert_eq!(policy.max_cell_size, 512 * 1024 * 1024); // 512 MB
        assert_eq!(policy.min_cell_size, 16 * 1024 * 1024); // 16 MB
    }

    #[test]
    fn test_write_result_serialization_with_temperature() {
        let wr = WriteResult {
            version: 5,
            cells_written: 2,
            rows_written: 1000,
            bytes_written: 4096,
            duration_ms: 42,
            temperature: 0.75,
        };
        let json = serde_json::to_string(&wr).unwrap();
        let wr2: WriteResult = serde_json::from_str(&json).unwrap();
        assert_eq!(wr2.version, 5);
        assert_eq!(wr2.cells_written, 2);
        assert_eq!(wr2.rows_written, 1000);
        assert!((wr2.temperature - 0.75).abs() < f64::EPSILON);

        // temperature defaults to 0.0 when absent
        let json_no_temp = r#"{"version":1,"cells_written":1,"rows_written":10,"bytes_written":100,"duration_ms":5}"#;
        let wr3: WriteResult = serde_json::from_str(json_no_temp).unwrap();
        assert!((wr3.temperature - 0.0).abs() < f64::EPSILON);
    }
}
