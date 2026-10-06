//! Types for Frames: their schemas, cell sizing and write results.
//!
//! A Frame is stored as a Delta Lake table (see the `apiary-comb` crate); these
//! are the Apiary-side descriptions around it.

use serde::{Deserialize, Serialize};

/// Schema definition for a frame, stored as field name to type string mappings.
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
    /// Delta table version after the write was committed.
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

impl FrameSchema {
    /// Create a FrameSchema from a JSON schema definition.
    ///
    /// Two forms are accepted:
    /// - a dict of name to type, such as `{"ts": "datetime", "temp": "float64"}`
    ///   (every field is nullable);
    /// - the serialised form of a [`FrameSchema`], such as
    ///   `{"fields": [{"name": "ts", "data_type": "datetime", "nullable": false}]}`.
    ///
    /// The forms cannot be confused: in the first, a column named `fields` has a
    /// string value, while the second has an array.
    pub fn from_json_value(value: &serde_json::Value) -> crate::Result<Self> {
        match value {
            serde_json::Value::Object(map)
                if map.len() == 1 && map.get("fields").is_some_and(|v| v.is_array()) =>
            {
                serde_json::from_value(value.clone()).map_err(|e| crate::ApiaryError::Schema {
                    message: format!("Invalid frame schema: {e}"),
                })
            }
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
    fn test_frame_schema_from_serialised_form() {
        let json = serde_json::json!({
            "fields": [
                {"name": "x", "data_type": "Int64"},
                {"name": "y", "data_type": "string", "nullable": false}
            ]
        });
        let schema = FrameSchema::from_json_value(&json).unwrap();
        assert_eq!(schema.field_names(), vec!["x", "y"]);
        assert!(schema.field("x").unwrap().nullable);
        assert!(!schema.field("y").unwrap().nullable);

        // Round trip through serde
        let again = FrameSchema::from_json_value(&serde_json::to_value(&schema).unwrap()).unwrap();
        assert_eq!(again.field_names(), vec!["x", "y"]);

        // A column literally named "fields" is still a flat-form column
        let flat = serde_json::json!({"fields": "string"});
        let schema = FrameSchema::from_json_value(&flat).unwrap();
        assert_eq!(schema.field_names(), vec!["fields"]);

        // Malformed serialised form is an error, not a silent column
        let bad = serde_json::json!({"fields": [{"nom": "x"}]});
        assert!(FrameSchema::from_json_value(&bad).is_err());
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
