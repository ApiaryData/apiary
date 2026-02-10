//! Cell writer for creating Parquet files in object storage.
//!
//! The cell writer handles partitioning incoming data, writing Parquet cells
//! with LZ4 compression, computing cell-level statistics, and respecting
//! leafcutter sizing policies.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::*;
use arrow::compute;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Schema, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::info;
use uuid::Uuid;

use apiary_core::{
    ApiaryError, CellId, CellMetadata, CellSizingPolicy, ColumnStats, FieldDef, FrameSchema,
    Result, StorageBackend,
};

/// Writes Arrow RecordBatches as Parquet cells to object storage.
pub struct CellWriter {
    storage: Arc<dyn StorageBackend>,
    frame_path: String,
    schema: FrameSchema,
    partition_by: Vec<String>,
    sizing: CellSizingPolicy,
}

impl CellWriter {
    /// Create a new CellWriter.
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        frame_path: String,
        schema: FrameSchema,
        partition_by: Vec<String>,
        sizing: CellSizingPolicy,
    ) -> Self {
        Self {
            storage,
            frame_path,
            schema,
            partition_by,
            sizing,
        }
    }

    /// Write a RecordBatch, partitioning and sizing as needed.
    /// Returns the CellMetadata for each cell written.
    pub async fn write(&self, batch: &RecordBatch) -> Result<Vec<CellMetadata>> {
        // Validate schema
        self.validate_schema(batch)?;

        // Partition the data
        let partitions = self.partition_data(batch)?;

        let mut all_cells = Vec::new();

        for (partition_values, partition_batch) in &partitions {
            // Split by cell sizing if needed
            let sub_batches = self.split_by_size(partition_batch)?;

            for sub_batch in &sub_batches {
                let cell = self
                    .write_cell(sub_batch, partition_values)
                    .await?;
                all_cells.push(cell);
            }
        }

        Ok(all_cells)
    }

    /// Validate the incoming batch against the frame schema.
    fn validate_schema(&self, batch: &RecordBatch) -> Result<()> {
        // Check that partition columns have no nulls and no path traversal characters
        for part_col in &self.partition_by {
            if let Some(col_idx) = batch.schema().index_of(part_col).ok() {
                let col = batch.column(col_idx);
                if col.null_count() > 0 {
                    return Err(ApiaryError::Schema {
                        message: format!(
                            "Partition column '{}' contains null values",
                            part_col
                        ),
                    });
                }
                // Validate partition values don't contain path traversal characters
                for row_idx in 0..batch.num_rows() {
                    let val = array_value_to_string(col, row_idx);
                    if val.contains("..") || val.contains('/') || val.contains('\\') || val.contains('\0') {
                        return Err(ApiaryError::Schema {
                            message: format!(
                                "Partition column '{}' contains invalid characters (path separators or '..'): '{}'",
                                part_col, val
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Partition data by partition column values.
    fn partition_data(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<(HashMap<String, String>, RecordBatch)>> {
        if self.partition_by.is_empty() || batch.num_rows() == 0 {
            return Ok(vec![(HashMap::new(), batch.clone())]);
        }

        // Build partition keys for each row
        let mut partition_keys: Vec<HashMap<String, String>> = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let mut key = HashMap::new();
            for col_name in &self.partition_by {
                let col_idx = batch
                    .schema()
                    .index_of(col_name)
                    .map_err(|_| ApiaryError::Schema {
                        message: format!("Partition column '{}' not found in data", col_name),
                    })?;
                let col = batch.column(col_idx);
                let val = array_value_to_string(col, row_idx);
                key.insert(col_name.clone(), val);
            }
            partition_keys.push(key);
        }

        // Group rows by partition key
        let mut groups: HashMap<String, (HashMap<String, String>, Vec<usize>)> = HashMap::new();
        for (row_idx, key) in partition_keys.iter().enumerate() {
            let key_str = partition_key_string(key, &self.partition_by);
            groups
                .entry(key_str)
                .or_insert_with(|| (key.clone(), Vec::new()))
                .1
                .push(row_idx);
        }

        // Build sub-batches
        let mut result = Vec::new();
        for (_, (partition_values, row_indices)) in groups {
            let indices = UInt32Array::from(
                row_indices.iter().map(|i| *i as u32).collect::<Vec<_>>(),
            );
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| compute::take(col, &indices, None).unwrap())
                .collect();
            let sub_batch =
                RecordBatch::try_new(batch.schema(), columns).map_err(|e| {
                    ApiaryError::Internal {
                        message: format!("Failed to create partition batch: {}", e),
                    }
                })?;
            result.push((partition_values, sub_batch));
        }

        Ok(result)
    }

    /// Split a batch into multiple batches if it exceeds the target cell size.
    fn split_by_size(&self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        // Estimate the size of the batch
        let estimated_size: usize = batch
            .columns()
            .iter()
            .map(|col| col.get_buffer_memory_size())
            .sum();

        let target = self.sizing.target_cell_size as usize;

        if estimated_size <= target || batch.num_rows() <= 1 {
            return Ok(vec![batch.clone()]);
        }

        // Calculate how many chunks we need
        let num_chunks = (estimated_size + target - 1) / target;
        let rows_per_chunk = (batch.num_rows() + num_chunks - 1) / num_chunks;
        let rows_per_chunk = rows_per_chunk.max(1);

        let mut batches = Vec::new();
        let mut start = 0;

        while start < batch.num_rows() {
            let end = (start + rows_per_chunk).min(batch.num_rows());
            let sub_batch = batch.slice(start, end - start);
            batches.push(sub_batch);
            start = end;
        }

        Ok(batches)
    }

    /// Write a single cell to storage. Returns metadata about the cell.
    async fn write_cell(
        &self,
        batch: &RecordBatch,
        partition_values: &HashMap<String, String>,
    ) -> Result<CellMetadata> {
        let cell_id = CellId::new(format!("cell_{}", Uuid::new_v4()));
        let rows = batch.num_rows() as u64;

        // Build the storage path
        let partition_path = if partition_values.is_empty() {
            String::new()
        } else {
            let parts: Vec<String> = self
                .partition_by
                .iter()
                .filter_map(|col| {
                    partition_values
                        .get(col)
                        .map(|val| format!("{}={}", col, val))
                })
                .collect();
            parts.join("/") + "/"
        };

        let cell_filename = format!("{}.parquet", cell_id.as_str());
        let relative_path = format!("{}{}", partition_path, cell_filename);
        let storage_key = format!("{}/{}", self.frame_path, relative_path);

        // Compute cell-level statistics
        let stats = compute_column_stats(batch, &self.schema)?;

        // Write the Parquet file
        let parquet_bytes = write_parquet_bytes(batch)?;
        let byte_size = parquet_bytes.len() as u64;

        self.storage
            .put(&storage_key, Bytes::from(parquet_bytes))
            .await?;

        info!(
            cell_id = %cell_id,
            rows,
            bytes = byte_size,
            path = %relative_path,
            "Wrote cell to storage"
        );

        Ok(CellMetadata {
            id: cell_id,
            path: relative_path,
            format: "parquet".into(),
            partition_values: partition_values.clone(),
            rows,
            bytes: byte_size,
            stats,
        })
    }
}

/// Convert the frame's type string to an Arrow DataType.
pub fn type_string_to_arrow(type_str: &str) -> DataType {
    match type_str.to_lowercase().as_str() {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" | "int" | "integer" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float16" | "half" => DataType::Float16,
        "float32" | "float" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "string" | "utf8" | "text" => DataType::Utf8,
        "boolean" | "bool" => DataType::Boolean,
        "datetime" | "timestamp" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        "date" => DataType::Date32,
        "binary" | "bytes" => DataType::Binary,
        _ => DataType::Utf8, // Default to string
    }
}

/// Build an Arrow Schema from a FrameSchema.
pub fn frame_schema_to_arrow(schema: &FrameSchema) -> Schema {
    let fields: Vec<Field> = schema
        .fields
        .iter()
        .map(|f| Field::new(&f.name, type_string_to_arrow(&f.data_type), f.nullable))
        .collect();
    Schema::new(fields)
}

/// Build a FrameSchema from an Arrow Schema.
pub fn arrow_schema_to_frame(schema: &Schema) -> FrameSchema {
    let fields: Vec<FieldDef> = schema
        .fields()
        .iter()
        .map(|f| FieldDef {
            name: f.name().clone(),
            data_type: arrow_type_to_string(f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();
    FrameSchema { fields }
}

/// Convert Arrow DataType to type string.
fn arrow_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Float16 => "float16".into(),
        DataType::Float32 => "float32".into(),
        DataType::Float64 => "float64".into(),
        DataType::Utf8 => "string".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Timestamp(_, _) => "datetime".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Binary => "binary".into(),
        _ => "string".into(),
    }
}

/// Write a RecordBatch to Parquet bytes in memory with LZ4 compression.
fn write_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .build();

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| ApiaryError::Storage {
                message: format!("Failed to create Parquet writer: {}", e),
                source: None,
            })?;
        writer
            .write(batch)
            .map_err(|e| ApiaryError::Storage {
                message: format!("Failed to write Parquet data: {}", e),
                source: None,
            })?;
        writer
            .close()
            .map_err(|e| ApiaryError::Storage {
                message: format!("Failed to close Parquet writer: {}", e),
                source: None,
            })?;
    }
    Ok(buf)
}

/// Compute column-level statistics from a RecordBatch.
fn compute_column_stats(
    batch: &RecordBatch,
    schema: &FrameSchema,
) -> Result<HashMap<String, ColumnStats>> {
    let mut stats = HashMap::new();

    for field in &schema.fields {
        if let Ok(col_idx) = batch.schema().index_of(&field.name) {
            let col = batch.column(col_idx);
            let null_count = col.null_count() as u64;

            let (min_val, max_val) = compute_min_max(col);

            stats.insert(
                field.name.clone(),
                ColumnStats {
                    min: min_val,
                    max: max_val,
                    null_count,
                    distinct_count: None,
                },
            );
        }
    }

    Ok(stats)
}

/// Compute min/max values for an array.
fn compute_min_max(array: &dyn Array) -> (Option<serde_json::Value>, Option<serde_json::Value>) {
    if array.is_empty() || array.null_count() == array.len() {
        return (None, None);
    }

    match array.data_type() {
        DataType::Int8 => numeric_min_max::<Int8Type>(array),
        DataType::Int16 => numeric_min_max::<Int16Type>(array),
        DataType::Int32 => numeric_min_max::<Int32Type>(array),
        DataType::Int64 => numeric_min_max::<Int64Type>(array),
        DataType::UInt8 => numeric_min_max::<UInt8Type>(array),
        DataType::UInt16 => numeric_min_max::<UInt16Type>(array),
        DataType::UInt32 => numeric_min_max::<UInt32Type>(array),
        DataType::UInt64 => uint64_min_max(array),
        DataType::Float32 => float_min_max::<Float32Type>(array),
        DataType::Float64 => float_min_max::<Float64Type>(array),
        DataType::Utf8 => string_min_max(array),
        DataType::Boolean => bool_min_max(array),
        _ => (None, None),
    }
}

fn numeric_min_max<T>(array: &dyn Array) -> (Option<serde_json::Value>, Option<serde_json::Value>)
where
    T: ArrowPrimitiveType,
    T::Native: Into<i64>,
{
    let arr = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();
    let values: Vec<T::Native> = arr.iter().flatten().collect();
    if values.is_empty() {
        return (None, None);
    }
    let min: i64 = values.iter().copied().map(Into::into).min().unwrap();
    let max: i64 = values.iter().copied().map(Into::into).max().unwrap();
    (
        Some(serde_json::Value::Number(min.into())),
        Some(serde_json::Value::Number(max.into())),
    )
}

fn uint64_min_max(array: &dyn Array) -> (Option<serde_json::Value>, Option<serde_json::Value>) {
    let arr = array
        .as_any()
        .downcast_ref::<PrimitiveArray<UInt64Type>>()
        .unwrap();
    let values: Vec<u64> = arr.iter().flatten().collect();
    if values.is_empty() {
        return (None, None);
    }
    let min = *values.iter().min().unwrap();
    let max = *values.iter().max().unwrap();
    (
        Some(serde_json::json!(min as f64)),
        Some(serde_json::json!(max as f64)),
    )
}

fn float_min_max<T>(array: &dyn Array) -> (Option<serde_json::Value>, Option<serde_json::Value>)
where
    T: ArrowPrimitiveType,
    T::Native: Into<f64>,
{
    let arr = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();
    let values: Vec<f64> = arr.iter().flatten().map(|v| v.into()).collect();
    if values.is_empty() {
        return (None, None);
    }
    let min = values
        .iter()
        .copied()
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap();
    let max = values
        .iter()
        .copied()
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap();
    (
        Some(serde_json::json!(min)),
        Some(serde_json::json!(max)),
    )
}

fn string_min_max(array: &dyn Array) -> (Option<serde_json::Value>, Option<serde_json::Value>) {
    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
    let values: Vec<&str> = arr.iter().flatten().collect();
    if values.is_empty() {
        return (None, None);
    }
    let min = values.iter().min().unwrap();
    let max = values.iter().max().unwrap();
    (
        Some(serde_json::Value::String(min.to_string())),
        Some(serde_json::Value::String(max.to_string())),
    )
}

fn bool_min_max(array: &dyn Array) -> (Option<serde_json::Value>, Option<serde_json::Value>) {
    let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    let values: Vec<bool> = arr.iter().flatten().collect();
    if values.is_empty() {
        return (None, None);
    }
    // Min is false if any value is false, otherwise true
    let has_false = values.iter().any(|v| !v);
    // Max is true if any value is true, otherwise false
    let has_true = values.iter().any(|v| *v);
    (
        Some(serde_json::Value::Bool(!has_false)),
        Some(serde_json::Value::Bool(has_true)),
    )
}

/// Extract a string value from an array at a given index.
fn array_value_to_string(array: &ArrayRef, idx: usize) -> String {
    if array.is_null(idx) {
        return "null".into();
    }

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(idx).to_string()
        }
        _ => format!("{:?}", array),
    }
}

/// Build a canonical string from partition values for grouping.
fn partition_key_string(
    values: &HashMap<String, String>,
    partition_by: &[String],
) -> String {
    partition_by
        .iter()
        .map(|col| {
            let val = values.get(col).map(|v| v.as_str()).unwrap_or("");
            format!("{}={}", col, val)
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_string_to_arrow() {
        assert_eq!(type_string_to_arrow("int64"), DataType::Int64);
        assert_eq!(type_string_to_arrow("float64"), DataType::Float64);
        assert_eq!(type_string_to_arrow("string"), DataType::Utf8);
        assert_eq!(type_string_to_arrow("boolean"), DataType::Boolean);
    }

    #[test]
    fn test_frame_schema_to_arrow() {
        let schema = FrameSchema {
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
        };
        let arrow_schema = frame_schema_to_arrow(&schema);
        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "region");
        assert_eq!(*arrow_schema.field(0).data_type(), DataType::Utf8);
    }

    #[test]
    fn test_write_parquet_bytes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();

        let bytes = write_parquet_bytes(&batch).unwrap();
        assert!(!bytes.is_empty());
        // Parquet magic bytes
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_compute_column_stats() {
        let schema = FrameSchema {
            fields: vec![
                FieldDef {
                    name: "name".into(),
                    data_type: "string".into(),
                    nullable: false,
                },
                FieldDef {
                    name: "value".into(),
                    data_type: "float64".into(),
                    nullable: true,
                },
            ],
        };

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["alpha", "gamma", "beta"])),
                Arc::new(Float64Array::from(vec![10.5, 30.2, 20.1])),
            ],
        )
        .unwrap();

        let stats = compute_column_stats(&batch, &schema).unwrap();

        assert!(stats.contains_key("name"));
        assert!(stats.contains_key("value"));

        let name_stats = &stats["name"];
        assert_eq!(
            name_stats.min,
            Some(serde_json::Value::String("alpha".into()))
        );
        assert_eq!(
            name_stats.max,
            Some(serde_json::Value::String("gamma".into()))
        );

        let value_stats = &stats["value"];
        assert_eq!(value_stats.min, Some(serde_json::json!(10.5)));
        assert_eq!(value_stats.max, Some(serde_json::json!(30.2)));
    }
}
