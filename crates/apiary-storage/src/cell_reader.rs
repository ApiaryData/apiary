//! Cell reader for loading Parquet files from object storage.
//!
//! Supports projection pushdown (reading only requested columns) and
//! reading cells back as Arrow RecordBatches.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use apiary_core::{ApiaryError, CellMetadata, Result, StorageBackend};

/// Reads Parquet cells from object storage into Arrow RecordBatches.
pub struct CellReader {
    storage: Arc<dyn StorageBackend>,
    frame_path: String,
}

impl CellReader {
    /// Create a new CellReader.
    pub fn new(storage: Arc<dyn StorageBackend>, frame_path: String) -> Self {
        Self {
            storage,
            frame_path,
        }
    }

    /// Read a single cell from storage. Returns all record batches in the cell.
    ///
    /// If `projection` is Some, only the specified columns are read (projection pushdown).
    pub async fn read_cell(
        &self,
        cell: &CellMetadata,
        projection: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>> {
        let storage_key = format!("{}/{}", self.frame_path, cell.path);
        let data = self.storage.get(&storage_key).await?;

        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data.to_vec()))
                .map_err(|e| ApiaryError::Storage {
                    message: format!("Failed to open Parquet reader: {}", e),
                    source: None,
                })?;

        // Apply projection if specified
        let reader = if let Some(cols) = projection {
            let parquet_schema = reader_builder.schema().clone();
            let indices: Vec<usize> = cols
                .iter()
                .filter_map(|col_name| parquet_schema.index_of(col_name).ok())
                .collect();

            let mask = parquet::arrow::ProjectionMask::roots(
                reader_builder.parquet_schema(),
                indices,
            );
            reader_builder
                .with_projection(mask)
                .build()
                .map_err(|e| ApiaryError::Storage {
                    message: format!("Failed to build Parquet reader: {}", e),
                    source: None,
                })?
        } else {
            reader_builder.build().map_err(|e| ApiaryError::Storage {
                message: format!("Failed to build Parquet reader: {}", e),
                source: None,
            })?
        };

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| ApiaryError::Storage {
                message: format!("Failed to read Parquet batch: {}", e),
                source: None,
            })?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Read multiple cells and concatenate them into a single list of RecordBatches.
    pub async fn read_cells(
        &self,
        cells: &[&CellMetadata],
        projection: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for cell in cells {
            let batches = self.read_cell(cell, projection).await?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    /// Read cells and merge them into a single RecordBatch.
    pub async fn read_cells_merged(
        &self,
        cells: &[&CellMetadata],
        projection: Option<&[String]>,
    ) -> Result<Option<RecordBatch>> {
        let batches = self.read_cells(cells, projection).await?;

        if batches.is_empty() {
            return Ok(None);
        }

        let schema = batches[0].schema();
        concat_batches(&schema, &batches)
    }
}

/// Concatenate multiple RecordBatches with the same schema into one.
fn concat_batches(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<Option<RecordBatch>> {
    if batches.is_empty() {
        return Ok(None);
    }

    let merged = arrow::compute::concat_batches(schema, batches).map_err(|e| {
        ApiaryError::Internal {
            message: format!("Failed to concatenate record batches: {}", e),
        }
    })?;

    Ok(Some(merged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_writer::CellWriter;
    use apiary_core::{CellSizingPolicy, FieldDef, FrameSchema};
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field};
    use crate::local::LocalBackend;

    async fn make_storage() -> (Arc<dyn StorageBackend>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_path_buf()).await.unwrap();
        (Arc::new(backend), dir)
    }

    fn test_schema() -> FrameSchema {
        FrameSchema {
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
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_read_cell_roundtrip() {
        let (storage, _dir) = make_storage().await;
        let frame_path = "test_hive/test_box/test_frame";
        let schema = test_schema();
        let sizing = CellSizingPolicy::new(256 * 1024 * 1024, 512 * 1024 * 1024, 16 * 1024 * 1024);

        let writer = CellWriter::new(
            storage.clone(),
            frame_path.into(),
            schema,
            vec![],
            sizing,
        );

        let batch = test_batch();
        let cells = writer.write(&batch).await.unwrap();
        assert_eq!(cells.len(), 1);

        let reader = CellReader::new(storage, frame_path.into());
        let read_batches = reader.read_cell(&cells[0], None).await.unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_read_cell_with_projection() {
        let (storage, _dir) = make_storage().await;
        let frame_path = "test_hive/test_box/test_frame";
        let schema = test_schema();
        let sizing = CellSizingPolicy::new(256 * 1024 * 1024, 512 * 1024 * 1024, 16 * 1024 * 1024);

        let writer = CellWriter::new(
            storage.clone(),
            frame_path.into(),
            schema,
            vec![],
            sizing,
        );

        let batch = test_batch();
        let cells = writer.write(&batch).await.unwrap();

        let reader = CellReader::new(storage, frame_path.into());
        let projection = vec!["name".to_string()];
        let read_batches = reader
            .read_cell(&cells[0], Some(&projection))
            .await
            .unwrap();

        assert_eq!(read_batches[0].num_columns(), 1);
        assert_eq!(read_batches[0].schema().field(0).name(), "name");
    }
}
