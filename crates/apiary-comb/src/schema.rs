//! Frame schemas: conversion between Apiary's type strings and Arrow, and
//! conforming incoming batches to what a Frame's Delta table can store.
//!
//! A Frame's schema is recorded in the registry as type strings (`"int64"`,
//! `"datetime"`, ...). [`frame_schema_to_arrow`] gives the schema users declared;
//! [`delta_schema`] gives the schema the Delta table actually stores, which
//! differs only where Delta has no equivalent type.

use std::sync::Arc;

use arrow::array::{ArrayRef, new_null_array};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use tracing::warn;

use apiary_core::{ApiaryError, FieldDef, FrameSchema, Result};

/// Convert a type string to an Arrow type. Unknown strings become `Utf8`.
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
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "date" => DataType::Date32,
        "binary" | "bytes" => DataType::Binary,
        _ => DataType::Utf8, // Default to string
    }
}

/// The Arrow schema a Frame's users declared.
pub fn frame_schema_to_arrow(schema: &FrameSchema) -> Schema {
    let fields: Vec<Field> = schema
        .fields
        .iter()
        .map(|f| Field::new(&f.name, type_string_to_arrow(&f.data_type), f.nullable))
        .collect();
    Schema::new(fields)
}

/// Convert an Arrow schema to a [`FrameSchema`].
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

/// Convert an Arrow type to a type string.
pub fn arrow_type_to_string(dt: &DataType) -> String {
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

/// The type Delta Lake stores for a declared type.
///
/// Delta has no unsigned integers and no half floats, so unsigned integers
/// are stored as the next wider signed type (`uint64` as `decimal(20,0)`) and
/// `float16` as `float32`. Values are preserved; only the type seen by queries
/// differs from the declared one.
pub fn delta_type(dt: &DataType) -> DataType {
    match dt {
        DataType::UInt8 => DataType::Int16,
        DataType::UInt16 => DataType::Int32,
        DataType::UInt32 => DataType::Int64,
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::Float16 => DataType::Float32,
        DataType::Date64 => DataType::Date32,
        other => other.clone(),
    }
}

/// The Arrow schema of a Frame's Delta table.
pub fn delta_schema(schema: &FrameSchema) -> SchemaRef {
    let fields: Vec<Field> = frame_schema_to_arrow(schema)
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), delta_type(f.data_type()), f.is_nullable()))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Conform an incoming batch to a Frame's Delta schema.
///
/// Rules (from the V1 storage engine, kept):
/// - Extra columns are dropped with a warning.
/// - A missing nullable column is filled with nulls.
/// - A missing non-nullable column is an error.
/// - Columns are cast to the Frame's type; a value that does not fit is an error.
/// - Partition columns may not contain nulls or path separators.
///
/// The result has exactly the target schema's columns, in its order.
pub fn conform_batch(
    batch: &RecordBatch,
    target: &SchemaRef,
    partition_by: &[String],
) -> Result<RecordBatch> {
    let source = batch.schema();

    for field in source.fields() {
        if target.index_of(field.name()).is_err() {
            warn!(column = %field.name(), "Extra column in write data will be dropped");
        }
    }

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let column = match source.index_of(field.name()) {
            Ok(idx) => {
                let array = batch.column(idx);
                if array.data_type() == field.data_type() {
                    Arc::clone(array)
                } else {
                    cast_with_options(array, field.data_type(), &options).map_err(|e| {
                        ApiaryError::Schema {
                            message: format!(
                                "Column '{}' ({}) cannot be written as {}: {e}",
                                field.name(),
                                array.data_type(),
                                field.data_type()
                            ),
                        }
                    })?
                }
            }
            Err(_) if field.is_nullable() => new_null_array(field.data_type(), batch.num_rows()),
            Err(_) => {
                return Err(ApiaryError::Schema {
                    message: format!(
                        "Missing non-nullable column '{}' in write data",
                        field.name()
                    ),
                });
            }
        };
        if !field.is_nullable() && column.null_count() > 0 {
            return Err(ApiaryError::Schema {
                message: format!(
                    "Non-nullable column '{}' contains null values",
                    field.name()
                ),
            });
        }
        columns.push(column);
    }

    let conformed =
        RecordBatch::try_new(Arc::clone(target), columns).map_err(|e| ApiaryError::Schema {
            message: format!("Write data does not match the frame schema: {e}"),
        })?;

    validate_partition_columns(&conformed, partition_by)?;
    Ok(conformed)
}

/// Partition values become directory names, so reject nulls and anything that
/// could escape the table directory.
fn validate_partition_columns(batch: &RecordBatch, partition_by: &[String]) -> Result<()> {
    for part_col in partition_by {
        let Ok(idx) = batch.schema().index_of(part_col) else {
            continue;
        };
        let column = batch.column(idx);
        if column.null_count() > 0 {
            return Err(ApiaryError::Schema {
                message: format!("Partition column '{part_col}' contains null values"),
            });
        }
        let formatter = arrow::util::display::ArrayFormatter::try_new(
            column.as_ref(),
            &arrow::util::display::FormatOptions::default(),
        )
        .map_err(|e| ApiaryError::Schema {
            message: format!("Cannot read partition column '{part_col}': {e}"),
        })?;
        for row in 0..batch.num_rows() {
            let value = formatter.value(row).to_string();
            if value.contains("..")
                || value.contains('/')
                || value.contains('\\')
                || value.contains('\0')
            {
                return Err(ApiaryError::Schema {
                    message: format!(
                        "Partition column '{part_col}' contains invalid characters (path separators or '..'): '{value}'"
                    ),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray, UInt8Array};

    fn field(name: &str, ty: &str, nullable: bool) -> FieldDef {
        FieldDef {
            name: name.into(),
            data_type: ty.into(),
            nullable,
        }
    }

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
                field("region", "string", false),
                field("temp", "float64", true),
            ],
        };
        let arrow_schema = frame_schema_to_arrow(&schema);
        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "region");
        assert_eq!(*arrow_schema.field(0).data_type(), DataType::Utf8);
    }

    #[test]
    fn delta_schema_widens_unsigned_and_half_floats() {
        let schema = FrameSchema {
            fields: vec![
                field("a", "uint8", true),
                field("b", "uint16", true),
                field("c", "uint32", true),
                field("d", "uint64", true),
                field("e", "float16", true),
                field("f", "int64", true),
            ],
        };
        let delta = delta_schema(&schema);
        let types: Vec<_> = delta
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        assert_eq!(
            types,
            vec![
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Decimal128(20, 0),
                DataType::Float32,
                DataType::Int64,
            ]
        );
    }

    fn target() -> SchemaRef {
        delta_schema(&FrameSchema {
            fields: vec![
                field("region", "string", false),
                field("temp", "float64", true),
                field("count", "uint8", true),
            ],
        })
    }

    fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(cols).unwrap()
    }

    #[test]
    fn conform_casts_reorders_and_drops_extras() {
        let b = batch(vec![
            (
                "count",
                Arc::new(UInt8Array::from(vec![1u8, 2])) as ArrayRef,
            ),
            ("junk", Arc::new(Int32Array::from(vec![9, 9])) as ArrayRef),
            (
                "region",
                Arc::new(StringArray::from(vec!["n", "s"])) as ArrayRef,
            ),
        ]);
        let out = conform_batch(&b, &target(), &[]).unwrap();
        assert_eq!(out.schema(), target());
        assert_eq!(out.num_rows(), 2);
        // missing nullable column is null-filled
        assert_eq!(out.column(1).null_count(), 2);
        // uint8 input was cast to the stored int16
        assert_eq!(*out.column(2).data_type(), DataType::Int16);
    }

    #[test]
    fn conform_rejects_missing_non_nullable_column() {
        let b = batch(vec![(
            "temp",
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        )]);
        let err = conform_batch(&b, &target(), &[]).unwrap_err();
        assert!(err.to_string().contains("region"), "{err}");
    }

    #[test]
    fn conform_rejects_values_that_do_not_fit() {
        let schema = delta_schema(&FrameSchema {
            fields: vec![field("n", "int8", false)],
        });
        let b = batch(vec![(
            "n",
            Arc::new(Int64Array::from(vec![1_000])) as ArrayRef,
        )]);
        assert!(conform_batch(&b, &schema, &[]).is_err());
    }

    #[test]
    fn conform_rejects_null_and_path_like_partition_values() {
        let schema = delta_schema(&FrameSchema {
            fields: vec![field("region", "string", true)],
        });
        let nulls = batch(vec![(
            "region",
            Arc::new(StringArray::from(vec![Some("n"), None])) as ArrayRef,
        )]);
        assert!(conform_batch(&nulls, &schema, &["region".into()]).is_err());

        let traversal = batch(vec![(
            "region",
            Arc::new(StringArray::from(vec!["../etc"])) as ArrayRef,
        )]);
        assert!(conform_batch(&traversal, &schema, &["region".into()]).is_err());

        let ok = batch(vec![(
            "region",
            Arc::new(StringArray::from(vec!["north"])) as ArrayRef,
        )]);
        assert!(conform_batch(&ok, &schema, &["region".into()]).is_ok());
    }
}
