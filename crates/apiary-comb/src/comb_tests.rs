//! Tests for [`Comb`] against real Delta tables on the local filesystem.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, UInt8Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use apiary_core::{FieldDef, FrameSchema};

use crate::comb::{CellState, Comb, STATE_TAG};

fn field(name: &str, ty: &str, nullable: bool) -> FieldDef {
    FieldDef {
        name: name.into(),
        data_type: ty.into(),
        nullable,
    }
}

fn sensor_schema() -> FrameSchema {
    FrameSchema {
        fields: vec![
            field("region", "string", false),
            field("sensor", "int64", false),
            field("temp", "float64", true),
        ],
    }
}

fn sensor_batch(region: &str, sensors: Vec<i64>) -> RecordBatch {
    let n = sensors.len();
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec![region; n])) as ArrayRef,
        ),
        ("sensor", Arc::new(Int64Array::from(sensors)) as ArrayRef),
        (
            "temp",
            Arc::new(Float64Array::from(vec![20.5; n])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn comb() -> (TempDir, Comb) {
    let dir = TempDir::new().unwrap();
    let comb = Comb::from_local_path(dir.path()).unwrap();
    (dir, comb)
}

const CELL: u64 = 64 * 1024 * 1024;

#[tokio::test]
async fn missing_table_opens_as_none_and_create_is_idempotent() {
    let (_dir, comb) = comb();
    assert!(
        comb.open_frame_table("h", "b", "f")
            .await
            .unwrap()
            .is_none(),
        "a frame with no table yet must open as None"
    );

    let first = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &[])
        .await
        .unwrap();
    let again = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &[])
        .await
        .unwrap();
    assert_eq!(first.version(), again.version());
    assert!(
        comb.open_frame_table("h", "b", "f")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn append_then_read_round_trips() {
    let (_dir, comb) = comb();
    let table = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &[])
        .await
        .unwrap();

    let committed = comb
        .append(
            &table,
            &sensor_batch("north", vec![1, 2, 3]),
            CELL,
            CellState::Nectar,
        )
        .await
        .unwrap();
    assert_eq!(committed.rows, 3);
    assert_eq!(committed.cells, 1);
    assert!(committed.bytes > 0);

    let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
    let batch = comb.read(&table, None).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.schema().field(0).name(), "region");
    // Plain Utf8, not Utf8View: results are consumed outside DataFusion.
    assert_eq!(*batch.schema().field(0).data_type(), DataType::Utf8);
}

#[tokio::test]
async fn partitioned_write_prunes_on_read_and_lays_out_directories() {
    let (dir, comb) = comb();
    let table = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &["region".to_string()])
        .await
        .unwrap();

    let mixed = arrow::compute::concat_batches(
        &sensor_batch("north", vec![1]).schema(),
        &[
            sensor_batch("north", vec![1, 2]),
            sensor_batch("south", vec![3]),
        ],
    )
    .unwrap();
    let committed = comb
        .append(&table, &mixed, CELL, CellState::Nectar)
        .await
        .unwrap();
    assert_eq!(committed.cells, 2, "one cell per partition");

    let frame_dir = dir.path().join("h").join("b").join("f");
    assert!(frame_dir.join("region=north").is_dir());
    assert!(frame_dir.join("region=south").is_dir());

    let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
    let mut filter = HashMap::new();
    filter.insert("region".to_string(), "south".to_string());
    let south = comb.read(&table, Some(&filter)).await.unwrap().unwrap();
    assert_eq!(south.num_rows(), 1);

    filter.insert("region".to_string(), "west".to_string());
    assert!(comb.read(&table, Some(&filter)).await.unwrap().is_none());
}

#[tokio::test]
async fn cells_are_tagged_with_their_state_in_the_delta_log() {
    let (dir, comb) = comb();
    let table = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &[])
        .await
        .unwrap();
    comb.append(&table, &sensor_batch("n", vec![1]), CELL, CellState::Nectar)
        .await
        .unwrap();

    let log = dir
        .path()
        .join("h/b/f/_delta_log/00000000000000000001.json");
    let text = std::fs::read_to_string(&log).unwrap();
    assert!(
        text.contains(&format!("\"{STATE_TAG}\":\"nectar\"")),
        "add action should carry the state tag: {text}"
    );
    assert_eq!(CellState::parse("capped"), Some(CellState::Capped));
    assert_eq!(CellState::parse("honey"), None);
}

#[tokio::test]
async fn overwrite_replaces_everything_in_one_commit() {
    let (_dir, comb) = comb();
    let table = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &[])
        .await
        .unwrap();
    let first = comb
        .append(
            &table,
            &sensor_batch("n", vec![1, 2, 3, 4]),
            CELL,
            CellState::Nectar,
        )
        .await
        .unwrap();

    let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
    let second = comb
        .overwrite(&table, &sensor_batch("n", vec![9]), CELL, CellState::Nectar)
        .await
        .unwrap();
    assert_eq!(second.version, first.version + 1);

    let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
    let batch = comb.read(&table, None).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn empty_batch_commits_nothing() {
    let (_dir, comb) = comb();
    let table = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &[])
        .await
        .unwrap();
    let before = table.version();

    let committed = comb
        .append(&table, &sensor_batch("n", vec![]), CELL, CellState::Nectar)
        .await
        .unwrap();
    assert_eq!(committed.cells, 0);
    assert_eq!(committed.rows, 0);
    assert_eq!(Some(committed.version), before);
}

#[tokio::test]
async fn schema_violations_are_reported() {
    let (_dir, comb) = comb();
    let table = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &[])
        .await
        .unwrap();
    let missing_region = RecordBatch::try_from_iter(vec![(
        "sensor",
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )])
    .unwrap();
    let err = comb
        .append(&table, &missing_region, CELL, CellState::Nectar)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("region"), "{err}");
}

#[tokio::test]
async fn a_frame_needs_at_least_one_column() {
    let (_dir, comb) = comb();
    let err = comb
        .create_frame_table("h", "b", "f", &FrameSchema { fields: vec![] }, &[])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("at least one column"), "{err}");
}

#[tokio::test]
async fn partition_column_must_exist_in_schema() {
    let (_dir, comb) = comb();
    let err = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &["nope".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("nope"), "{err}");
}

#[tokio::test]
async fn column_types_round_trip() {
    let (_dir, comb) = comb();
    let schema = FrameSchema {
        fields: vec![
            field("ts", "datetime", true),
            field("small", "uint8", true),
            field("big", "uint64", true),
            field("d", "date", true),
            field("flag", "boolean", true),
            field("blob", "binary", true),
            field("n32", "int32", true),
        ],
    };
    let table = comb
        .create_frame_table("h", "b", "types", &schema, &[])
        .await
        .unwrap();

    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_700_000_000_000_000i64,
            ])) as ArrayRef,
        ),
        ("small", Arc::new(UInt8Array::from(vec![200u8])) as ArrayRef),
        (
            "big",
            Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
        ),
        ("d", Arc::new(Date32Array::from(vec![19_000])) as ArrayRef),
        ("flag", Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
        (
            "blob",
            Arc::new(BinaryArray::from_vec(vec![b"abc"])) as ArrayRef,
        ),
        ("n32", Arc::new(Int32Array::from(vec![-7])) as ArrayRef),
    ])
    .unwrap();
    comb.append(&table, &batch, CELL, CellState::Nectar)
        .await
        .unwrap();

    let table = comb
        .open_frame_table("h", "b", "types")
        .await
        .unwrap()
        .unwrap();
    let out = comb.read(&table, None).await.unwrap().unwrap();

    // Timestamps stay naive microseconds, as in V1.
    assert_eq!(
        *out.column(0).data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    let ts = out
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts.value(0), 1_700_000_000_000_000);

    // Unsigned integers are stored as the next wider signed type, values intact.
    let small = out.column(1).as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!(small.value(0), 200);
    let big = out
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(big.value(0), i128::from(u64::MAX));

    assert_eq!(out.column(3).len(), 1);
    assert_eq!(
        out.column(6)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        -7
    );
}

#[tokio::test]
async fn concurrent_appends_from_many_writers_all_commit() {
    let (_dir, comb) = comb();
    comb.create_frame_table("h", "b", "f", &sensor_schema(), &[])
        .await
        .unwrap();

    let writers: i64 = 8;
    let per_writer: i64 = 5;
    let comb = Arc::new(comb);
    let mut handles = Vec::new();
    for w in 0..writers {
        let comb = Arc::clone(&comb);
        handles.push(tokio::spawn(async move {
            for i in 0..per_writer {
                // Each append works from its own view of the table, as a
                // separate Node would.
                let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
                let id = w * 100 + i;
                comb.append(
                    &table,
                    &sensor_batch("n", vec![id]),
                    CELL,
                    CellState::Nectar,
                )
                .await
                .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
    let batch = comb.read(&table, None).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), (writers * per_writer) as usize);
    // create is version 0, then one version per append
    assert_eq!(table.version(), Some((writers * per_writer) as u64));
}

#[test]
fn storage_uri_parsing() {
    let dir = TempDir::new().unwrap();

    let local = Comb::from_storage_uri(&format!("local://{}", dir.path().display())).unwrap();
    assert_eq!(local.root().scheme(), "file");
    assert!(local.storage_options().is_empty());
    let url = local.frame_url("hive", "box", "frame").unwrap();
    assert!(url.as_str().ends_with("/hive/box/frame/"), "{url}");

    let bare = Comb::from_storage_uri(&dir.path().display().to_string()).unwrap();
    assert_eq!(bare.root(), local.root());

    let s3 =
        Comb::from_storage_uri("s3://bucket/some/prefix?region=eu-west-1&endpoint=http://m:9000")
            .unwrap();
    assert_eq!(s3.root().as_str(), "s3://bucket/some/prefix/");
    let opts = s3.storage_options();
    assert_eq!(
        opts.get("aws_conditional_put").map(String::as_str),
        Some("etag")
    );
    assert_eq!(
        opts.get("AWS_REGION").map(String::as_str),
        Some("eu-west-1")
    );
    assert_eq!(
        opts.get("AWS_ENDPOINT_URL").map(String::as_str),
        Some("http://m:9000")
    );
    assert_eq!(opts.get("AWS_ALLOW_HTTP").map(String::as_str), Some("true"));
    assert_eq!(
        s3.frame_url("h", "b", "f").unwrap().as_str(),
        "s3://bucket/some/prefix/h/b/f/"
    );

    assert!(Comb::from_storage_uri("s3://").is_err());
}

#[tokio::test]
async fn frame_stats_and_registered_tables_see_the_data() {
    let (_dir, comb) = comb();
    let table = comb
        .create_frame_table("h", "b", "f", &sensor_schema(), &["region".to_string()])
        .await
        .unwrap();
    assert_eq!(
        comb.frame_stats(&table).unwrap(),
        crate::comb::FrameStats::default()
    );

    let mixed = arrow::compute::concat_batches(
        &sensor_batch("north", vec![1]).schema(),
        &[
            sensor_batch("north", vec![1, 2]),
            sensor_batch("south", vec![3]),
        ],
    )
    .unwrap();
    comb.append(&table, &mixed, CELL, CellState::Nectar)
        .await
        .unwrap();

    let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
    let stats = comb.frame_stats(&table).unwrap();
    assert_eq!(stats.cells, 2);
    assert_eq!(stats.rows, 3);
    assert!(stats.bytes > 0);

    let ctx = datafusion::prelude::SessionContext::new();
    comb.register_table(&ctx, "sensors", &table).await.unwrap();
    let rows = ctx
        .sql("SELECT count(*) AS n FROM sensors WHERE region = 'north'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2);
}
