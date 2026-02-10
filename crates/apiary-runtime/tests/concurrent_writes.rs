//! Integration test: Concurrent writes.
//!
//! Two writers → both succeed via conditional retry (optimistic concurrency).

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use apiary_core::config::NodeConfig;
use apiary_runtime::ApiaryNode;

async fn start_test_node(tmpdir: &std::path::Path) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmpdir.display());
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = tmpdir.join("cache");
    ApiaryNode::start(config).await.expect("Node should start")
}

#[tokio::test]
async fn test_sequential_writes_both_succeed() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_test_node(tmp.path()).await;

    node.registry.create_hive("test").await.unwrap();
    node.registry.create_box("test", "data").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [
            {"name": "id", "data_type": "Int64"},
            {"name": "value", "data_type": "Float64"}
        ]
    });
    node.registry
        .create_frame("test", "data", "metrics", schema_json, vec![])
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    // First write
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();

    let r1 = node
        .write_to_frame("test", "data", "metrics", &batch1)
        .await
        .unwrap();
    assert_eq!(r1.rows_written, 3);

    // Second write (append)
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Float64Array::from(vec![4.0, 5.0])),
        ],
    )
    .unwrap();

    let r2 = node
        .write_to_frame("test", "data", "metrics", &batch2)
        .await
        .unwrap();
    assert_eq!(r2.rows_written, 2);

    // Both writes should be visible
    let batches = node
        .sql("SELECT COUNT(*) as cnt FROM test.data.metrics")
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5, "Both writes should produce 5 total rows");

    node.shutdown().await;
}

#[tokio::test]
async fn test_multiple_writes_accumulate() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_test_node(tmp.path()).await;

    node.registry.create_hive("test").await.unwrap();
    node.registry.create_box("test", "data").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [{"name": "x", "data_type": "Int64"}]
    });
    node.registry
        .create_frame("test", "data", "accum", schema_json, vec![])
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

    // Perform 5 sequential writes
    for i in 0..5 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![i * 10, i * 10 + 1]))],
        )
        .unwrap();
        node.write_to_frame("test", "data", "accum", &batch)
            .await
            .unwrap();
    }

    // All 10 rows should be visible
    let batches = node
        .sql("SELECT COUNT(*) as cnt FROM test.data.accum")
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10);

    node.shutdown().await;
}

#[tokio::test]
async fn test_write_to_separate_frames() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_test_node(tmp.path()).await;

    node.registry.create_hive("test").await.unwrap();
    node.registry.create_box("test", "data").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [{"name": "x", "data_type": "Int64"}]
    });
    node.registry
        .create_frame("test", "data", "frame_a", schema_json.clone(), vec![])
        .await
        .unwrap();
    node.registry
        .create_frame("test", "data", "frame_b", schema_json, vec![])
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let batch_b =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20]))]).unwrap();

    // Write to both frames
    node.write_to_frame("test", "data", "frame_a", &batch_a)
        .await
        .unwrap();
    node.write_to_frame("test", "data", "frame_b", &batch_b)
        .await
        .unwrap();

    // Each frame should have independent data
    let count_a = node
        .sql("SELECT COUNT(*) as cnt FROM test.data.frame_a")
        .await
        .unwrap();
    let count_b = node
        .sql("SELECT COUNT(*) as cnt FROM test.data.frame_b")
        .await
        .unwrap();

    assert_eq!(
        count_a[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        count_b[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    node.shutdown().await;
}
