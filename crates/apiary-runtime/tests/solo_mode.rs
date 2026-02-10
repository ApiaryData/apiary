//! Integration test: Solo mode end-to-end flow.
//!
//! start → create hive/box/frame → write data → query → verify results.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use apiary_core::config::NodeConfig;
use apiary_runtime::ApiaryNode;

/// Helper to create a test node with a temporary directory.
async fn start_test_node(tmpdir: &std::path::Path) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmpdir.display());
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024; // 64 MB per bee
    config.cache_dir = tmpdir.join("cache");
    ApiaryNode::start(config).await.expect("Node should start")
}

#[tokio::test]
async fn test_solo_create_write_query_verify() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_test_node(tmp.path()).await;

    // Create namespace
    node.registry.create_hive("analytics").await.unwrap();
    node.registry
        .create_box("analytics", "sensors")
        .await
        .unwrap();

    let schema_json = serde_json::json!({
        "fields": [
            {"name": "region", "data_type": "Utf8"},
            {"name": "temp", "data_type": "Float64"}
        ]
    });
    node.registry
        .create_frame("analytics", "sensors", "temperature", schema_json, vec![])
        .await
        .unwrap();

    // Write data
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("temp", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["north", "south", "north", "south"])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 12.0, 22.0])),
        ],
    )
    .unwrap();

    let result = node
        .write_to_frame("analytics", "sensors", "temperature", &batch)
        .await
        .unwrap();
    assert_eq!(result.rows_written, 4);
    assert!(result.cells_written >= 1);

    // Query via SQL
    let batches = node
        .sql("SELECT region, AVG(temp) as avg_temp FROM analytics.sensors.temperature GROUP BY region ORDER BY region")
        .await
        .unwrap();

    assert!(!batches.is_empty());
    let result_batch = &batches[0];
    assert_eq!(result_batch.num_rows(), 2);

    // Verify values
    let regions = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(regions.value(0), "north");
    assert_eq!(regions.value(1), "south");

    let avgs = result_batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((avgs.value(0) - 11.0).abs() < 0.01); // avg(10, 12) = 11
    assert!((avgs.value(1) - 21.0).abs() < 0.01); // avg(20, 22) = 21

    node.shutdown().await;
}

#[tokio::test]
async fn test_solo_read_from_frame() {
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
        .create_frame("test", "data", "readings", schema_json, vec![])
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
        ],
    )
    .unwrap();

    node.write_to_frame("test", "data", "readings", &batch)
        .await
        .unwrap();

    // Read back
    let data = node
        .read_from_frame("test", "data", "readings", None)
        .await
        .unwrap();
    assert!(data.is_some());
    let read_batch = data.unwrap();
    assert_eq!(read_batch.num_rows(), 3);

    node.shutdown().await;
}

#[tokio::test]
async fn test_solo_overwrite_frame() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_test_node(tmp.path()).await;

    node.registry.create_hive("test").await.unwrap();
    node.registry.create_box("test", "data").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [
            {"name": "x", "data_type": "Int64"}
        ]
    });
    node.registry
        .create_frame("test", "data", "values", schema_json, vec![])
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

    // Write initial data
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    node.write_to_frame("test", "data", "values", &batch1)
        .await
        .unwrap();

    // Overwrite with new data
    let batch2 =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20]))]).unwrap();
    let result = node
        .overwrite_frame("test", "data", "values", &batch2)
        .await
        .unwrap();
    assert_eq!(result.rows_written, 2);

    // Verify only new data exists
    let batches = node
        .sql("SELECT COUNT(*) as cnt FROM test.data.values")
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2);

    node.shutdown().await;
}

#[tokio::test]
async fn test_solo_sql_custom_commands() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_test_node(tmp.path()).await;

    node.registry.create_hive("myhive").await.unwrap();
    node.registry.create_box("myhive", "mybox").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [{"name": "id", "data_type": "Int64"}]
    });
    node.registry
        .create_frame("myhive", "mybox", "mytable", schema_json, vec![])
        .await
        .unwrap();

    // SHOW HIVES
    let batches = node.sql("SHOW HIVES").await.unwrap();
    assert!(!batches.is_empty());

    // USE HIVE
    let _ = node.sql("USE HIVE myhive").await.unwrap();

    // SHOW BOXES IN <hive>
    let batches = node.sql("SHOW BOXES IN myhive").await.unwrap();
    assert!(!batches.is_empty());

    node.shutdown().await;
}

#[tokio::test]
async fn test_solo_partitioned_data() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_test_node(tmp.path()).await;

    node.registry.create_hive("test").await.unwrap();
    node.registry.create_box("test", "data").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [
            {"name": "region", "data_type": "Utf8"},
            {"name": "value", "data_type": "Float64"}
        ]
    });
    node.registry
        .create_frame(
            "test",
            "data",
            "partitioned",
            schema_json,
            vec!["region".to_string()],
        )
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["east", "west", "east", "west"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
        ],
    )
    .unwrap();

    node.write_to_frame("test", "data", "partitioned", &batch)
        .await
        .unwrap();

    // Read with partition filter
    let mut filter = HashMap::new();
    filter.insert("region".to_string(), "east".to_string());
    let data = node
        .read_from_frame("test", "data", "partitioned", Some(&filter))
        .await
        .unwrap();
    assert!(data.is_some());
    let filtered = data.unwrap();
    assert_eq!(filtered.num_rows(), 2);

    node.shutdown().await;
}
