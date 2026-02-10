//! Chaos test: Multiple nodes with random shutdown during operations.
//!
//! Starts N nodes on the same shared storage, writes data, randomly shuts
//! down nodes, and verifies that the remaining nodes and data are consistent.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use apiary_core::config::NodeConfig;
use apiary_runtime::ApiaryNode;

async fn start_chaos_node(storage_path: &std::path::Path, idx: usize) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", storage_path.display());
    config.cores = 2;
    config.memory_per_bee = 32 * 1024 * 1024;
    config.cache_dir = storage_path.join(format!("cache_chaos_{idx}"));
    config.heartbeat_interval = std::time::Duration::from_millis(500);
    config.dead_threshold = std::time::Duration::from_secs(5);
    ApiaryNode::start(config)
        .await
        .expect("Chaos node should start")
}

#[tokio::test]
async fn test_chaos_data_survives_node_shutdown() {
    let tmp = tempfile::TempDir::new().unwrap();
    let storage_path = tmp.path();

    // Start 3 nodes
    let node0 = start_chaos_node(storage_path, 0).await;
    let node1 = start_chaos_node(storage_path, 1).await;
    let node2 = start_chaos_node(storage_path, 2).await;

    // Let them discover each other
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Write data via node0
    node0.registry.create_hive("chaos").await.unwrap();
    node0.registry.create_box("chaos", "data").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [{"name": "x", "data_type": "Int64"}]
    });
    node0
        .registry
        .create_frame("chaos", "data", "values", schema_json, vec![])
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        ]))],
    )
    .unwrap();

    node0
        .write_to_frame("chaos", "data", "values", &batch)
        .await
        .unwrap();

    // Kill node1 (simulate failure)
    node1.shutdown().await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // node0 and node2 should still be able to query the data
    let batches = node0
        .sql("SELECT COUNT(*) as cnt FROM chaos.data.values")
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10, "Data should survive node1 shutdown");

    let batches = node2
        .sql("SELECT COUNT(*) as cnt FROM chaos.data.values")
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10, "Node2 should also see all data");

    // Kill node0
    node0.shutdown().await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // node2 alone should still query successfully
    let batches = node2
        .sql("SELECT SUM(x) as total FROM chaos.data.values")
        .await
        .unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 55, "Sum 1..10 = 55, even with 2 nodes down");

    node2.shutdown().await;
}

#[tokio::test]
async fn test_chaos_new_node_sees_historical_data() {
    let tmp = tempfile::TempDir::new().unwrap();
    let storage_path = tmp.path();

    // Start node, write data, shut it down
    let node0 = start_chaos_node(storage_path, 0).await;

    node0.registry.create_hive("history").await.unwrap();
    node0.registry.create_box("history", "data").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [{"name": "v", "data_type": "Int64"}]
    });
    node0
        .registry
        .create_frame("history", "data", "log", schema_json, vec![])
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![100, 200, 300]))],
    )
    .unwrap();

    node0
        .write_to_frame("history", "data", "log", &batch)
        .await
        .unwrap();

    node0.shutdown().await;

    // Start a completely new node on the same storage
    let node1 = start_chaos_node(storage_path, 1).await;

    // New node should see historical data
    let batches = node1
        .sql("SELECT COUNT(*) as cnt FROM history.data.log")
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "New node should see data from dead node");

    node1.shutdown().await;
}
