//! Integration test: Multi-node mode.
//!
//! Multiple nodes sharing the same storage backend can discover each other
//! and each can see the other via the world view.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use apiary_core::config::NodeConfig;
use apiary_runtime::ApiaryNode;

/// Start a test node with specific node-id suffix for identification.
async fn start_node(tmpdir: &std::path::Path, cache_suffix: &str) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmpdir.display());
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = tmpdir.join(format!("cache_{cache_suffix}"));
    config.heartbeat_interval = std::time::Duration::from_millis(500);
    config.dead_threshold = std::time::Duration::from_secs(5);
    ApiaryNode::start(config).await.expect("Node should start")
}

#[tokio::test]
async fn test_two_nodes_discover_each_other() {
    let tmp = tempfile::TempDir::new().unwrap();

    // Start two nodes sharing the same storage
    let node1 = start_node(tmp.path(), "node1").await;
    let node2 = start_node(tmp.path(), "node2").await;

    // Allow heartbeats and world view to propagate
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Both nodes should see each other
    let swarm1 = node1.swarm_status().await;
    let swarm2 = node2.swarm_status().await;

    assert!(
        swarm1.nodes.len() >= 2,
        "Node 1 should see at least 2 nodes, saw {}",
        swarm1.nodes.len()
    );
    assert!(
        swarm2.nodes.len() >= 2,
        "Node 2 should see at least 2 nodes, saw {}",
        swarm2.nodes.len()
    );

    // Both should see the other as alive
    for node_info in &swarm1.nodes {
        assert_eq!(
            node_info.state, "alive",
            "All nodes should be alive, got: {}",
            node_info.state
        );
    }

    node1.shutdown().await;
    node2.shutdown().await;
}

#[tokio::test]
async fn test_multi_node_shared_data() {
    let tmp = tempfile::TempDir::new().unwrap();

    let node1 = start_node(tmp.path(), "writer").await;

    // Write data on node 1
    node1.registry.create_hive("shared").await.unwrap();
    node1.registry.create_box("shared", "data").await.unwrap();

    let schema_json = serde_json::json!({
        "fields": [
            {"name": "id", "data_type": "Int64"},
            {"name": "value", "data_type": "Float64"}
        ]
    });
    node1
        .registry
        .create_frame("shared", "data", "metrics", schema_json, vec![])
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
        ],
    )
    .unwrap();

    node1
        .write_to_frame("shared", "data", "metrics", &batch)
        .await
        .unwrap();

    // Start node 2 (same storage)
    let node2 = start_node(tmp.path(), "reader").await;

    // Node 2 should be able to query data written by node 1
    let batches = node2
        .sql("SELECT COUNT(*) as cnt FROM shared.data.metrics")
        .await
        .unwrap();

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5, "Node 2 should see all 5 rows from node 1");

    node1.shutdown().await;
    node2.shutdown().await;
}

#[tokio::test]
async fn test_graceful_departure_detected() {
    let tmp = tempfile::TempDir::new().unwrap();

    let node1 = start_node(tmp.path(), "stayer").await;
    let node2 = start_node(tmp.path(), "leaver").await;

    // Let heartbeats propagate
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let swarm_before = node1.swarm_status().await;
    let nodes_before = swarm_before.nodes.len();
    assert!(nodes_before >= 2);

    // Gracefully shut down node 2
    node2.shutdown().await;

    // Allow world view to update
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Node 1 should eventually see fewer alive nodes
    let swarm_after = node1.swarm_status().await;
    let alive_count = swarm_after
        .nodes
        .iter()
        .filter(|n| n.state == "alive")
        .count();
    assert!(
        alive_count < nodes_before,
        "After graceful departure, alive count should decrease"
    );

    node1.shutdown().await;
}
