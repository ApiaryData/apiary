//! Integration test: a Node's Bees.
//!
//! A query is a Forager's Patch, run under that Bee's share of the Node's memory.
//! Memory a Bee takes goes back when the Patch ends, a busy Node reads warmer than
//! an idle one, and the temperature bands classify as documented.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;

use apiary_core::config::NodeConfig;
use apiary_runtime::{ApiaryNode, TemperatureRegulation};

async fn start(tmp: &std::path::Path, cores: usize) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmp.display());
    config.cores = cores;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = tmp.join("cache");
    config.deposit_interval = Duration::from_secs(3600);
    ApiaryNode::start(config).await.expect("the Node starts")
}

async fn make_frame(node: &ApiaryNode) {
    node.registry.create_hive("farm").await.unwrap();
    node.registry.create_box("farm", "field").await.unwrap();
    node.registry
        .create_frame(
            "farm",
            "field",
            "r",
            serde_json::json!({"n": "int64"}),
            vec![],
        )
        .await
        .unwrap();
    let batch = RecordBatch::try_from_iter(vec![(
        "n",
        Arc::new(Int64Array::from_iter_values(0..1000)) as arrow::array::ArrayRef,
    )])
    .unwrap();
    node.ingest("farm", "field", "r", &batch).await.unwrap();
}

#[tokio::test]
async fn an_idle_node_is_cold_and_each_bee_has_a_share_of_memory() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start(tmp.path(), 4).await;
    let status = node.colony_status().await;
    assert!(status.temperature < 0.3, "{}", status.temperature);
    assert_eq!(status.regulation, "cold");

    let bees = node.bee_status().await;
    assert_eq!(bees.len(), 4);
    for bee in &bees {
        assert_eq!(bee.memory_used, 0);
        // Until a query is split into one Patch per partition, a Bee's Patch is a
        // whole query over four partitions, so its share is four partitions' worth.
        assert_eq!(bee.memory_budget, 4 * 64 * 1024 * 1024);
    }
    node.shutdown().await;
}

#[tokio::test]
async fn a_query_gives_its_memory_back_and_the_node_cools_again() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = Arc::new(start(tmp.path(), 2).await);
    make_frame(&node).await;

    // Several queries at once: they queue for the two Foragers and all finish.
    let mut handles = Vec::new();
    for i in 0..8 {
        let node = Arc::clone(&node);
        handles.push(tokio::spawn(async move {
            node.sql(&format!("SELECT count(n), {i} FROM farm.field.r"))
                .await
        }));
    }
    for handle in handles {
        let batches = handle.await.unwrap().unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1000);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    for bee in node.bee_status().await {
        assert_eq!(bee.memory_used, 0, "{} gave its memory back", bee.bee_id);
        assert_eq!(bee.state, "idle");
    }
    assert!(node.colony_status().await.temperature < 0.3);
    // The Bees that answered have aged: they finished Patches.
    assert!(node.bee_status().await.iter().any(|b| b.age > 1));
    node.shutdown().await;
}

#[test]
fn the_temperature_bands_classify_as_documented() {
    use TemperatureRegulation as R;
    assert_eq!(R::of(0.0), R::Cold);
    assert_eq!(R::of(0.29), R::Cold);
    assert_eq!(R::of(0.3), R::Ideal);
    assert_eq!(R::of(0.7), R::Ideal);
    assert_eq!(R::of(0.71), R::Warm);
    assert_eq!(R::of(0.85), R::Warm);
    assert_eq!(R::of(0.86), R::Hot);
    assert_eq!(R::of(0.95), R::Hot);
    assert_eq!(R::of(0.96), R::Critical);
    assert_eq!(R::of(1.0), R::Critical);
}
