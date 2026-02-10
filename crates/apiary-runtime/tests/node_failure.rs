//! Integration test: Node failure handling.
//!
//! Verifies that the abandonment tracker and world view handle failures correctly.

use apiary_core::config::NodeConfig;
use apiary_core::types::TaskId;
use apiary_runtime::behavioral::{AbandonmentDecision, AbandonmentTracker};
use apiary_runtime::ApiaryNode;

#[tokio::test]
async fn test_abandonment_tracker_retry_then_abandon() {
    let tracker = AbandonmentTracker::new(3);
    let task_id = TaskId::generate();

    assert_eq!(tracker.record_failure(&task_id), AbandonmentDecision::Retry);
    assert_eq!(tracker.record_failure(&task_id), AbandonmentDecision::Retry);
    assert_eq!(
        tracker.record_failure(&task_id),
        AbandonmentDecision::Abandon
    );
}

#[tokio::test]
async fn test_abandonment_success_clears_count() {
    let tracker = AbandonmentTracker::new(3);
    let task_id = TaskId::generate();

    tracker.record_failure(&task_id);
    tracker.record_failure(&task_id);
    assert_eq!(tracker.get_count(&task_id), 2);

    tracker.record_success(&task_id);
    assert_eq!(tracker.get_count(&task_id), 0);

    // After reset, retry should work again
    assert_eq!(tracker.record_failure(&task_id), AbandonmentDecision::Retry);
}

#[tokio::test]
async fn test_node_colony_status_reports_temperature() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmp.path().display());
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = tmp.path().join("cache");

    let node = ApiaryNode::start(config).await.unwrap();

    let status = node.colony_status().await;
    assert!(status.temperature >= 0.0);
    assert!(status.temperature <= 1.0);
    assert!(!status.regulation.is_empty());
    assert_eq!(status.setpoint, 0.5);

    // Idle node should be cold or ideal
    assert!(
        status.regulation == "cold" || status.regulation == "ideal",
        "Idle node should be cold/ideal, got: {}",
        status.regulation
    );

    node.shutdown().await;
}

#[tokio::test]
async fn test_node_bee_status_reflects_pool() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmp.path().display());
    config.cores = 4;
    config.memory_per_bee = 32 * 1024 * 1024;
    config.cache_dir = tmp.path().join("cache");

    let node = ApiaryNode::start(config).await.unwrap();

    let bees = node.bee_status().await;
    assert_eq!(bees.len(), 4, "Should have 4 bees");
    for bee in &bees {
        assert_eq!(bee.state, "idle");
        assert_eq!(bee.memory_budget, 32 * 1024 * 1024);
        assert_eq!(bee.memory_used, 0);
    }

    node.shutdown().await;
}

#[tokio::test]
async fn test_node_swarm_status_solo() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmp.path().display());
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = tmp.path().join("cache");

    let node = ApiaryNode::start(config).await.unwrap();

    let swarm = node.swarm_status().await;
    assert_eq!(swarm.nodes.len(), 1, "Solo mode should see 1 node");
    assert_eq!(swarm.nodes[0].state, "alive");
    assert_eq!(swarm.total_bees, 2);

    node.shutdown().await;
}
