//! Heartbeat writing, world view building, and node lifecycle.
//!
//! Each node writes a heartbeat file to object storage at a regular interval.
//! The [`WorldViewBuilder`] polls the heartbeat prefix to build a
//! [`WorldView`] — a snapshot of all known nodes and their status.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use apiary_core::error::ApiaryError;
use apiary_core::storage::StorageBackend;
use apiary_core::types::NodeId;
use apiary_core::Result;

use crate::bee::BeePool;

// ---------------------------------------------------------------------------
// Heartbeat data structures
// ---------------------------------------------------------------------------

/// Capacity information for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatCapacity {
    pub cores: usize,
    pub memory_total_bytes: u64,
    pub memory_per_bee: u64,
    pub target_cell_size: u64,
}

/// Current load information for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatLoad {
    pub bees_total: usize,
    pub bees_busy: usize,
    pub bees_idle: usize,
    pub memory_pressure: f64,
    pub queue_depth: usize,
    pub colony_temperature: f64,
}

/// A cache entry describing cached frame data on a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub frame: String,
    pub cells: usize,
    pub bytes: u64,
}

/// Cache summary for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatCache {
    pub size_bytes: u64,
    pub entries: Vec<CacheEntry>,
}

/// A heartbeat written by a node to object storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub node_id: String,
    pub timestamp: DateTime<Utc>,
    pub version: u64,
    pub capacity: HeartbeatCapacity,
    pub load: HeartbeatLoad,
    pub cache: HeartbeatCache,
}

// ---------------------------------------------------------------------------
// HeartbeatWriter
// ---------------------------------------------------------------------------

/// Writes the local node's heartbeat to storage at a regular interval.
pub struct HeartbeatWriter {
    storage: Arc<dyn StorageBackend>,
    node_id: NodeId,
    interval: Duration,
    version: AtomicU64,
    bee_pool: Arc<BeePool>,
    cores: usize,
    memory_total_bytes: u64,
    memory_per_bee: u64,
    target_cell_size: u64,
}

impl HeartbeatWriter {
    /// Create a new heartbeat writer from a node config and bee pool.
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        config: &apiary_core::config::NodeConfig,
        bee_pool: Arc<BeePool>,
    ) -> Self {
        Self {
            storage,
            node_id: config.node_id.clone(),
            interval: config.heartbeat_interval,
            version: AtomicU64::new(0),
            bee_pool,
            cores: config.cores,
            memory_total_bytes: config.memory_bytes,
            memory_per_bee: config.memory_per_bee,
            target_cell_size: config.target_cell_size,
        }
    }

    /// Collect a heartbeat snapshot from the current node state.
    pub async fn collect_heartbeat(&self) -> Heartbeat {
        let statuses = self.bee_pool.status().await;
        let bees_total = statuses.len();
        let bees_busy = statuses.iter().filter(|s| s.state != "idle").count();
        let bees_idle = bees_total - bees_busy;

        let total_memory_used: u64 = statuses.iter().map(|s| s.memory_used).sum();
        let total_budget: u64 = statuses.iter().map(|s| s.memory_budget).sum();
        let memory_pressure = if total_budget > 0 {
            total_memory_used as f64 / total_budget as f64
        } else {
            0.0
        };

        // Colony temperature is a heuristic: fraction of bees busy
        let colony_temperature = if bees_total > 0 {
            bees_busy as f64 / bees_total as f64
        } else {
            0.0
        };

        let version = self.version.fetch_add(1, Ordering::Relaxed) + 1;

        Heartbeat {
            node_id: self.node_id.as_str().to_string(),
            timestamp: Utc::now(),
            version,
            capacity: HeartbeatCapacity {
                cores: self.cores,
                memory_total_bytes: self.memory_total_bytes,
                memory_per_bee: self.memory_per_bee,
                target_cell_size: self.target_cell_size,
            },
            load: HeartbeatLoad {
                bees_total,
                bees_busy,
                bees_idle,
                memory_pressure,
                queue_depth: 0, // TODO: expose queue depth from BeePool
                colony_temperature,
            },
            cache: HeartbeatCache {
                size_bytes: 0,
                entries: vec![],
            },
        }
    }

    /// Write a single heartbeat to storage.
    pub async fn write_once(&self) -> Result<()> {
        let heartbeat = self.collect_heartbeat().await;
        let key = format!("_heartbeats/node_{}.json", self.node_id);
        let json = serde_json::to_vec_pretty(&heartbeat)
            .map_err(|e| ApiaryError::Serialization(e.to_string()))?;
        self.storage.put(&key, json.into()).await
    }

    /// Run the heartbeat writer loop until the cancellation token fires.
    pub async fn run(&self, cancel: tokio::sync::watch::Receiver<bool>) {
        // Write the first heartbeat immediately on start (join the swarm).
        if let Err(e) = self.write_once().await {
            warn!(error = %e, "Failed to write initial heartbeat");
        } else {
            info!(node_id = %self.node_id, "Heartbeat writer started");
        }

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.interval) => {
                    if let Err(e) = self.write_once().await {
                        warn!(error = %e, "Failed to write heartbeat");
                    }
                }
                _ = wait_for_cancel(&cancel) => {
                    debug!(node_id = %self.node_id, "Heartbeat writer stopping");
                    break;
                }
            }
        }
    }

    /// Delete this node's heartbeat file (graceful departure).
    pub async fn delete_heartbeat(&self) -> Result<()> {
        let key = format!("_heartbeats/node_{}.json", self.node_id);
        self.storage.delete(&key).await
    }
}

// ---------------------------------------------------------------------------
// World View
// ---------------------------------------------------------------------------

/// The state of a node as observed via its heartbeat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Heartbeat is recent — node is healthy.
    Alive,
    /// Heartbeat is stale (> dead_threshold / 2) — may be in trouble.
    Suspect,
    /// Heartbeat is very stale (> dead_threshold) — considered dead.
    Dead,
}

/// Status of a single node in the world view.
#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub node_id: NodeId,
    pub heartbeat: Heartbeat,
    pub state: NodeState,
}

/// A snapshot of all known nodes in the swarm.
#[derive(Debug, Clone)]
pub struct WorldView {
    pub nodes: HashMap<NodeId, NodeStatus>,
    pub updated_at: DateTime<Utc>,
}

impl WorldView {
    /// Create an empty world view.
    pub fn empty() -> Self {
        Self {
            nodes: HashMap::new(),
            updated_at: Utc::now(),
        }
    }

    /// Return only alive nodes.
    pub fn alive_nodes(&self) -> Vec<&NodeStatus> {
        self.nodes
            .values()
            .filter(|n| n.state == NodeState::Alive)
            .collect()
    }

    /// Return the total number of bees across alive nodes.
    pub fn total_bees(&self) -> usize {
        self.alive_nodes()
            .iter()
            .map(|n| n.heartbeat.load.bees_total)
            .sum()
    }

    /// Return the total number of idle bees across alive nodes.
    pub fn total_idle_bees(&self) -> usize {
        self.alive_nodes()
            .iter()
            .map(|n| n.heartbeat.load.bees_idle)
            .sum()
    }
}

// ---------------------------------------------------------------------------
// WorldViewBuilder
// ---------------------------------------------------------------------------

/// Periodically polls the heartbeat prefix and builds the world view.
pub struct WorldViewBuilder {
    storage: Arc<dyn StorageBackend>,
    poll_interval: Duration,
    dead_threshold: Duration,
    world_view: Arc<RwLock<WorldView>>,
}

impl WorldViewBuilder {
    /// Create a new world view builder.
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        poll_interval: Duration,
        dead_threshold: Duration,
    ) -> Self {
        Self {
            storage,
            poll_interval,
            dead_threshold,
            world_view: Arc::new(RwLock::new(WorldView::empty())),
        }
    }

    /// Return a shared handle to the world view.
    pub fn world_view(&self) -> Arc<RwLock<WorldView>> {
        Arc::clone(&self.world_view)
    }

    /// Build the world view once by reading all heartbeat files.
    pub async fn build_once(&self) -> Result<WorldView> {
        let keys = self.storage.list("_heartbeats/").await?;
        let now = Utc::now();
        let mut nodes = HashMap::new();

        for key in &keys {
            // Only process JSON heartbeat files
            if !key.ends_with(".json") {
                continue;
            }

            match self.storage.get(key).await {
                Ok(data) => {
                    match serde_json::from_slice::<Heartbeat>(&data) {
                        Ok(hb) => {
                            let age = now
                                .signed_duration_since(hb.timestamp)
                                .to_std()
                                .unwrap_or(Duration::from_secs(86400 * 365));

                            let state = if age > self.dead_threshold {
                                NodeState::Dead
                            } else if age > self.dead_threshold / 2 {
                                NodeState::Suspect
                            } else {
                                NodeState::Alive
                            };

                            let node_id = NodeId::new(&hb.node_id);
                            nodes.insert(
                                node_id.clone(),
                                NodeStatus {
                                    node_id,
                                    heartbeat: hb,
                                    state,
                                },
                            );
                        }
                        Err(e) => {
                            warn!(key = %key, error = %e, "Failed to parse heartbeat");
                        }
                    }
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "Failed to read heartbeat file");
                }
            }
        }

        Ok(WorldView {
            nodes,
            updated_at: now,
        })
    }

    /// Poll once, update the shared world view, and return it.
    pub async fn poll_once(&self) -> Result<WorldView> {
        let view = self.build_once().await?;
        {
            let mut wv = self.world_view.write().await;
            *wv = view.clone();
        }
        Ok(view)
    }

    /// Run the world view builder loop until cancellation.
    pub async fn run(&self, cancel: tokio::sync::watch::Receiver<bool>) {
        // Build immediately on start.
        if let Err(e) = self.poll_once().await {
            warn!(error = %e, "Failed to build initial world view");
        } else {
            info!("World view builder started");
        }

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {
                    if let Err(e) = self.poll_once().await {
                        warn!(error = %e, "Failed to poll world view");
                    }
                }
                _ = wait_for_cancel(&cancel) => {
                    debug!("World view builder stopping");
                    break;
                }
            }
        }
    }

    /// Clean up heartbeat files for nodes that have been dead longer than `cleanup_age`.
    pub async fn cleanup_stale(&self, cleanup_age: Duration) -> Result<usize> {
        let view = self.world_view.read().await;
        let now = Utc::now();
        let mut cleaned = 0;

        for status in view.nodes.values() {
            if status.state == NodeState::Dead {
                let age = now
                    .signed_duration_since(status.heartbeat.timestamp)
                    .to_std()
                    .unwrap_or(Duration::from_secs(0));
                if age > cleanup_age {
                    let key =
                        format!("_heartbeats/node_{}.json", status.heartbeat.node_id);
                    if let Err(e) = self.storage.delete(&key).await {
                        warn!(key = %key, error = %e, "Failed to clean up stale heartbeat");
                    } else {
                        cleaned += 1;
                        info!(node_id = %status.heartbeat.node_id, "Cleaned up stale heartbeat");
                    }
                }
            }
        }

        Ok(cleaned)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Wait until the watch channel signals `true` (cancellation).
async fn wait_for_cancel(rx: &tokio::sync::watch::Receiver<bool>) {
    let mut rx = rx.clone();
    // Wait until the value is true
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            // Sender dropped — treat as cancel
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apiary_core::config::NodeConfig;
    use apiary_storage::local::LocalBackend;

    async fn make_storage(tmp: &tempfile::TempDir) -> Arc<dyn StorageBackend> {
        Arc::new(LocalBackend::new(tmp.path()).await.unwrap())
    }

    fn make_config(node_id: &str, cores: usize, memory: u64, pool_tmp: &tempfile::TempDir) -> NodeConfig {
        let mut config = NodeConfig::detect("local://test");
        config.node_id = NodeId::new(node_id);
        config.cores = cores;
        config.memory_bytes = memory;
        config.memory_per_bee = if cores > 0 { memory / cores as u64 } else { memory };
        config.target_cell_size = config.memory_per_bee / 4;
        config.cache_dir = pool_tmp.path().to_path_buf();
        config
    }

    fn make_pool(config: &NodeConfig) -> Arc<BeePool> {
        Arc::new(BeePool::new(config))
    }

    #[tokio::test]
    async fn test_heartbeat_write_and_read() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let config = make_config("test-node", 4, 4 * 1024 * 1024 * 1024, &pool_tmp);
        let pool = make_pool(&config);

        let writer = HeartbeatWriter::new(Arc::clone(&storage), &config, pool);

        // Write a heartbeat
        writer.write_once().await.unwrap();

        // Read it back
        let data = storage.get("_heartbeats/node_test-node.json").await.unwrap();
        let hb: Heartbeat = serde_json::from_slice(&data).unwrap();

        assert_eq!(hb.node_id, "test-node");
        assert_eq!(hb.version, 1);
        assert_eq!(hb.capacity.cores, 4);
        assert_eq!(hb.load.bees_total, 4); // from pool with 4 cores
        assert_eq!(hb.load.bees_idle, 4);
        assert_eq!(hb.load.bees_busy, 0);
    }

    #[tokio::test]
    async fn test_heartbeat_version_increments() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let config = make_config("inc-node", 2, 1024, &pool_tmp);
        let pool = make_pool(&config);

        let writer = HeartbeatWriter::new(Arc::clone(&storage), &config, pool);

        writer.write_once().await.unwrap();
        writer.write_once().await.unwrap();
        writer.write_once().await.unwrap();

        let data = storage.get("_heartbeats/node_inc-node.json").await.unwrap();
        let hb: Heartbeat = serde_json::from_slice(&data).unwrap();
        assert_eq!(hb.version, 3);
    }

    #[tokio::test]
    async fn test_heartbeat_delete() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let config = make_config("del-node", 2, 1024, &pool_tmp);
        let pool = make_pool(&config);

        let writer = HeartbeatWriter::new(Arc::clone(&storage), &config, pool);

        writer.write_once().await.unwrap();
        assert!(storage.exists("_heartbeats/node_del-node.json").await.unwrap());

        writer.delete_heartbeat().await.unwrap();
        assert!(!storage.exists("_heartbeats/node_del-node.json").await.unwrap());
    }

    #[tokio::test]
    async fn test_world_view_discovers_nodes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let config_a = make_config("node-a", 2, 1024, &pool_tmp);
        let pool = make_pool(&config_a);

        let writer1 = HeartbeatWriter::new(Arc::clone(&storage), &config_a, Arc::clone(&pool));

        let config_b = make_config("node-b", 4, 2048, &pool_tmp);
        let writer2 = HeartbeatWriter::new(Arc::clone(&storage), &config_b, pool);

        writer1.write_once().await.unwrap();
        writer2.write_once().await.unwrap();

        // Build world view
        let builder = WorldViewBuilder::new(
            Arc::clone(&storage),
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        let view = builder.build_once().await.unwrap();

        assert_eq!(view.nodes.len(), 2);
        assert!(view.nodes.contains_key(&NodeId::new("node-a")));
        assert!(view.nodes.contains_key(&NodeId::new("node-b")));

        // Both should be alive
        assert_eq!(view.nodes[&NodeId::new("node-a")].state, NodeState::Alive);
        assert_eq!(view.nodes[&NodeId::new("node-b")].state, NodeState::Alive);
    }

    #[tokio::test]
    async fn test_world_view_stale_heartbeat_becomes_dead() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;

        // Write a heartbeat with an old timestamp
        let stale_hb = Heartbeat {
            node_id: "stale-node".to_string(),
            timestamp: Utc::now() - chrono::Duration::seconds(60),
            version: 1,
            capacity: HeartbeatCapacity {
                cores: 2,
                memory_total_bytes: 1024,
                memory_per_bee: 512,
                target_cell_size: 128,
            },
            load: HeartbeatLoad {
                bees_total: 2,
                bees_busy: 0,
                bees_idle: 2,
                memory_pressure: 0.0,
                queue_depth: 0,
                colony_temperature: 0.0,
            },
            cache: HeartbeatCache {
                size_bytes: 0,
                entries: vec![],
            },
        };

        let json = serde_json::to_vec(&stale_hb).unwrap();
        storage
            .put("_heartbeats/node_stale-node.json", json.into())
            .await
            .unwrap();

        let builder = WorldViewBuilder::new(
            Arc::clone(&storage),
            Duration::from_secs(5),
            Duration::from_secs(30), // dead after 30s
        );
        let view = builder.build_once().await.unwrap();

        assert_eq!(view.nodes.len(), 1);
        assert_eq!(
            view.nodes[&NodeId::new("stale-node")].state,
            NodeState::Dead
        );
    }

    #[tokio::test]
    async fn test_world_view_suspect_state() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;

        // Write heartbeat at exactly suspect range (between threshold/2 and threshold)
        let suspect_hb = Heartbeat {
            node_id: "suspect-node".to_string(),
            timestamp: Utc::now() - chrono::Duration::seconds(20), // 20s old, threshold=30s, half=15s
            version: 1,
            capacity: HeartbeatCapacity {
                cores: 2,
                memory_total_bytes: 1024,
                memory_per_bee: 512,
                target_cell_size: 128,
            },
            load: HeartbeatLoad {
                bees_total: 2,
                bees_busy: 0,
                bees_idle: 2,
                memory_pressure: 0.0,
                queue_depth: 0,
                colony_temperature: 0.0,
            },
            cache: HeartbeatCache {
                size_bytes: 0,
                entries: vec![],
            },
        };

        let json = serde_json::to_vec(&suspect_hb).unwrap();
        storage
            .put("_heartbeats/node_suspect-node.json", json.into())
            .await
            .unwrap();

        let builder = WorldViewBuilder::new(
            Arc::clone(&storage),
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        let view = builder.build_once().await.unwrap();

        assert_eq!(
            view.nodes[&NodeId::new("suspect-node")].state,
            NodeState::Suspect
        );
    }

    #[tokio::test]
    async fn test_world_view_poll_updates_shared_state() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let config = make_config("shared-node", 2, 1024, &pool_tmp);
        let pool = make_pool(&config);

        let writer = HeartbeatWriter::new(Arc::clone(&storage), &config, pool);
        writer.write_once().await.unwrap();

        let builder = WorldViewBuilder::new(
            Arc::clone(&storage),
            Duration::from_secs(5),
            Duration::from_secs(30),
        );

        // Before polling, world view is empty
        {
            let wv_arc = builder.world_view();
            let wv = wv_arc.read().await;
            assert_eq!(wv.nodes.len(), 0);
        }

        // After polling, world view has the node
        builder.poll_once().await.unwrap();
        {
            let wv_arc = builder.world_view();
            let wv = wv_arc.read().await;
            assert_eq!(wv.nodes.len(), 1);
            assert!(wv.nodes.contains_key(&NodeId::new("shared-node")));
        }
    }

    #[tokio::test]
    async fn test_world_view_graceful_departure() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let config = make_config("departing-node", 2, 1024, &pool_tmp);
        let pool = make_pool(&config);

        let writer = HeartbeatWriter::new(Arc::clone(&storage), &config, pool);

        writer.write_once().await.unwrap();

        let builder = WorldViewBuilder::new(
            Arc::clone(&storage),
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        let view = builder.build_once().await.unwrap();
        assert_eq!(view.nodes.len(), 1);

        // Graceful departure: delete heartbeat
        writer.delete_heartbeat().await.unwrap();

        // World view should now be empty
        let view = builder.build_once().await.unwrap();
        assert_eq!(view.nodes.len(), 0);
    }

    #[tokio::test]
    async fn test_world_view_solo_mode() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let config = make_config("solo-node", 2, 1024, &pool_tmp);
        let pool = make_pool(&config);

        let writer = HeartbeatWriter::new(Arc::clone(&storage), &config, pool);
        writer.write_once().await.unwrap();

        let builder = WorldViewBuilder::new(
            Arc::clone(&storage),
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        let view = builder.build_once().await.unwrap();

        // Solo mode: exactly 1 node, alive
        assert_eq!(view.nodes.len(), 1);
        assert_eq!(view.alive_nodes().len(), 1);
        assert_eq!(view.alive_nodes()[0].node_id, NodeId::new("solo-node"));
    }

    #[tokio::test]
    async fn test_world_view_totals() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let config_n1 = make_config("n1", 2, 1024, &pool_tmp);
        let pool = make_pool(&config_n1);

        let writer1 = HeartbeatWriter::new(Arc::clone(&storage), &config_n1, Arc::clone(&pool));

        let config_n2 = make_config("n2", 4, 2048, &pool_tmp);
        let writer2 = HeartbeatWriter::new(Arc::clone(&storage), &config_n2, pool);

        writer1.write_once().await.unwrap();
        writer2.write_once().await.unwrap();

        let builder = WorldViewBuilder::new(
            Arc::clone(&storage),
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        let view = builder.build_once().await.unwrap();

        // Both writers share the same BeePool with 2 bees, so totals reflect that
        assert_eq!(view.total_bees(), 4); // 2 + 2
        assert_eq!(view.total_idle_bees(), 4); // all idle
    }

    #[tokio::test]
    async fn test_cleanup_stale_heartbeats() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;

        // Write a very old heartbeat (dead for > 1 hour)
        let old_hb = Heartbeat {
            node_id: "ancient-node".to_string(),
            timestamp: Utc::now() - chrono::Duration::hours(2),
            version: 1,
            capacity: HeartbeatCapacity {
                cores: 2,
                memory_total_bytes: 1024,
                memory_per_bee: 512,
                target_cell_size: 128,
            },
            load: HeartbeatLoad {
                bees_total: 2,
                bees_busy: 0,
                bees_idle: 2,
                memory_pressure: 0.0,
                queue_depth: 0,
                colony_temperature: 0.0,
            },
            cache: HeartbeatCache {
                size_bytes: 0,
                entries: vec![],
            },
        };

        let json = serde_json::to_vec(&old_hb).unwrap();
        storage
            .put("_heartbeats/node_ancient-node.json", json.into())
            .await
            .unwrap();

        let builder = WorldViewBuilder::new(
            Arc::clone(&storage),
            Duration::from_secs(5),
            Duration::from_secs(30),
        );

        // Build view first
        builder.poll_once().await.unwrap();

        // Clean up heartbeats dead > 1 hour
        let cleaned = builder
            .cleanup_stale(Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(cleaned, 1);

        // File should be gone
        assert!(!storage.exists("_heartbeats/node_ancient-node.json").await.unwrap());
    }

    #[tokio::test]
    async fn test_heartbeat_writer_with_cancel() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = make_storage(&tmp).await;
        let pool_tmp = tempfile::TempDir::new().unwrap();
        let mut config = make_config("cancel-node", 2, 1024, &pool_tmp);
        config.heartbeat_interval = Duration::from_millis(50);
        let pool = make_pool(&config);

        let writer = Arc::new(HeartbeatWriter::new(
            Arc::clone(&storage),
            &config,
            pool,
        ));

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let w = Arc::clone(&writer);
        let handle = tokio::spawn(async move {
            w.run(cancel_rx).await;
        });

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Cancel it
        cancel_tx.send(true).unwrap();
        handle.await.unwrap();

        // Should have written at least the initial heartbeat
        let data = storage.get("_heartbeats/node_cancel-node.json").await.unwrap();
        let hb: Heartbeat = serde_json::from_slice(&data).unwrap();
        assert!(hb.version >= 1);
    }
}
