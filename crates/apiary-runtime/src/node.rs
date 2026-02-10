//! The Apiary node — a stateless compute instance in the swarm.
//!
//! [`ApiaryNode`] is the main runtime entry point. It initialises the
//! storage backend, detects system capacity, and manages the node lifecycle.

use std::sync::Arc;

use tracing::info;

use apiary_core::config::NodeConfig;
use apiary_core::error::ApiaryError;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::storage::StorageBackend;
use apiary_core::Result;
use apiary_storage::local::LocalBackend;
use apiary_storage::s3::S3Backend;

/// An Apiary compute node — the runtime for one machine in the swarm.
///
/// The node holds a reference to the storage backend and its configuration.
/// In solo mode it uses [`LocalBackend`]; in multi-node mode it uses
/// [`S3Backend`]. The node is otherwise stateless — all committed state
/// lives in object storage.
pub struct ApiaryNode {
    /// Node configuration including auto-detected capacity.
    pub config: NodeConfig,

    /// The shared storage backend (object storage or local filesystem).
    pub storage: Arc<dyn StorageBackend>,

    /// Registry manager for DDL operations.
    pub registry: RegistryManager,
}

impl ApiaryNode {
    /// Start a new Apiary node with the given configuration.
    ///
    /// Initialises the appropriate storage backend based on `config.storage_uri`
    /// and logs the node's capacity.
    pub async fn start(config: NodeConfig) -> Result<Self> {
        let storage: Arc<dyn StorageBackend> = if config.storage_uri.starts_with("s3://") {
            Arc::new(S3Backend::new(&config.storage_uri)?)
        } else {
            // Parse local URI: "local://<path>" or treat as raw path
            let path = config
                .storage_uri
                .strip_prefix("local://")
                .unwrap_or(&config.storage_uri);

            // Expand ~ to home directory
            let expanded = if path.starts_with("~/") || path.starts_with("~\\") {
                let home = home_dir().ok_or_else(|| ApiaryError::Config {
                    message: "Cannot determine home directory".to_string(),
                })?;
                home.join(&path[2..])
            } else {
                std::path::PathBuf::from(path)
            };

            Arc::new(LocalBackend::new(expanded).await?)
        };

        info!(
            node_id = %config.node_id,
            cores = config.cores,
            memory_mb = config.memory_bytes / (1024 * 1024),
            memory_per_bee_mb = config.memory_per_bee / (1024 * 1024),
            target_cell_size_mb = config.target_cell_size / (1024 * 1024),
            storage_uri = %config.storage_uri,
            "Apiary node started"
        );

        // Initialize registry
        let registry = RegistryManager::new(Arc::clone(&storage));
        let _ = registry.load_or_create().await?;
        info!("Registry loaded");

        Ok(Self { config, storage, registry })
    }

    /// Gracefully shut down the node.
    ///
    /// In the future this will stop the heartbeat writer, drain in-progress
    /// tasks, and clean up scratch directories. For now it logs the shutdown.
    pub async fn shutdown(&self) {
        info!(node_id = %self.config.node_id, "Apiary node shutting down");
    }
}

/// Best-effort home directory detection.
fn home_dir() -> Option<std::path::PathBuf> {
    #[cfg(target_os = "windows")]
    {
        std::env::var("USERPROFILE")
            .ok()
            .map(std::path::PathBuf::from)
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var("HOME").ok().map(std::path::PathBuf::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start_local_node() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("local://test");
        config.storage_uri = format!("local://{}", tmp.path().display());
        let node = ApiaryNode::start(config).await.unwrap();
        assert!(node.config.cores > 0);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn test_start_with_raw_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("test");
        config.storage_uri = tmp.path().to_string_lossy().to_string();
        let node = ApiaryNode::start(config).await.unwrap();
        node.shutdown().await;
    }
}
