//! The Apiary node — a stateless compute instance in the swarm.
//!
//! [`ApiaryNode`] is the main runtime entry point. It initialises the
//! storage backend, detects system capacity, creates the bee pool,
//! and manages the node lifecycle.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tracing::info;

use apiary_core::config::NodeConfig;
use apiary_core::error::ApiaryError;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::storage::StorageBackend;
use apiary_core::{
    CellSizingPolicy, FrameSchema, LedgerAction, Result, WriteResult,
};
use apiary_query::ApiaryQueryContext;
use apiary_storage::cell_reader::CellReader;
use apiary_storage::cell_writer::CellWriter;
use apiary_storage::ledger::Ledger;
use apiary_storage::local::LocalBackend;
use apiary_storage::s3::S3Backend;

use crate::bee::{BeePool, BeeStatus};

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
    pub registry: Arc<RegistryManager>,

    /// DataFusion-based SQL query context.
    pub query_ctx: Arc<tokio::sync::Mutex<ApiaryQueryContext>>,

    /// Pool of bees for isolated task execution.
    pub bee_pool: Arc<BeePool>,
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
        let registry = Arc::new(RegistryManager::new(Arc::clone(&storage)));
        let _ = registry.load_or_create().await?;
        info!("Registry loaded");

        // Initialize query context
        let query_ctx = Arc::new(tokio::sync::Mutex::new(ApiaryQueryContext::new(
            Arc::clone(&storage),
            Arc::clone(&registry),
        )));

        // Initialize bee pool
        let bee_pool = Arc::new(BeePool::new(&config));
        info!(bees = config.cores, "Bee pool initialized");

        Ok(Self { config, storage, registry, query_ctx, bee_pool })
    }

    /// Gracefully shut down the node.
    ///
    /// In the future this will stop the heartbeat writer, drain in-progress
    /// tasks, and clean up scratch directories. For now it logs the shutdown.
    pub async fn shutdown(&self) {
        info!(node_id = %self.config.node_id, "Apiary node shutting down");
    }

    /// Write data to a frame. This is the end-to-end write path:
    /// 1. Resolve frame from registry
    /// 2. Open/create ledger
    /// 3. Validate schema
    /// 4. Partition data
    /// 5. Write cells to storage
    /// 6. Commit ledger entry
    pub async fn write_to_frame(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        batch: &RecordBatch,
    ) -> Result<WriteResult> {
        let start = std::time::Instant::now();

        // Resolve frame metadata
        let frame = self.registry.get_frame(hive, box_name, frame_name).await?;
        let schema = FrameSchema::from_json_value(&frame.schema)?;
        let frame_path = format!("{}/{}/{}", hive, box_name, frame_name);

        // Open or create ledger
        let mut ledger = match Ledger::open(Arc::clone(&self.storage), &frame_path).await {
            Ok(l) => l,
            Err(_) => {
                Ledger::create(
                    Arc::clone(&self.storage),
                    &frame_path,
                    schema.clone(),
                    frame.partition_by.clone(),
                    &self.config.node_id,
                )
                .await?
            }
        };

        // Write cells
        let sizing = CellSizingPolicy::new(
            self.config.target_cell_size,
            self.config.max_cell_size,
            self.config.min_cell_size,
        );

        let writer = CellWriter::new(
            Arc::clone(&self.storage),
            frame_path,
            schema,
            frame.partition_by.clone(),
            sizing,
        );

        let cells = writer.write(batch).await?;

        let cells_written = cells.len();
        let rows_written: u64 = cells.iter().map(|c| c.rows).sum();
        let bytes_written: u64 = cells.iter().map(|c| c.bytes).sum();

        // Commit to ledger
        let version = ledger
            .commit(LedgerAction::AddCells { cells }, &self.config.node_id)
            .await?;

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(WriteResult {
            version,
            cells_written,
            rows_written,
            bytes_written,
            duration_ms,
        })
    }

    /// Read data from a frame, optionally filtering by partition values.
    /// Returns all matching data as a merged RecordBatch.
    pub async fn read_from_frame(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        partition_filter: Option<&HashMap<String, String>>,
    ) -> Result<Option<RecordBatch>> {
        let frame_path = format!("{}/{}/{}", hive, box_name, frame_name);

        let ledger = match Ledger::open(Arc::clone(&self.storage), &frame_path).await {
            Ok(l) => l,
            Err(ApiaryError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e),
        };

        let cells = if let Some(filter) = partition_filter {
            ledger.prune_cells(filter, &HashMap::new())
        } else {
            ledger.active_cells().iter().collect()
        };

        if cells.is_empty() {
            return Ok(None);
        }

        let reader = CellReader::new(Arc::clone(&self.storage), frame_path);
        reader.read_cells_merged(&cells, None).await
    }

    /// Overwrite all data in a frame with new data.
    /// Commits a RewriteCells entry removing all existing cells and adding new ones.
    pub async fn overwrite_frame(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        batch: &RecordBatch,
    ) -> Result<WriteResult> {
        let start = std::time::Instant::now();

        let frame = self.registry.get_frame(hive, box_name, frame_name).await?;
        let schema = FrameSchema::from_json_value(&frame.schema)?;
        let frame_path = format!("{}/{}/{}", hive, box_name, frame_name);

        let mut ledger = Ledger::open(Arc::clone(&self.storage), &frame_path).await?;

        let sizing = CellSizingPolicy::new(
            self.config.target_cell_size,
            self.config.max_cell_size,
            self.config.min_cell_size,
        );

        let writer = CellWriter::new(
            Arc::clone(&self.storage),
            frame_path,
            schema,
            frame.partition_by.clone(),
            sizing,
        );

        let new_cells = writer.write(batch).await?;

        let cells_written = new_cells.len();
        let rows_written: u64 = new_cells.iter().map(|c| c.rows).sum();
        let bytes_written: u64 = new_cells.iter().map(|c| c.bytes).sum();

        // Remove all old cells, add new ones
        let removed: Vec<_> = ledger.active_cells().iter().map(|c| c.id.clone()).collect();

        let version = ledger
            .commit(
                LedgerAction::RewriteCells {
                    removed,
                    added: new_cells,
                },
                &self.config.node_id,
            )
            .await?;

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(WriteResult {
            version,
            cells_written,
            rows_written,
            bytes_written,
            duration_ms,
        })
    }

    /// Initialize the ledger for a frame (called after create_frame in registry).
    pub async fn init_frame_ledger(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
    ) -> Result<()> {
        let frame = self.registry.get_frame(hive, box_name, frame_name).await?;
        let schema = FrameSchema::from_json_value(&frame.schema)?;
        let frame_path = format!("{}/{}/{}", hive, box_name, frame_name);

        Ledger::create(
            Arc::clone(&self.storage),
            &frame_path,
            schema,
            frame.partition_by.clone(),
            &self.config.node_id,
        )
        .await?;

        Ok(())
    }

    /// Execute a SQL query and return results as RecordBatches.
    ///
    /// The query is executed through the BeePool — assigned to an idle bee
    /// or queued if all bees are busy. Each bee runs in its own sealed
    /// chamber with memory budget and timeout enforcement.
    ///
    /// Supports:
    /// - Standard SQL (SELECT, GROUP BY, ORDER BY, etc.) over frames
    /// - Custom commands: USE HIVE, USE BOX, SHOW HIVES, SHOW BOXES, SHOW FRAMES, DESCRIBE
    /// - 3-part table names: hive.box.frame
    /// - 1-part names after USE HIVE / USE BOX
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let query_ctx = Arc::clone(&self.query_ctx);
        let query_owned = query.to_string();
        let rt_handle = tokio::runtime::Handle::current();

        let handle = self.bee_pool.submit(move || {
            rt_handle.block_on(async {
                let mut ctx = query_ctx.lock().await;
                ctx.sql(&query_owned).await
            })
        }).await;

        handle.await.map_err(|e| ApiaryError::Internal {
            message: format!("Task join error: {e}"),
        })?
    }

    /// Return the status of each bee in the pool.
    pub async fn bee_status(&self) -> Vec<BeeStatus> {
        self.bee_pool.status().await
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
