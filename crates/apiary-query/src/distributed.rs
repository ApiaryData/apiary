//! Distributed query execution — plan queries across the swarm.
//!
//! The distributed planner assigns cells to nodes based on cache locality
//! and available capacity. The coordinator writes a query manifest to storage,
//! executes local tasks, polls for partial results from workers, merges them,
//! and returns the final result.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

use apiary_core::error::ApiaryError;
use apiary_core::storage::StorageBackend;
use apiary_core::types::NodeId;
use apiary_core::Result;

/// Node state in the swarm.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node heartbeat is fresh
    Alive,
    /// Node heartbeat is stale (missed recent heartbeats)
    Suspect,
    /// Node heartbeat is very stale or missing
    Dead,
}

/// Information about a cell needed for query planning.
#[derive(Clone, Debug)]
pub struct CellInfo {
    /// Storage key for the cell
    pub storage_key: String,
    /// Size in bytes
    pub bytes: u64,
    /// Partition values (for cache locality tracking)
    pub partition: Vec<(String, String)>,
}

/// Information about a node from the world view.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub state: NodeState,
    pub cores: usize,
    pub memory_bytes: u64,
    pub memory_per_bee: u64,
    pub target_cell_size: u64,
    pub bees_total: usize,
    pub bees_busy: usize,
    pub idle_bees: usize,
    /// Cached cells: storage_key -> size_bytes
    pub cached_cells: HashMap<String, u64>,
}

/// Query execution plan.
#[derive(Clone, Debug)]
pub enum QueryPlan {
    /// Execute locally on a single node (all cells fit in one bee's budget).
    Local {
        cells: Vec<CellInfo>,
    },
    /// Distribute across multiple nodes.
    Distributed {
        assignments: HashMap<NodeId, Vec<CellInfo>>,
    },
}

/// A task assigned to a specific node in a distributed query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannedTask {
    /// Unique task ID
    pub task_id: String,
    /// Node assigned to execute this task
    pub node_id: NodeId,
    /// Cells to scan (storage keys)
    pub cells: Vec<String>,
    /// SQL fragment to execute
    pub sql_fragment: String,
}

/// Query manifest written to storage for distributed execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryManifest {
    /// Unique query ID
    pub query_id: String,
    /// Original SQL query
    pub original_sql: String,
    /// Tasks assigned to each node
    pub tasks: Vec<PlannedTask>,
    /// SQL to merge partial results
    pub merge_sql: Option<String>,
    /// Query timeout in seconds
    pub timeout_secs: u64,
    /// Timestamp when query was created
    pub created_at: u64,
}

/// Plan a query: decide whether to execute locally or distribute across nodes.
///
/// # Strategy
/// 1. If all cells fit in one bee's memory budget and we have an idle bee, run locally
/// 2. Otherwise, assign cells to nodes using cache locality and capacity
/// 3. Apply leafcutter sizing: split assignments that exceed a bee's budget
pub fn plan_query(
    cells: Vec<CellInfo>,
    nodes: Vec<NodeInfo>,
    local_node_id: &NodeId,
) -> Result<QueryPlan> {
    if nodes.is_empty() {
        return Err(ApiaryError::Internal {
            message: "No alive nodes in world view".into(),
        });
    }
    
    // Find local node info
    let local_node = nodes.iter()
        .find(|n| &n.node_id == local_node_id)
        .ok_or_else(|| ApiaryError::Internal {
            message: format!("Local node {} not found in world view", local_node_id),
        })?;
    
    // Calculate total size
    let total_size: u64 = cells.iter().map(|c| c.bytes).sum();
    
    // If small enough for one bee and we have idle capacity, run locally
    if total_size < local_node.memory_per_bee && local_node.idle_bees > 0 {
        info!(
            total_size_mb = total_size / (1024 * 1024),
            bee_budget_mb = local_node.memory_per_bee / (1024 * 1024),
            "Query fits in one bee, executing locally"
        );
        return Ok(QueryPlan::Local { cells });
    }
    
    // If only one alive node, run locally (no distribution possible)
    if nodes.len() == 1 {
        info!("Only one node available, executing locally");
        return Ok(QueryPlan::Local { cells });
    }
    
    // Distribute across nodes
    info!(
        total_cells = cells.len(),
        total_size_mb = total_size / (1024 * 1024),
        alive_nodes = nodes.len(),
        "Distributing query across swarm"
    );
    
    let assignments = assign_cells_to_nodes(cells, &nodes);
    
    if assignments.is_empty() {
        return Err(ApiaryError::Internal {
            message: "Failed to assign cells to any node".into(),
        });
    }
    
    Ok(QueryPlan::Distributed { assignments })
}

/// Extract node information from the world view (helper for runtime).
/// This will be called by the runtime layer that has access to WorldView.
pub fn extract_alive_nodes<T>(
    world_view_nodes: &HashMap<NodeId, T>,
    node_extractor: impl Fn(&T) -> Option<NodeInfo>,
) -> Vec<NodeInfo> {
    world_view_nodes.values()
        .filter_map(|node| node_extractor(node))
        .collect()
}

/// Assign cells to nodes based on cache locality and capacity.
fn assign_cells_to_nodes(
    cells: Vec<CellInfo>,
    nodes: &[NodeInfo],
) -> HashMap<NodeId, Vec<CellInfo>> {
    let mut assignments: HashMap<NodeId, Vec<CellInfo>> = HashMap::new();
    
    for cell in cells {
        // Try to find a node that has this cell cached
        let caching_node = nodes.iter()
            .filter(|n| n.idle_bees > 0)
            .find(|n| n.cached_cells.contains_key(&cell.storage_key));
        
        if let Some(node) = caching_node {
            // Assign to caching node
            assignments.entry(node.node_id.clone())
                .or_insert_with(Vec::new)
                .push(cell);
            continue;
        }
        
        // No cache hit, assign to node with most idle capacity
        if let Some(best_node) = nodes.iter()
            .filter(|n| n.idle_bees > 0)
            .max_by_key(|n| n.idle_bees)
        {
            assignments.entry(best_node.node_id.clone())
                .or_insert_with(Vec::new)
                .push(cell);
        }
    }
    
    // TODO: Apply leafcutter sizing to split large assignments
    
    assignments
}

/// Generate SQL fragment for a query (simplified for v1).
///
/// For now, we support simple scans and basic aggregations.
pub fn generate_sql_fragment(original_sql: &str, is_aggregation: bool) -> (String, Option<String>) {
    // Simplified fragment generation for v1
    // In a real implementation, we'd parse the SQL and decompose aggregations
    
    if is_aggregation {
        // For aggregations, generate partial aggregation fragment
        // This is a placeholder - real implementation would parse and transform
        let fragment = format!(
            "SELECT * FROM __scan__ /* TODO: Transform to partial aggregation */"
        );
        let merge = Some(format!(
            "SELECT * FROM __partials__ /* TODO: Transform to final aggregation */"
        ));
        (fragment, merge)
    } else {
        // For simple scans, use the original WHERE clause
        let fragment = original_sql.to_string();
        (fragment, None)
    }
}

/// Query manifest path in storage.
pub fn manifest_path(query_id: &str) -> String {
    format!("_queries/{}/manifest.json", query_id)
}

/// Partial result path for a node.
pub fn partial_result_path(query_id: &str, node_id: &NodeId) -> String {
    format!("_queries/{}/partial_{}.arrow", query_id, node_id)
}

/// Create a new query manifest.
pub fn create_manifest(
    original_sql: &str,
    tasks: Vec<PlannedTask>,
    merge_sql: Option<String>,
    timeout_secs: u64,
) -> QueryManifest {
    let query_id = Uuid::new_v4().to_string();
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    QueryManifest {
        query_id,
        original_sql: original_sql.to_string(),
        tasks,
        merge_sql,
        timeout_secs,
        created_at,
    }
}

/// Write query manifest to storage.
pub async fn write_manifest(
    storage: &Arc<dyn StorageBackend>,
    manifest: &QueryManifest,
) -> Result<()> {
    let path = manifest_path(&manifest.query_id);
    let json = serde_json::to_vec(manifest).map_err(|e| ApiaryError::Internal {
        message: format!("Failed to serialize manifest: {}", e),
    })?;
    
    storage.put(&path, json.into()).await?;
    info!(query_id = %manifest.query_id, "Query manifest written");
    Ok(())
}

/// Read query manifest from storage.
pub async fn read_manifest(
    storage: &Arc<dyn StorageBackend>,
    query_id: &str,
) -> Result<QueryManifest> {
    let path = manifest_path(query_id);
    let bytes = storage.get(&path).await?;
    let manifest = serde_json::from_slice(&bytes).map_err(|e| ApiaryError::Internal {
        message: format!("Failed to deserialize manifest: {}", e),
    })?;
    Ok(manifest)
}

/// Write partial result as Arrow IPC file.
pub async fn write_partial_result(
    storage: &Arc<dyn StorageBackend>,
    query_id: &str,
    node_id: &NodeId,
    batches: &[RecordBatch],
) -> Result<()> {
    if batches.is_empty() {
        return Err(ApiaryError::Internal {
            message: "Cannot write empty partial result".into(),
        });
    }
    
    let path = partial_result_path(query_id, node_id);
    
    // Write to Arrow IPC format
    let mut buf = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut buf, &batches[0].schema())
            .map_err(|e| ApiaryError::Internal {
                message: format!("Failed to create Arrow writer: {}", e),
            })?;
        
        for batch in batches {
            writer.write(batch).map_err(|e| ApiaryError::Internal {
                message: format!("Failed to write batch: {}", e),
            })?;
        }
        
        writer.finish().map_err(|e| ApiaryError::Internal {
            message: format!("Failed to finish Arrow writer: {}", e),
        })?;
    }
    
    storage.put(&path, buf.into()).await?;
    info!(query_id = %query_id, node_id = %node_id, "Partial result written");
    Ok(())
}

/// Read partial result from Arrow IPC file.
pub async fn read_partial_result(
    storage: &Arc<dyn StorageBackend>,
    query_id: &str,
    node_id: &NodeId,
) -> Result<Vec<RecordBatch>> {
    let path = partial_result_path(query_id, node_id);
    let bytes = storage.get(&path).await?;
    
    let cursor = std::io::Cursor::new(bytes.to_vec());
    let reader = FileReader::try_new(cursor, None)
        .map_err(|e| ApiaryError::Internal {
            message: format!("Failed to create Arrow reader: {}", e),
        })?;
    
    let batches: Result<Vec<_>> = reader
        .map(|result| result.map_err(|e| ApiaryError::Internal {
            message: format!("Failed to read batch: {}", e),
        }))
        .collect();
    
    batches
}

/// Clean up query directory after completion.
pub async fn cleanup_query(
    storage: &Arc<dyn StorageBackend>,
    query_id: &str,
) -> Result<()> {
    let prefix = format!("_queries/{}/", query_id);
    let keys = storage.list(&prefix).await?;
    
    for key in keys {
        if let Err(e) = storage.delete(&key).await {
            warn!(key = %key, error = %e, "Failed to delete query file");
        }
    }
    
    info!(query_id = %query_id, "Query files cleaned up");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn mock_cell_info(key: &str, bytes: u64) -> CellInfo {
        CellInfo {
            storage_key: key.to_string(),
            bytes,
            partition: vec![],
        }
    }
    
    #[test]
    fn test_assign_cells_prefers_cache_locality() {
        let cells = vec![
            mock_cell_info("cell1", 100_000_000),
            mock_cell_info("cell2", 100_000_000),
        ];
        
        let mut cached = HashMap::new();
        cached.insert("cell1".to_string(), 100_000_000);
        
        let nodes = vec![
            NodeInfo {
                node_id: NodeId::from("node1"),
                state: NodeState::Alive,
                cores: 4,
                memory_bytes: 4_000_000_000,
                memory_per_bee: 1_000_000_000,
                target_cell_size: 256_000_000,
                bees_total: 4,
                bees_busy: 0,
                idle_bees: 4,
                cached_cells: cached,
            },
            NodeInfo {
                node_id: NodeId::from("node2"),
                state: NodeState::Alive,
                cores: 4,
                memory_bytes: 4_000_000_000,
                memory_per_bee: 1_000_000_000,
                target_cell_size: 256_000_000,
                bees_total: 4,
                bees_busy: 0,
                idle_bees: 4,
                cached_cells: HashMap::new(),
            },
        ];
        
        let assignments = assign_cells_to_nodes(cells, &nodes);
        
        // cell1 should go to node1 (cache hit)
        assert!(assignments.get(&NodeId::from("node1")).is_some());
        let node1_cells = assignments.get(&NodeId::from("node1")).unwrap();
        assert_eq!(node1_cells.len(), 1);
        assert_eq!(node1_cells[0].storage_key, "cell1");
    }
    
    #[test]
    fn test_assign_cells_distributes_to_idle_nodes() {
        let cells = vec![
            mock_cell_info("cell1", 100_000_000),
            mock_cell_info("cell2", 100_000_000),
        ];
        
        let nodes = vec![
            NodeInfo {
                node_id: NodeId::from("node1"),
                state: NodeState::Alive,
                cores: 4,
                memory_bytes: 4_000_000_000,
                memory_per_bee: 1_000_000_000,
                target_cell_size: 256_000_000,
                bees_total: 4,
                bees_busy: 3,
                idle_bees: 1,
                cached_cells: HashMap::new(),
            },
            NodeInfo {
                node_id: NodeId::from("node2"),
                state: NodeState::Alive,
                cores: 4,
                memory_bytes: 4_000_000_000,
                memory_per_bee: 1_000_000_000,
                target_cell_size: 256_000_000,
                bees_total: 4,
                bees_busy: 0,
                idle_bees: 4,
                cached_cells: HashMap::new(),
            },
        ];
        
        let assignments = assign_cells_to_nodes(cells, &nodes);
        
        // Both cells should go to node2 (more idle capacity)
        assert!(assignments.get(&NodeId::from("node2")).is_some());
        let node2_cells = assignments.get(&NodeId::from("node2")).unwrap();
        assert_eq!(node2_cells.len(), 2);
    }
}
