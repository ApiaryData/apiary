//! Apiary runtime — node lifecycle, bee pool, and swarm coordination.
//!
//! This crate contains the [`ApiaryNode`] which is the main entry point
//! for starting and running an Apiary compute node, the [`BeePool`]
//! which manages isolated execution chambers (mason bee pattern), and the
//! heartbeat / world view system for multi-node awareness.

pub mod bee;
pub mod cache;
pub mod heartbeat;
pub mod node;

pub use bee::{BeePool, BeeState, BeeStatus, MasonChamber};
pub use cache::{CacheEntry, CellCache};
pub use heartbeat::{
    Heartbeat, HeartbeatWriter, NodeState, NodeStatus, WorldView, WorldViewBuilder,
};
pub use node::ApiaryNode;
pub use apiary_query::ApiaryQueryContext;

/// Convert WorldView to a vector of NodeInfo for distributed query planning.
pub fn world_view_to_node_info(world_view: &WorldView) -> Vec<apiary_query::distributed::NodeInfo> {
    world_view.alive_nodes()
        .iter()
        .map(|node| {
            // Extract cached cells from heartbeat
            let cached_cells = node.heartbeat.cache.cached_cells.clone();
            
            apiary_query::distributed::NodeInfo {
                node_id: node.node_id.clone(),
                state: match node.state {
                    NodeState::Alive => apiary_query::distributed::NodeState::Alive,
                    NodeState::Suspect => apiary_query::distributed::NodeState::Suspect,
                    NodeState::Dead => apiary_query::distributed::NodeState::Dead,
                },
                cores: node.heartbeat.capacity.cores,
                memory_bytes: node.heartbeat.capacity.memory_total_bytes,
                memory_per_bee: node.heartbeat.capacity.memory_per_bee,
                target_cell_size: node.heartbeat.capacity.target_cell_size,
                bees_total: node.heartbeat.load.bees_total,
                bees_busy: node.heartbeat.load.bees_busy,
                idle_bees: node.heartbeat.load.bees_idle,
                cached_cells,
            }
        })
        .collect()
}
