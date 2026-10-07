//! Apiary runtime — node lifecycle, bee pool, and swarm coordination.
//!
//! This crate contains the [`ApiaryNode`] which is the main entry point
//! for starting and running an Apiary compute node, the [`BeePool`]
//! which manages isolated execution chambers (mason bee pattern), and the
//! heartbeat / world view system for multi-node awareness.

pub mod bee;
pub mod behavioral;
pub mod cache;
pub mod deposit;
pub mod heartbeat;
pub mod node;
pub mod upkeep;

pub use apiary_plan::ApiaryQueryContext;
pub use bee::{BeePool, BeeState, BeeStatus, MasonChamber};
pub use behavioral::{
    AbandonmentDecision, AbandonmentTracker, ColonyThermometer, TemperatureRegulation,
};
pub use cache::{CacheEntry, CellCache};
pub use deposit::{DepositReport, Depositor};
pub use heartbeat::{
    Heartbeat, HeartbeatWriter, NodeState, NodeStatus, WorldView, WorldViewBuilder,
};
pub use node::{ApiaryNode, ColonyStatus, IngestResult, SwarmNodeInfo, SwarmStatus};
pub use upkeep::{ClearReport, Upkeep, UpkeepSettings};
