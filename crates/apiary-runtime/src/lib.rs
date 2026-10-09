//! Apiary runtime — node lifecycle, the Bees' duties, and swarm coordination.
//!
//! This crate contains the [`ApiaryNode`], the main entry point for starting
//! and running an Apiary compute node; the [`NodeDuties`] its Bees answer
//! (queries, ripening, clearing, surveying), run by the `apiary-colony` Bees; and
//! the heartbeat / world view system for multi-node awareness.

pub mod behavioral;
pub mod budget;
pub mod cache;
pub mod deposit;
pub mod duties;
pub mod heartbeat;
pub mod node;
pub mod upkeep;

pub use apiary_colony::TemperatureRegulation;
pub use apiary_plan::ApiaryQueryContext;
pub use behavioral::{AbandonmentDecision, AbandonmentTracker};
pub use budget::CommitBudget;
pub use cache::{CacheEntry, CellCache};
pub use deposit::{DepositReport, Depositor};
pub use duties::{DutiesSettings, NodeDuties};
pub use heartbeat::{
    Heartbeat, HeartbeatWriter, LoadSource, NodeState, NodeStatus, WorldView, WorldViewBuilder,
};
pub use node::{ApiaryNode, BeeStatus, ColonyStatus, IngestResult, SwarmNodeInfo, SwarmStatus};
pub use upkeep::{ClearReport, Upkeep, UpkeepSettings};
