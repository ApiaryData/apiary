//! Apiary runtime — node lifecycle, bee pool, and swarm coordination.
//!
//! This crate contains the [`ApiaryNode`] which is the main entry point
//! for starting and running an Apiary compute node, and the [`BeePool`]
//! which manages isolated execution chambers (mason bee pattern).

pub mod bee;
pub mod node;

pub use bee::{BeePool, BeeState, BeeStatus, MasonChamber};
pub use node::ApiaryNode;
pub use apiary_query::ApiaryQueryContext;
