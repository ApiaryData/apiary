//! Apiary runtime — node lifecycle, bee pool, and swarm coordination.
//!
//! This crate contains the [`ApiaryNode`] which is the main entry point
//! for starting and running an Apiary compute node.

pub mod node;

pub use node::ApiaryNode;
pub use apiary_query::ApiaryQueryContext;
