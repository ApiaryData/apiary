//! The comb and storage backends for Apiary.
//!
//! - [`Comb`] — every Frame is a Delta Lake table, written through `delta-rs`
//! - [`LocalBackend`] and [`S3Backend`] — [`StorageBackend`](apiary_core::StorageBackend)
//!   implementations for the registry, heartbeats and other small control files
//!
//! Schemas, and conforming incoming batches to them, are in [`schema`].

pub mod comb;
pub mod local;
pub mod s3;
pub mod schema;

pub use comb::{CellState, Comb, Committed, FrameStats, STATE_TAG, query_session};
pub use deltalake::DeltaTable;
pub use local::LocalBackend;
pub use s3::S3Backend;

#[cfg(test)]
mod comb_tests;
