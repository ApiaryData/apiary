//! The comb and storage backends for Apiary.
//!
//! - [`Comb`] — every Frame is a Delta Lake table, written through `delta-rs`
//! - [`LocalBackend`] and [`S3Backend`] — [`StorageBackend`](apiary_core::StorageBackend)
//!   implementations for the registry, heartbeats and other small control files
//!
//! Schemas, and conforming incoming batches to them, are in [`schema`].

pub mod cell;
pub mod comb;
pub mod crop;
pub mod custom_store;
pub mod local;
pub mod s3;
pub mod schema;
pub mod upkeep;

pub use cell::{Capped, Cell, Nectar, Recipe, Ripe, RipenessChecks};
pub use comb::{
    CellState, Comb, Committed, FrameStats, REWRITE_FENCE, STAGE_COLUMN, STATE_TAG, query_session,
};
pub use crop::{Crop, FrameCrop, FrameKey, Segment};
pub use deltalake::DeltaTable;
pub use local::LocalBackend;
pub use s3::S3Backend;
pub use upkeep::{CapOptions, CapReport, HarvestReport, NectarSurvey};

#[cfg(test)]
mod comb_tests;
#[cfg(test)]
mod upkeep_tests;
