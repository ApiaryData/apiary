//! Apiary core types, traits, configuration, and errors.
//!
//! This crate provides the foundational building blocks for the Apiary
//! distributed data processing framework: typed identifiers, the
//! [`StorageBackend`] trait, node configuration with system detection,
//! and the unified error type.

pub mod clock;
pub mod config;
pub mod env;
pub mod error;
pub mod frame_types;
pub mod registry;
pub mod registry_manager;
pub mod rng;
pub mod storage;
pub mod types;

pub use clock::{Clock, ManualClock, Millis, SystemClock};
pub use config::NodeConfig;
pub use env::Env;
pub use error::ApiaryError;
pub use frame_types::{CellSizingPolicy, FieldDef, FrameSchema, WriteResult};
pub use registry::{Box, Frame, Hive, Registry};
pub use registry_manager::RegistryManager;
pub use rng::{SeededRng, StdSeededRng};
pub use storage::StorageBackend;
pub use types::*;

/// Convenience Result type using [`ApiaryError`].
pub type Result<T> = std::result::Result<T, ApiaryError>;
