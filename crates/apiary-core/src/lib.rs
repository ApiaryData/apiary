//! Apiary core types, traits, configuration, and errors.
//!
//! This crate provides the foundational building blocks for the Apiary
//! distributed data processing framework: typed identifiers, the
//! [`StorageBackend`] trait, node configuration with system detection,
//! and the unified error type.

pub mod config;
pub mod error;
pub mod ledger_types;
pub mod registry;
pub mod registry_manager;
pub mod storage;
pub mod types;

pub use config::NodeConfig;
pub use error::ApiaryError;
pub use ledger_types::{
    CellMetadata, CellSizingPolicy, ColumnStats, FieldDef, FrameSchema, LedgerAction,
    LedgerCheckpoint, LedgerEntry, WriteResult,
};
pub use registry::{Box, Frame, Hive, Registry};
pub use registry_manager::RegistryManager;
pub use storage::StorageBackend;
pub use types::*;

/// Convenience Result type using [`ApiaryError`].
pub type Result<T> = std::result::Result<T, ApiaryError>;
