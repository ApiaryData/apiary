//! Unified error types for Apiary.
//!
//! All errors in Apiary are represented by [`ApiaryError`], using `thiserror`
//! for ergonomic error definitions. Library code never uses `unwrap()` —
//! all fallible operations return `Result<T, ApiaryError>`.

use crate::types::{BeeId, FrameId};
use thiserror::Error;

/// The unified error type for all Apiary operations.
#[derive(Error, Debug)]
pub enum ApiaryError {
    /// A storage operation failed.
    #[error("Storage error: {message}")]
    Storage {
        /// Human-readable description of the failure.
        message: String,
        /// The underlying error, if available.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// The requested key was not found in storage.
    #[error("Not found: {key}")]
    NotFound {
        /// The storage key that was not found.
        key: String,
    },

    /// A conditional write conflict occurred (another writer committed first).
    #[error("Write conflict on key: {key}")]
    WriteConflict {
        /// The storage key where the conflict occurred.
        key: String,
    },

    /// An entity with this name already exists.
    #[error("{entity_type} already exists: {name}")]
    AlreadyExists {
        /// The type of entity (e.g., "Hive", "Frame").
        entity_type: String,
        /// The conflicting name.
        name: String,
    },

    /// The requested entity was not found in the registry.
    #[error("{entity_type} not found: {name}")]
    EntityNotFound {
        /// The type of entity (e.g., "Hive", "Frame").
        entity_type: String,
        /// The name that was not found.
        name: String,
    },

    /// A schema validation error occurred during a write.
    #[error("Schema error: {message}")]
    Schema {
        /// Description of the schema mismatch.
        message: String,
    },

    /// A bee exceeded its memory budget.
    #[error("Memory exceeded for bee {bee_id}: requested {requested} bytes, budget {budget} bytes")]
    MemoryExceeded {
        /// The bee that exceeded its budget.
        bee_id: BeeId,
        /// The memory budget in bytes.
        budget: u64,
        /// The amount of memory requested in bytes.
        requested: u64,
    },

    /// A task exceeded its timeout.
    #[error("Task timeout: {message}")]
    TaskTimeout {
        /// Description of the timed-out task.
        message: String,
    },

    /// A task was abandoned after exceeding the retry limit.
    #[error("Task abandoned: {message}")]
    TaskAbandoned {
        /// Description including attempt history.
        message: String,
    },

    /// Invalid configuration was provided.
    #[error("Configuration error: {message}")]
    Config {
        /// Description of the configuration problem.
        message: String,
    },

    /// A frame path could not be resolved.
    #[error("Cannot resolve frame path '{path}': {reason}")]
    Resolution {
        /// The path that could not be resolved.
        path: String,
        /// The reason resolution failed.
        reason: String,
    },

    /// An unsupported operation was attempted.
    #[error("Unsupported: {message}")]
    Unsupported {
        /// Description of the unsupported operation.
        message: String,
    },

    /// A ledger operation failed.
    #[error("Ledger error for frame {frame_id}: {message}")]
    Ledger {
        /// The frame whose ledger had an error.
        frame_id: FrameId,
        /// Description of the ledger error.
        message: String,
    },

    /// An internal error (bug).
    #[error("Internal error: {message}")]
    Internal {
        /// Description of the internal error.
        message: String,
    },
}

impl ApiaryError {
    /// Create a storage error from a message and source error.
    pub fn storage(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Storage {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Create a storage error from a message only.
    pub fn storage_msg(message: impl Into<String>) -> Self {
        Self::Storage {
            message: message.into(),
            source: None,
        }
    }
}
