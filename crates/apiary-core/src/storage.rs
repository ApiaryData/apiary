//! The StorageBackend trait — the single interface for all storage operations.
//!
//! Every storage operation in Apiary goes through this trait. Implementations
//! include [`LocalBackend`](crate) (filesystem) and [`S3Backend`](crate)
//! (any S3-compatible endpoint). Application code never accesses the
//! filesystem or object storage directly.

use async_trait::async_trait;
use bytes::Bytes;

use crate::Result;

/// The unified storage interface for all Apiary operations.
///
/// All data — ledger entries, cell files, registry state, heartbeats —
/// is read and written through this trait. Implementations must be
/// `Send + Sync` for use across async tasks and threads.
///
/// # Conditional Writes
///
/// [`put_if_not_exists`](StorageBackend::put_if_not_exists) is the
/// serialisation mechanism for concurrent writes. It replaces consensus
/// protocols (Raft, Paxos) with a single atomic operation provided by
/// the storage layer.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Write an object. Overwrites if it already exists.
    async fn put(&self, key: &str, data: Bytes) -> Result<()>;

    /// Read an object. Returns [`ApiaryError::NotFound`] if the key does not exist.
    async fn get(&self, key: &str) -> Result<Bytes>;

    /// List all object keys matching the given prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Delete an object. Does not error if the key does not exist.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Conditional write: succeeds only if the key does not already exist.
    ///
    /// Returns `Ok(true)` if the write succeeded (key was created).
    /// Returns `Ok(false)` if the key already existed (no write performed).
    ///
    /// This is the atomic operation that provides write serialisation
    /// across concurrent nodes, replacing consensus protocols.
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> Result<bool>;

    /// Check if an object exists at the given key.
    async fn exists(&self, key: &str) -> Result<bool>;
}
