//! Storage backend implementations for Apiary.
//!
//! This crate provides concrete implementations of the
//! [`StorageBackend`](apiary_core::StorageBackend) trait:
//!
//! - [`LocalBackend`] — filesystem-backed storage for solo mode and development
//! - [`S3Backend`] — S3-compatible object storage for multi-node deployments

pub mod local;
pub mod s3;

pub use local::LocalBackend;
pub use s3::S3Backend;
