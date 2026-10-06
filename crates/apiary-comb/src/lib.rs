//! Storage backend implementations and data operations for Apiary.
//!
//! This crate provides concrete implementations of the
//! [`StorageBackend`](apiary_core::StorageBackend) trait:
//!
//! - [`LocalBackend`] — filesystem-backed storage for solo mode and development
//! - [`S3Backend`] — S3-compatible object storage for multi-node deployments
//!
//! It also provides the transaction ledger, cell writer, and cell reader:
//!
//! - [`Ledger`] — ACID transaction log for frames
//! - [`CellWriter`] — Parquet cell writing with partitioning and statistics
//! - [`CellReader`] — Parquet cell reading with projection pushdown

pub mod cell_reader;
pub mod cell_writer;
pub mod ledger;
pub mod local;
pub mod s3;

pub use cell_reader::CellReader;
pub use cell_writer::CellWriter;
pub use ledger::Ledger;
pub use local::LocalBackend;
pub use s3::S3Backend;
