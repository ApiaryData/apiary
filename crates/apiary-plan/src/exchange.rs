//! Handing batches from one stage to the next.
//!
//! A stage boundary in a query is a forager handing nectar to a receiver. Each
//! Patch of the stage above writes its output partitions to an [`Exchange`], and
//! the stage below reads, for each of its partitions, the matching output of
//! every Patch above. The exchange kept in the comb store ([`StoreExchange`]) is
//! the fallback that works between any two Nodes; Nodes in one colony will also
//! be able to hand batches over directly.
//!
//! # One winner per Patch
//!
//! Two Bees can run one Patch (a straggler is re-advertised, two Followers claim
//! it before gossip tells them). Each attempt writes its outputs under its own
//! name and then tries to create the Patch's completion record, which can be
//! created only once. The attempt that creates it wins and readers use its files;
//! the other discards its work. A Patch is therefore committed exactly once, with
//! no coordination between Bees.

use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::error::{DataFusionError, Result as DfResult};
use serde::{Deserialize, Serialize};

use apiary_core::{ApiaryError, Result, StorageBackend};

/// The address of one Patch of one query: a stage, and an index within it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PatchKey {
    /// The query.
    pub query: String,
    /// The stage within the query.
    pub stage: usize,
    /// The Patch within the stage: the partition of the stage's plan it runs.
    pub index: usize,
}

impl PatchKey {
    /// The key of Patch `index` of `stage` in `query`.
    pub fn new(query: impl Into<String>, stage: usize, index: usize) -> Self {
        Self {
            query: query.into(),
            stage,
            index,
        }
    }
}

/// What the winning attempt at a Patch recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    /// The attempt whose files are the Patch's output.
    pub attempt: String,
    /// The Node that ran it.
    pub node: String,
    /// Rows in each output partition.
    pub rows: Vec<u64>,
    /// Bytes in each output partition.
    pub bytes: Vec<u64>,
}

/// How an attempt's bid to complete a Patch went.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Completed {
    /// This attempt's record was created: its outputs are the Patch's.
    Won,
    /// Another attempt got there first; this one's work is surplus.
    Lost(Completion),
}

/// Where stages hand batches to each other.
#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    /// Store output partition `output` of an attempt at `patch`.
    async fn put(&self, patch: &PatchKey, attempt: &str, output: usize, data: Bytes) -> Result<()>;

    /// Try to complete `patch` with `record`. Exactly one attempt wins.
    async fn complete(&self, patch: &PatchKey, record: &Completion) -> Result<Completed>;

    /// How `patch` was completed, if it has been.
    async fn completion(&self, patch: &PatchKey) -> Result<Option<Completion>>;

    /// Read output partition `output` of a completed Patch.
    async fn get(&self, patch: &PatchKey, done: &Completion, output: usize) -> Result<Bytes>;

    /// Remove everything a query left behind.
    async fn clear(&self, query: &str) -> Result<()>;
}

/// The exchange kept in the comb store, under `outputs/`.
pub struct StoreExchange {
    store: Arc<dyn StorageBackend>,
}

impl StoreExchange {
    /// An exchange over `store`.
    pub fn new(store: Arc<dyn StorageBackend>) -> Self {
        Self { store }
    }

    fn dir(patch: &PatchKey) -> String {
        format!("outputs/{}/{}/{}", patch.query, patch.stage, patch.index)
    }

    fn file(patch: &PatchKey, attempt: &str, output: usize) -> String {
        format!("{}/{attempt}/{output}.arrow", Self::dir(patch))
    }

    fn done(patch: &PatchKey) -> String {
        format!("{}/done.json", Self::dir(patch))
    }
}

fn codec(e: serde_json::Error) -> ApiaryError {
    ApiaryError::Internal {
        message: format!("Unreadable completion record: {e}"),
    }
}

#[async_trait]
impl Exchange for StoreExchange {
    async fn put(&self, patch: &PatchKey, attempt: &str, output: usize, data: Bytes) -> Result<()> {
        self.store
            .put(&Self::file(patch, attempt, output), data)
            .await
    }

    async fn complete(&self, patch: &PatchKey, record: &Completion) -> Result<Completed> {
        let body = serde_json::to_vec(record).map_err(codec)?;
        if self
            .store
            .put_if_not_exists(&Self::done(patch), Bytes::from(body))
            .await?
        {
            return Ok(Completed::Won);
        }
        match self.completion(patch).await? {
            // Our own record, found after a reply to our create was lost: still ours.
            Some(existing) if existing.attempt == record.attempt => Ok(Completed::Won),
            Some(existing) => {
                // Surplus work: take it away.
                for output in 0..record.rows.len() {
                    self.store
                        .delete(&Self::file(patch, &record.attempt, output))
                        .await?;
                }
                Ok(Completed::Lost(existing))
            }
            None => Err(ApiaryError::Internal {
                message: format!(
                    "The completion record of {patch:?} exists but cannot be read back"
                ),
            }),
        }
    }

    async fn completion(&self, patch: &PatchKey) -> Result<Option<Completion>> {
        match self.store.get(&Self::done(patch)).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(codec)?)),
            Err(ApiaryError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get(&self, patch: &PatchKey, done: &Completion, output: usize) -> Result<Bytes> {
        self.store
            .get(&Self::file(patch, &done.attempt, output))
            .await
    }

    async fn clear(&self, query: &str) -> Result<()> {
        for key in self.store.list(&format!("outputs/{query}/")).await? {
            self.store.delete(&key).await?;
        }
        Ok(())
    }
}

/// A schema as bytes: an Arrow IPC stream with no batches.
pub fn schema_to_bytes(schema: &SchemaRef) -> DfResult<Vec<u8>> {
    let mut out = Vec::new();
    StreamWriter::try_new(&mut out, schema)?.finish()?;
    Ok(out)
}

/// A schema from [`schema_to_bytes`].
pub fn schema_from_bytes(bytes: &[u8]) -> DfResult<SchemaRef> {
    Ok(StreamReader::try_new(Cursor::new(bytes), None)?.schema())
}

/// Batches as an Arrow IPC stream.
pub fn encode_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> DfResult<Bytes> {
    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(Bytes::from(out))
}

/// Batches from an Arrow IPC stream.
pub fn decode_batches(bytes: Bytes) -> DfResult<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(DataFusionError::from)
}
