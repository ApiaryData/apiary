//! Ingest, and the standing queries that run over what is ingested.
//!
//! A batch lands in the crop, synced to disk, before ingest returns. A **standing
//! query** is a rollup registered once: each time a batch lands for its source
//! Frame, the Node queues a Patch that runs the query over that batch and ingests
//! the result into the query's target Frame, where it is a Frame like any other:
//! in the crop at once, deposited with the rest. Because the rollup runs over the
//! batch on the Node that ingested it, partial aggregates (sums and counts, not
//! averages) go into the target and are combined when the target is queried.
//!
//! The Patch is a Forager's work, one the Node's Bees come back to with every
//! batch, as foragers return to a good source. The queue is bounded: a Node whose
//! rollups cannot keep up with ingest counts the batches it skipped, rather than
//! queueing without end, until backpressure arrives with the tremble signals.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use apiary_colony::Colony;
use apiary_comb::Crop;
use apiary_comb::schema::{conform_batch, delta_schema};
use apiary_core::registry_manager::RegistryManager;
use apiary_core::storage::StorageBackend;
use apiary_core::{ApiaryError, FrameSchema, Result};
use apiary_plan::ApiaryQueryContext;

use crate::duties::NodeDuties;

/// What one ingest landed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestResult {
    /// Rows landed in the crop.
    pub rows: u64,
    /// The crop segment they were written as (`None` for an empty batch).
    pub segment: Option<u64>,
    /// Bytes ingested since the crop was last deposited.
    pub crop_bytes: u64,
}

/// What ingest needs to know about a Frame, resolved once.
pub(crate) struct FrameSpec {
    /// The schema the Frame's table stores.
    pub schema: SchemaRef,
    pub partition_by: Vec<String>,
}

/// A rollup that runs over every batch ingested for a Frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandingQuery {
    /// Its name, unique within the Box.
    pub name: String,
    /// The Hive of both Frames.
    pub hive: String,
    /// The Box of both Frames.
    pub box_name: String,
    /// The Frame whose new batches it runs over.
    pub source: String,
    /// The Frame it ingests its result into.
    pub target: String,
    /// The query. It reads the batch as the table `batch`, and may also read other
    /// Frames by their three-part names.
    pub sql: String,
}

impl StandingQuery {
    fn key(&self) -> String {
        format!(
            "standing/{}.{}.{}.json",
            self.hive, self.box_name, self.name
        )
    }
}

/// How the standing queries have fared.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StandingCounts {
    /// Patches queued.
    pub queued: u64,
    /// Patches that ran and ingested their result.
    pub ran: u64,
    /// Patches that failed.
    pub failed: u64,
    /// Batches skipped because too many Patches were waiting.
    pub skipped: u64,
    /// Rows the rollups ingested into their targets.
    pub rows: u64,
}

#[derive(Default)]
struct StandingCounters {
    queued: AtomicU64,
    ran: AtomicU64,
    failed: AtomicU64,
    skipped: AtomicU64,
    rows: AtomicU64,
}

/// The most standing Patches that may wait for a Forager.
const STANDING_LIMIT: usize = 4096;

/// A Node's ingest path.
pub(crate) struct Ingestor {
    node: String,
    registry: Arc<RegistryManager>,
    crop: Arc<Crop>,
    store: Arc<dyn StorageBackend>,
    specs: RwLock<HashMap<(String, String, String), Arc<FrameSpec>>>,
    crop_bytes: Arc<AtomicU64>,
    crop_max_bytes: u64,
    colony: Arc<Colony>,
    duties: Arc<NodeDuties>,
    query_ctx: Arc<ApiaryQueryContext>,
    standing: Mutex<Vec<Arc<StandingQuery>>>,
    counters: StandingCounters,
}

impl Ingestor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        node: String,
        registry: Arc<RegistryManager>,
        crop: Arc<Crop>,
        store: Arc<dyn StorageBackend>,
        crop_bytes: Arc<AtomicU64>,
        crop_max_bytes: u64,
        colony: Arc<Colony>,
        duties: Arc<NodeDuties>,
        query_ctx: Arc<ApiaryQueryContext>,
    ) -> Self {
        Self {
            node,
            registry,
            crop,
            store,
            specs: RwLock::new(HashMap::new()),
            crop_bytes,
            crop_max_bytes,
            colony,
            duties,
            query_ctx,
            standing: Mutex::new(Vec::new()),
            counters: StandingCounters::default(),
        }
    }

    fn mark(&self, kind: &str, detail: impl std::fmt::Display) {
        tracing::info!(target: "apiary::mark", node = %self.node, kind, "{detail}");
    }

    /// What ingest needs to know about a Frame, from the registry the first time
    /// and from memory after.
    pub(crate) async fn frame_spec(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
    ) -> Result<Arc<FrameSpec>> {
        let key = (
            hive.to_string(),
            box_name.to_string(),
            frame_name.to_string(),
        );
        if let Some(spec) = self.specs.read().await.get(&key) {
            return Ok(Arc::clone(spec));
        }
        let frame = self.registry.get_frame(hive, box_name, frame_name).await?;
        let spec = Arc::new(FrameSpec {
            schema: delta_schema(&FrameSchema::from_json_value(&frame.schema)?),
            partition_by: frame.partition_by.clone(),
        });
        self.specs.write().await.insert(key, Arc::clone(&spec));
        Ok(spec)
    }

    /// Land a batch in the crop, and queue the Patches of the standing queries
    /// that run over it.
    pub(crate) async fn ingest(
        self: &Arc<Self>,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        batch: &RecordBatch,
    ) -> Result<IngestResult> {
        let spec = self.frame_spec(hive, box_name, frame_name).await?;
        let conformed = conform_batch(batch, &spec.schema, &spec.partition_by)?;
        let rows = conformed.num_rows() as u64;

        let log = self.crop.frame(hive, box_name, frame_name)?;
        let appended = conformed.clone();
        let segment = tokio::task::spawn_blocking(move || log.append(&appended))
            .await
            .map_err(|e| ApiaryError::Internal {
                message: format!("Ingest task failed: {e}"),
            })??;

        let bytes = segment.as_ref().map_or(0, |s| s.bytes);
        let pending = self.crop_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if pending >= self.crop_max_bytes {
            self.colony.wake();
        }
        self.mark(
            "ingest",
            format_args!("{hive}.{box_name}.{frame_name} {rows} rows"),
        );
        if rows > 0 {
            self.queue_standing(hive, box_name, frame_name, &conformed);
        }
        Ok(IngestResult {
            rows,
            segment: segment.map(|s| s.seq),
            crop_bytes: pending,
        })
    }

    /// Queue a Patch for each standing query that runs over `batch`.
    fn queue_standing(
        self: &Arc<Self>,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        batch: &RecordBatch,
    ) {
        let queries: Vec<Arc<StandingQuery>> = self
            .standing
            .lock()
            .expect("standing queries")
            .iter()
            .filter(|q| q.hive == hive && q.box_name == box_name && q.source == frame_name)
            .cloned()
            .collect();
        for query in queries {
            let me = Arc::clone(self);
            let batch = batch.clone();
            let queued = self.duties.submit_standing(
                STANDING_LIMIT,
                Box::new(move |pool| {
                    Box::pin(async move {
                        me.run_standing(&query, batch, pool).await;
                    })
                }),
            );
            if queued {
                self.counters.queued.fetch_add(1, Ordering::Relaxed);
                self.colony.wake();
            } else {
                self.counters.skipped.fetch_add(1, Ordering::Relaxed);
                warn!("A standing query was skipped: too many Patches are waiting");
            }
        }
    }

    /// Run a standing query over a batch and ingest its result into its target.
    async fn run_standing(
        self: &Arc<Self>,
        query: &StandingQuery,
        batch: RecordBatch,
        pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) {
        let result = async {
            let session = self
                .query_ctx
                .local_session(Some(pool), vec![("batch", batch)])?;
            let out = session
                .sql(&query.sql)
                .await
                .map_err(|e| ApiaryError::Internal {
                    message: format!("Standing query '{}': {e}", query.name),
                })?
                .collect()
                .await
                .map_err(|e| ApiaryError::Internal {
                    message: format!("Standing query '{}': {e}", query.name),
                })?;
            let mut rows = 0;
            for rolled in out.iter().filter(|b| b.num_rows() > 0) {
                rows += self
                    .ingest_boxed(&query.hive, &query.box_name, &query.target, rolled)
                    .await?
                    .rows;
            }
            Ok::<u64, ApiaryError>(rows)
        }
        .await;
        match result {
            Ok(rows) => {
                self.counters.ran.fetch_add(1, Ordering::Relaxed);
                self.counters.rows.fetch_add(rows, Ordering::Relaxed);
                self.mark(
                    "standing",
                    format_args!("{} rolled up {rows} rows", query.name),
                );
            }
            Err(e) => {
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, query = %query.name, "A standing query failed");
            }
        }
    }

    /// `ingest`, boxed so a standing query's result can itself feed another.
    fn ingest_boxed<'a>(
        self: &'a Arc<Self>,
        hive: &'a str,
        box_name: &'a str,
        frame: &'a str,
        batch: &'a RecordBatch,
    ) -> futures::future::BoxFuture<'a, Result<IngestResult>> {
        Box::pin(self.ingest(hive, box_name, frame, batch))
    }

    /// Register a standing query and keep it in the comb store, so it survives a
    /// restart and every Node of the colony reads the same ones when it starts.
    pub(crate) async fn register_standing(&self, query: StandingQuery) -> Result<()> {
        if query.source == query.target {
            return Err(ApiaryError::Config {
                message: format!(
                    "Standing query '{}' would ingest into the Frame it reads",
                    query.name
                ),
            });
        }
        // Both Frames must exist, and the query must run over the source's schema.
        let source = self
            .frame_spec(&query.hive, &query.box_name, &query.source)
            .await?;
        self.frame_spec(&query.hive, &query.box_name, &query.target)
            .await?;
        let session = self.query_ctx.local_session(
            None,
            vec![("batch", RecordBatch::new_empty(Arc::clone(&source.schema)))],
        )?;
        session
            .sql(&query.sql)
            .await
            .map_err(|e| ApiaryError::Config {
                message: format!("Standing query '{}' does not plan: {e}", query.name),
            })?;

        let body = serde_json::to_vec(&query).map_err(|e| ApiaryError::Internal {
            message: format!("Cannot write the standing query: {e}"),
        })?;
        self.store.put(&query.key(), Bytes::from(body)).await?;
        self.install(query);
        Ok(())
    }

    fn install(&self, query: StandingQuery) {
        let mut standing = self.standing.lock().expect("standing queries");
        standing.retain(|q| {
            !(q.hive == query.hive && q.box_name == query.box_name && q.name == query.name)
        });
        standing.push(Arc::new(query));
    }

    /// Remove a standing query.
    pub(crate) async fn remove_standing(
        &self,
        hive: &str,
        box_name: &str,
        name: &str,
    ) -> Result<()> {
        self.standing
            .lock()
            .expect("standing queries")
            .retain(|q| !(q.hive == hive && q.box_name == box_name && q.name == name));
        self.store
            .delete(&format!("standing/{hive}.{box_name}.{name}.json"))
            .await
    }

    /// Read the standing queries kept in the comb store.
    pub(crate) async fn load_standing(&self) -> Result<()> {
        for key in self.store.list("standing/").await? {
            match self.store.get(&key).await {
                Ok(bytes) => match serde_json::from_slice::<StandingQuery>(&bytes) {
                    Ok(query) => self.install(query),
                    Err(e) => debug!(%key, error = %e, "Unreadable standing query"),
                },
                Err(e) => debug!(%key, error = %e, "Cannot read a standing query"),
            }
        }
        Ok(())
    }

    /// The standing queries now registered.
    pub(crate) fn standing_queries(&self) -> Vec<StandingQuery> {
        self.standing
            .lock()
            .expect("standing queries")
            .iter()
            .map(|q| (**q).clone())
            .collect()
    }

    /// How the standing queries have fared.
    pub(crate) fn standing_counts(&self) -> StandingCounts {
        let c = &self.counters;
        StandingCounts {
            queued: c.queued.load(Ordering::Relaxed),
            ran: c.ran.load(Ordering::Relaxed),
            failed: c.failed.load(Ordering::Relaxed),
            skipped: c.skipped.load(Ordering::Relaxed),
            rows: c.rows.load(Ordering::Relaxed),
        }
    }
}
