//! Depositing the crop into the comb.
//!
//! Every deposit interval a Node moves what it has ingested from its crop, on
//! local disk, into each Frame's Delta table, oldest segments first. The crop
//! drops a segment only after the commit that holds it is confirmed.
//!
//! A deposit of segments up to `n` commits with a Delta application transaction
//! `(crop id, n)`. So a deposit is idempotent: if the Node dies after the commit
//! but before it releases the segments, the next deposit reads `n` back from the
//! table and releases them instead of depositing them again.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, info, warn};

use crate::budget::CommitBudget;
use apiary_comb::{Comb, Crop, FrameCrop, FrameKey, Segment};
use apiary_core::registry_manager::RegistryManager;
use apiary_core::{ApiaryError, CommitGate, FrameSchema, Result};

/// What a deposit moved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DepositReport {
    /// Frames that had something deposited.
    pub frames: usize,
    /// Crop segments deposited.
    pub segments: usize,
    /// Rows deposited.
    pub rows: u64,
}

impl DepositReport {
    fn add(&mut self, other: &DepositReport) {
        self.frames += other.frames;
        self.segments += other.segments;
        self.rows += other.rows;
    }
}

/// Moves a Node's crop into the comb.
pub struct Depositor {
    comb: Arc<Comb>,
    crop: Arc<Crop>,
    registry: Arc<RegistryManager>,
    target_cell_size: u64,
    /// The most crop bytes one deposit commit takes from a Frame.
    max_deposit_bytes: u64,
    /// Held while depositing, so deposits never overlap.
    busy: Mutex<()>,
    /// Each Frame's commit budget, if the Node keeps one.
    budget: Option<Arc<CommitBudget>>,
    /// Asked before every commit: a Node with no trustworthy clock must not
    /// stamp the Delta log. Ingest is unaffected; the crop needs no wall time.
    gate: Option<CommitGate>,
}

impl Depositor {
    /// Create a depositor. Each deposit commit takes at most four target Cells'
    /// worth of crop (and always at least one segment).
    pub fn new(
        comb: Arc<Comb>,
        crop: Arc<Crop>,
        registry: Arc<RegistryManager>,
        target_cell_size: u64,
    ) -> Self {
        Self {
            comb,
            crop,
            registry,
            target_cell_size,
            max_deposit_bytes: target_cell_size.saturating_mul(4).max(1),
            busy: Mutex::new(()),
            budget: None,
            gate: None,
        }
    }

    /// Keep each Frame's deposits within `budget`.
    pub fn with_budget(mut self, budget: Arc<CommitBudget>) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Ask `gate` before every commit.
    pub fn with_gate(mut self, gate: CommitGate) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Wait until no deposit is running, and hold off the next one.
    pub(crate) async fn pause(&self) -> MutexGuard<'_, ()> {
        self.busy.lock().await
    }

    /// Open a Frame's Delta table, creating it from the registry's schema if it
    /// has not been written to yet.
    pub(crate) async fn open_table(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
    ) -> Result<apiary_comb::DeltaTable> {
        open_or_create_table(&self.comb, &self.registry, hive, box_name, frame).await
    }

    /// Deposit once: for each Frame, a bounded chunk of its oldest segments.
    ///
    /// A Frame that fails is skipped and the rest go ahead; the first error is
    /// returned afterwards. Whatever failed stays in the crop for next time.
    pub async fn deposit_once(&self) -> Result<DepositReport> {
        let _busy = self.busy.lock().await;
        self.run(false).await
    }

    /// Deposit once, for each Frame whose commit budget allows it: a bounded
    /// chunk of its oldest segments. A Frame out of budget keeps its crop, and
    /// its next deposit takes more. Returns the report and whether any Frame
    /// was held back.
    pub async fn deposit_within_budget(&self) -> Result<(DepositReport, bool)> {
        let _busy = self.busy.lock().await;
        let mut total = DepositReport::default();
        let mut held_back = false;
        let mut first_error: Option<ApiaryError> = None;

        let crop = Arc::clone(&self.crop);
        for key in blocking(move || crop.frames()).await? {
            let name = format!("{}.{}.{}", key.hive, key.box_name, key.frame);
            if let Some(budget) = &self.budget
                && budget.remaining(&name) == 0
            {
                held_back = true;
                continue;
            }
            match self.deposit_frame(&key).await {
                Ok(report) => {
                    if report.segments > 0
                        && let Some(budget) = &self.budget
                    {
                        budget.record(&name, 1);
                    }
                    total.add(&report);
                }
                Err(e) => {
                    warn!(frame = %name, error = %e, "Deposit failed; the rows stay in the crop");
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok((total, held_back)),
        }
    }

    /// Deposit everything, repeating until no Frame has anything pending.
    pub async fn flush(&self) -> Result<DepositReport> {
        let _busy = self.busy.lock().await;
        self.run(true).await
    }

    async fn run(&self, until_empty: bool) -> Result<DepositReport> {
        let mut total = DepositReport::default();
        let mut first_error: Option<ApiaryError> = None;

        let crop = Arc::clone(&self.crop);
        let keys = blocking(move || crop.frames()).await?;
        for key in keys {
            loop {
                match self.deposit_frame(&key).await {
                    Ok(report) if report.segments == 0 => break,
                    Ok(report) => {
                        total.add(&report);
                        if !until_empty {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(
                            frame = %format!("{}.{}.{}", key.hive, key.box_name, key.frame),
                            error = %e,
                            "Deposit failed; the rows stay in the crop"
                        );
                        first_error.get_or_insert(e);
                        break;
                    }
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(total),
        }
    }

    /// Deposit a bounded chunk of one Frame's oldest segments.
    async fn deposit_frame(&self, key: &FrameKey) -> Result<DepositReport> {
        let log = self.crop.frame(&key.hive, &key.box_name, &key.frame)?;
        if blocking({
            let log = Arc::clone(&log);
            move || log.pending()
        })
        .await?
        .is_empty()
        {
            return Ok(DepositReport::default());
        }

        if let Some(gate) = &self.gate {
            gate()?;
        }
        let table = self
            .open_table(&key.hive, &key.box_name, &key.frame)
            .await?;
        let app_id = self.crop.app_id();

        // Recovery: segments a previous run committed but did not release.
        let deposited = self.comb.deposited_version(&table, &app_id).await?;
        let pending = blocking({
            let log = Arc::clone(&log);
            move || {
                log.release(deposited)?;
                log.pending()
            }
        })
        .await?;
        if pending.is_empty() {
            return Ok(DepositReport::default());
        }

        // Oldest first, up to the cap (always at least one segment).
        let mut chosen: Vec<Segment> = Vec::new();
        let mut bytes = 0;
        for segment in pending {
            if !chosen.is_empty() && bytes + segment.bytes > self.max_deposit_bytes {
                break;
            }
            bytes += segment.bytes;
            chosen.push(segment);
        }
        let last = chosen.last().map(|s| s.seq).unwrap_or(0);

        let batch = blocking({
            let log = Arc::clone(&log);
            let chosen = chosen.clone();
            move || read_as_one(&log, &chosen)
        })
        .await?;

        let committed = self
            .comb
            .deposit(&table, &batch, self.target_cell_size, &app_id, last)
            .await?;
        // The commit is confirmed: now the crop may let go.
        blocking(move || log.release(last)).await?;

        debug!(
            frame = %format!("{}.{}.{}", key.hive, key.box_name, key.frame),
            segments = chosen.len(),
            rows = committed.rows,
            version = committed.version,
            "Deposited crop into the comb"
        );
        Ok(DepositReport {
            frames: 1,
            segments: chosen.len(),
            rows: committed.rows,
        })
    }

    /// Log what a deposit moved, if anything.
    pub(crate) fn log(report: &DepositReport) {
        if report.segments > 0 {
            info!(
                frames = report.frames,
                segments = report.segments,
                rows = report.rows,
                "Crop deposited"
            );
        }
    }
}

/// Open a Frame's Delta table, creating it from the registry's schema if it has
/// not been written to yet.
pub(crate) async fn open_or_create_table(
    comb: &Comb,
    registry: &RegistryManager,
    hive: &str,
    box_name: &str,
    frame: &str,
) -> Result<apiary_comb::DeltaTable> {
    if let Some(table) = comb.open_frame_table(hive, box_name, frame).await? {
        return Ok(table);
    }
    let declared = registry.get_frame(hive, box_name, frame).await?;
    let schema = FrameSchema::from_json_value(&declared.schema)?;
    comb.create_frame_table(hive, box_name, frame, &schema, &declared.partition_by)
        .await
}

/// Read segments as one batch.
fn read_as_one(log: &FrameCrop, segments: &[Segment]) -> Result<RecordBatch> {
    let batches = log.read(segments)?;
    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| ApiaryError::Internal {
            message: "A crop segment held no batches".into(),
        })?;
    concat_batches(&schema, &batches).map_err(|e| ApiaryError::Internal {
        message: format!("Failed to merge crop segments: {e}"),
    })
}

/// Run blocking file work off the async threads.
async fn blocking<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| ApiaryError::Internal {
            message: format!("Crop task failed: {e}"),
        })?
}
