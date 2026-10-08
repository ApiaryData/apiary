//! A Frame as queries see it: the comb plus the crop, with a `_stage` column.
//!
//! Rows in a Frame live in one of two places at any moment. Rows already
//! deposited are in the Frame's Delta table (`_stage = 'comb'`); rows ingested
//! since the last deposit are on this Node's disk (`_stage = 'crop'`). A query
//! sees both. This module builds that view, and reports how many rows each
//! stage gave a query.
//!
//! # Seeing each row once
//!
//! A deposit commits to the table and only then releases the crop's segments,
//! so for a moment a row is in both places. To count it once, the loader reads
//! the table's snapshot first, learns from it the highest crop segment already
//! deposited, and takes only newer segments from the crop. A deposit can still
//! land between the two reads. The crop marks every release, and a released
//! segment is deleted only after the table has it, so the loader retries
//! whenever it finds a segment missing or a release newer than its snapshot. A
//! retry sees the rows in exactly one place.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::Column;
use datafusion::datasource::{MemTable, ViewTable, provider_as_source};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, lit};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use tracing::warn;

use apiary_comb::schema::delta_schema;
use apiary_comb::{Crop, STAGE_COLUMN, Segment};
use apiary_core::{ApiaryError, FrameSchema, Result};

use crate::catalog::{CatalogShared, add_file_discovery, add_metadata_read};

/// Schema metadata that marks the crop's scan, so the rows a query read from it
/// can be told apart from the comb's.
pub(crate) const STAGE_MARKER: &str = "apiary.stage";

/// Schema metadata key under which results report rows read from the crop.
pub const ROWS_FROM_CROP: &str = "apiary.rows.crop";

/// Schema metadata key under which results report rows read from the comb.
pub const ROWS_FROM_COMB: &str = "apiary.rows.comb";

/// How many times a load retries when a deposit lands underneath it. A deposit
/// landing during every one of them is not plausible; a crop that is simply
/// ahead of its table (the comb was wiped or restored) is, and retrying cannot
/// fix it, so the last attempt serves what it can.
const MAX_ATTEMPTS: usize = 5;

/// Rows a query read from each stage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StageRows {
    /// Rows read from this Node's crop: ingested, not yet deposited.
    pub crop: u64,
    /// Rows read from the comb: committed to the Frame's Delta table.
    pub comb: u64,
}

/// What a crop read found.
enum CropRead {
    Rows(Vec<RecordBatch>),
    /// A deposit landed during the read; load again.
    Retry,
}

/// Load a Frame: its table and its crop rows, as one queryable view.
pub(crate) async fn load_frame(
    shared: &CatalogShared,
    hive: &str,
    box_name: &str,
    frame: &str,
    declared: &FrameSchema,
) -> Result<Arc<dyn TableProvider>> {
    let schema = delta_schema(declared);

    let mut attempt = 0;
    let (table, crop_rows) = loop {
        attempt += 1;
        let started = Instant::now();

        // The table first: its snapshot says how much of the crop is deposited.
        let table = shared.comb.open_frame_table(hive, box_name, frame).await?;
        let crop_rows = match &shared.crop {
            None => Vec::new(),
            Some(crop) => {
                let deposited = match &table {
                    Some(table) => shared.comb.deposited_version(table, &crop.app_id()).await?,
                    None => 0,
                };
                let last_attempt = attempt >= MAX_ATTEMPTS;
                match read_crop(crop, hive, box_name, frame, deposited, last_attempt).await? {
                    CropRead::Rows(rows) => rows,
                    CropRead::Retry => continue,
                }
            }
        };
        add_file_discovery(started.elapsed());
        break (table, crop_rows);
    };

    // Build the scans.
    let scan_started = Instant::now();
    let comb_provider: Arc<dyn TableProvider> = match table {
        Some(table) => {
            shared
                .comb
                .table_provider(&shared.scan_state, &table)
                .await?
        }
        None => empty_table(&schema)?,
    };
    let provider = staged_view(comb_provider, &schema, crop_rows)?;
    add_metadata_read(scan_started.elapsed());
    Ok(provider)
}

/// Read the crop's rows newer than `deposited`, off the async threads.
///
/// Unless `best_effort`, a deposit landing during the read asks for a retry.
/// A best-effort read serves the segments that are there.
async fn read_crop(
    crop: &Arc<Crop>,
    hive: &str,
    box_name: &str,
    frame: &str,
    deposited: u64,
    best_effort: bool,
) -> Result<CropRead> {
    let crop = Arc::clone(crop);
    let (hive, box_name, frame) = (hive.to_string(), box_name.to_string(), frame.to_string());
    tokio::task::spawn_blocking(move || {
        let Some(log) = crop.frame_if_exists(&hive, &box_name, &frame)? else {
            return Ok(CropRead::Rows(Vec::new()));
        };
        let segments: Vec<Segment> = log
            .pending()?
            .into_iter()
            .filter(|s| s.seq > deposited)
            .collect();
        // A release newer than the table snapshot means a deposit landed
        // after the snapshot was read; the rows are now only in the table.
        if log.released()? > deposited {
            if !best_effort {
                return Ok(CropRead::Retry);
            }
            warn!(
                frame = %format!("{hive}.{box_name}.{frame}"),
                deposited,
                "The crop is ahead of the table (was the comb wiped or restored?); \
                 serving the rows the crop still has"
            );
        }
        if best_effort {
            // Take each segment that is still there.
            let mut rows = Vec::new();
            for segment in &segments {
                match log.read(std::slice::from_ref(segment)) {
                    Ok(batches) => rows.extend(batches),
                    Err(ApiaryError::NotFound { .. }) => {}
                    Err(e) => return Err(e),
                }
            }
            return Ok(CropRead::Rows(rows));
        }
        match log.read(&segments) {
            Ok(rows) => Ok(CropRead::Rows(rows)),
            // A segment vanished between listing and reading: same cause.
            Err(ApiaryError::NotFound { .. }) => Ok(CropRead::Retry),
            Err(e) => Err(e),
        }
    })
    .await
    .map_err(|e| ApiaryError::Internal {
        message: format!("Crop read failed: {e}"),
    })?
}

fn empty_table(schema: &SchemaRef) -> Result<Arc<dyn TableProvider>> {
    let empty = RecordBatch::new_empty(Arc::clone(schema));
    let table = MemTable::try_new(empty.schema(), vec![vec![empty]]).map_err(|e| {
        ApiaryError::Internal {
            message: format!("Failed to build an empty table: {e}"),
        }
    })?;
    Ok(Arc::new(table))
}

/// The columns of `schema`, by name, followed by `_stage` set to `stage`.
fn with_stage(schema: &SchemaRef, stage: &str) -> Vec<Expr> {
    schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .chain(std::iter::once(lit(stage).alias(STAGE_COLUMN)))
        .collect()
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiaryError {
    ApiaryError::Internal {
        message: format!("{context}: {e}"),
    }
}

/// The comb scan unioned with the crop's rows, each tagged with its stage.
fn staged_view(
    comb: Arc<dyn TableProvider>,
    schema: &SchemaRef,
    crop_rows: Vec<RecordBatch>,
) -> Result<Arc<dyn TableProvider>> {
    let comb_plan = LogicalPlanBuilder::scan("comb", provider_as_source(comb), None)
        .and_then(|b| b.project(with_stage(schema, "comb")))
        .and_then(|b| b.build())
        .map_err(|e| internal("Failed to plan the comb scan", e))?;

    let crop_rows: Vec<RecordBatch> = crop_rows.into_iter().filter(|b| b.num_rows() > 0).collect();
    let plan = if crop_rows.is_empty() {
        comb_plan
    } else {
        // Mark the crop's scan so a query's row counts can tell it from the comb.
        let mut metadata = HashMap::new();
        metadata.insert(STAGE_MARKER.to_string(), "crop".to_string());
        let marked: SchemaRef =
            Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata));
        let batches = crop_rows
            .into_iter()
            .map(|b| RecordBatch::try_new(Arc::clone(&marked), b.columns().to_vec()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| internal("Crop rows do not match the frame schema", e))?;
        let table = MemTable::try_new(Arc::clone(&marked), vec![batches])
            .map_err(|e| internal("Failed to build the crop scan", e))?;

        let crop_plan = LogicalPlanBuilder::scan("crop", provider_as_source(Arc::new(table)), None)
            .and_then(|b| b.project(with_stage(schema, "crop")))
            .and_then(|b| b.build())
            .map_err(|e| internal("Failed to plan the crop scan", e))?;
        LogicalPlanBuilder::from(comb_plan)
            .union(crop_plan)
            .and_then(|b| b.build())
            .map_err(|e| internal("Failed to union the comb and the crop", e))?
    };

    Ok(Arc::new(ViewTable::new(plan, None)))
}

/// Rows each stage gave an executed plan: the output of its scans.
///
/// The crop's scan carries [`STAGE_MARKER`] in its schema; every other table
/// scan is the comb's.
pub(crate) fn stage_rows(plan: &Arc<dyn ExecutionPlan>) -> StageRows {
    let mut rows = StageRows::default();
    visit(plan, &mut rows);
    rows
}

fn visit(plan: &Arc<dyn ExecutionPlan>, rows: &mut StageRows) {
    let children = plan.children();
    if children.is_empty() {
        // Leaves that are not table scans produce no rows from a stage.
        if matches!(
            plan.name(),
            "PlaceholderRowExec" | "EmptyExec" | "WorkTableExec"
        ) {
            return;
        }
        let is_crop = plan
            .schema()
            .metadata()
            .get(STAGE_MARKER)
            .is_some_and(|stage| stage == "crop");
        if is_crop {
            // The crop is scanned from memory, which keeps no row metrics, and
            // scans all of its rows: its exact row count is what was read.
            rows.crop += StatisticsContext::new()
                .compute(plan.as_ref(), &StatisticsArgs::new())
                .ok()
                .and_then(|stats| stats.num_rows.get_value().copied())
                .unwrap_or(0) as u64;
        } else {
            // Table scans report the rows they produced.
            rows.comb += plan.metrics().and_then(|m| m.output_rows()).unwrap_or(0) as u64;
        }
        return;
    }
    for child in children {
        visit(child, rows);
    }
}

/// A result schema with the stage row counts attached, as each batch's is.
pub(crate) fn schema_with_stage_metadata(
    schema: arrow::datatypes::SchemaRef,
    stages: StageRows,
) -> arrow::datatypes::SchemaRef {
    let mut metadata = schema.metadata().clone();
    metadata.insert(ROWS_FROM_CROP.to_string(), stages.crop.to_string());
    metadata.insert(ROWS_FROM_COMB.to_string(), stages.comb.to_string());
    Arc::new(schema.as_ref().clone().with_metadata(metadata))
}

/// Attach a query's stage row counts to the schema of each result batch, so
/// they reach whoever reads the results (Python, Arrow IPC, Flight).
pub(crate) fn with_stage_metadata(
    batches: Vec<RecordBatch>,
    stages: StageRows,
) -> Vec<RecordBatch> {
    batches
        .into_iter()
        .map(|batch| {
            let mut metadata = batch.schema().metadata().clone();
            metadata.insert(ROWS_FROM_CROP.to_string(), stages.crop.to_string());
            metadata.insert(ROWS_FROM_COMB.to_string(), stages.comb.to_string());
            let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
            RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap_or(batch)
        })
        .collect()
}
