//! Looking after the comb: capping, harvest, retirement and clearing.
//!
//! - **Capping** merges a Frame's small nectar Cells to the standard size,
//!   ripens them with the Frame's [`Recipe`], and replaces them with capped
//!   Cells in one commit that changes no data.
//! - **Harvest** copies capped Cells, oldest first, into the Frame's harvest
//!   table on another comb (the cloud bucket). Only capped Cells are taken.
//! - **Retiring** removes harvested Cells from the site table once they are past
//!   the site's retention window. A Cell that is not in the harvest never goes.
//! - **Clearing** deletes the files no table version needs any more.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use arrow::array::{ArrayRef, StringArray};
use arrow::compute::{cast, concat_batches};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use deltalake::kernel::Action;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::protocol::DeltaOperation;
use deltalake::writer::RecordBatchWriter;
use deltalake::{DeltaTable, DeltaTableError};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, info, warn};

use apiary_core::{ApiaryError, FrameSchema, Result};

use crate::cell::{Capped, Cell, Nectar, Recipe, Ripe, RipenessChecks, state_of};
use crate::comb::{CellState, Comb, delta_err};
use crate::schema::conform_batch;

/// A Cell's partition values, in a fixed order.
type PartitionKey = Vec<(String, Option<String>)>;

/// How capping chooses what to merge.
#[derive(Clone, Debug)]
pub struct CapOptions {
    /// The standard Cell size: nectar is merged up to this many bytes.
    pub target_cell_size: u64,
    /// A group of nectar smaller than half the standard size waits for more
    /// until its oldest Cell is this old, then is capped anyway.
    pub max_age: Duration,
    /// The time now, in milliseconds since the epoch.
    pub now_ms: i64,
}

/// What a capping pass did to one Frame.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CapReport {
    /// Nectar Cells replaced.
    pub nectar_cells: usize,
    /// Capped Cells written.
    pub capped_cells: usize,
    /// Rows in the capped Cells (fewer than went in if the recipe removed duplicates).
    pub rows: u64,
    /// Groups abandoned because a user write got in the way.
    pub aborted: usize,
}

impl CapReport {
    /// Add another report's counts.
    pub fn add(&mut self, other: &CapReport) {
        self.nectar_cells += other.nectar_cells;
        self.capped_cells += other.capped_cells;
        self.rows += other.rows;
        self.aborted += other.aborted;
    }
}

/// What a harvest pass did to one Frame.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HarvestReport {
    /// Capped Cells copied to the harvest table.
    pub cells: usize,
    /// Bytes copied.
    pub bytes: u64,
    /// Capped Cells still waiting (the pass hit its byte budget).
    pub remaining: usize,
}

impl HarvestReport {
    /// Add another report's counts.
    pub fn add(&mut self, other: &HarvestReport) {
        self.cells += other.cells;
        self.bytes += other.bytes;
        self.remaining += other.remaining;
    }
}

impl Comb {
    /// Set a Frame's ripening recipe, stored in its table properties.
    ///
    /// Every named column must exist in the table.
    pub async fn set_recipe(&self, table: &DeltaTable, recipe: &Recipe) -> Result<DeltaTable> {
        let schema = table_arrow_schema(table)?;
        for name in recipe.sort_by.iter().chain(&recipe.dedup_by) {
            if name.contains(',') || schema.index_of(name).is_err() {
                return Err(ApiaryError::Schema {
                    message: format!("Recipe column '{name}' is not a column of the frame"),
                });
            }
        }
        table
            .clone()
            .set_tbl_properties()
            .with_properties(recipe.to_properties())
            .with_raise_if_not_exists(false)
            .await
            .map_err(|e| delta_err("Failed to store the recipe", e))
    }

    /// A Frame's ripening recipe (empty if none was set).
    pub fn recipe(&self, table: &DeltaTable) -> Result<Recipe> {
        let snapshot = table
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?;
        Ok(Recipe::from_properties(snapshot.metadata().configuration()))
    }

    /// Merge, ripen and cap a Frame's nectar.
    ///
    /// Nectar is grouped by partition, oldest first, into groups up to the
    /// standard Cell size. A group is capped once it is at least half a standard
    /// Cell or its oldest Cell is older than `options.max_age`. Each group is
    /// its own commit, marked as changing no data. If a user write (a delete or
    /// an overwrite) touched the same Cells, the group is abandoned and its
    /// new files removed: ripening yields to users.
    pub async fn cap(&self, table: &DeltaTable, options: &CapOptions) -> Result<CapReport> {
        let recipe = self.recipe(table)?;
        let snapshot = table
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?;
        let partition_by = snapshot.metadata().partition_columns().to_vec();
        let schema = table_arrow_schema(table)?;

        #[allow(deprecated)]
        let mut nectar: Vec<Cell<Nectar>> = snapshot
            .log_data()
            .into_iter()
            .filter_map(|file| Cell::from_add(file.add_action()))
            .collect();
        nectar.sort_by(|a, b| {
            a.modified_ms()
                .cmp(&b.modified_ms())
                .then(a.path().cmp(b.path()))
        });

        let mut by_partition: BTreeMap<PartitionKey, Vec<Cell<Nectar>>> = BTreeMap::new();
        for cell in nectar {
            let mut key: Vec<_> = cell
                .partition_values()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            key.sort();
            by_partition.entry(key).or_default().push(cell);
        }

        let max_age_ms = options.max_age.as_millis() as i64;
        let store = table.log_store().object_store(None);
        let mut report = CapReport::default();

        for cells in by_partition.into_values() {
            for group in chunk(cells, options.target_cell_size) {
                let bytes: u64 = group.iter().map(Cell::bytes).sum();
                let oldest = group.iter().map(Cell::modified_ms).min().unwrap_or(0);
                let full = bytes >= options.target_cell_size / 2;
                let stale = options.now_ms - oldest >= max_age_ms;
                if !(full || stale) {
                    continue;
                }
                match self
                    .cap_group(
                        table,
                        &store,
                        &schema,
                        &partition_by,
                        &recipe,
                        options,
                        group,
                    )
                    .await?
                {
                    Some(done) => report.add(&done),
                    None => report.aborted += 1,
                }
            }
        }
        if report.nectar_cells > 0 {
            info!(
                nectar = report.nectar_cells,
                capped = report.capped_cells,
                rows = report.rows,
                "Capped nectar"
            );
        }
        Ok(report)
    }

    /// Cap one group. `None` if a user write got in the way.
    #[allow(clippy::too_many_arguments)]
    async fn cap_group(
        &self,
        table: &DeltaTable,
        store: &std::sync::Arc<dyn ObjectStore>,
        schema: &arrow::datatypes::SchemaRef,
        partition_by: &[String],
        recipe: &Recipe,
        options: &CapOptions,
        group: Vec<Cell<Nectar>>,
    ) -> Result<Option<CapReport>> {
        let mut batches = Vec::new();
        for cell in &group {
            batches.push(read_cell(store, cell, schema, partition_by).await?);
        }
        let merged = concat_batches(schema, &batches).map_err(|e| ApiaryError::Internal {
            message: format!("Failed to merge Cells: {e}"),
        })?;
        let ripened = recipe.apply(&merged)?;

        let adds = self
            .write_cells(table, &ripened, options.target_cell_size, CellState::Nectar)
            .await?;
        let checks = RipenessChecks {
            recipe: recipe.clone(),
        };
        let mut capped: Vec<Cell<Capped>> = Vec::with_capacity(adds.len());
        for add in adds {
            let ripe: Cell<Ripe> = Cell::ripened(add, recipe.sort_by.clone());
            capped.push(ripe.cap(&checks).map_err(|cell| ApiaryError::Internal {
                message: format!("Cell {} failed the ripeness checks", cell.path()),
            })?);
        }

        let report = CapReport {
            nectar_cells: group.len(),
            capped_cells: capped.len(),
            rows: ripened.num_rows() as u64,
            aborted: 0,
        };
        let new_paths: Vec<String> = capped.iter().map(|c| c.path().to_string()).collect();
        match self.commit_capping(table, group, capped).await {
            Ok(_) => Ok(Some(report)),
            Err(CapError::Conflict(e)) => {
                warn!(error = %e, "Capping yielded to a concurrent write");
                for path in new_paths {
                    let _ = store.delete(&StorePath::from(path)).await;
                }
                Ok(None)
            }
            Err(CapError::Other(e)) => Err(e),
        }
    }

    /// One Delta commit: remove the nectar, add the capped Cells, change no data.
    async fn commit_capping(
        &self,
        table: &DeltaTable,
        out: Vec<Cell<Nectar>>,
        capped: Vec<Cell<Capped>>,
    ) -> std::result::Result<u64, CapError> {
        let snapshot = table
            .snapshot()
            .map_err(|e| CapError::Other(delta_err("Failed to read table snapshot", e)))?;
        let removed: HashSet<&str> = out.iter().map(Cell::path).collect();
        #[allow(deprecated)]
        let mut actions: Vec<Action> = snapshot
            .log_data()
            .into_iter()
            .filter(|f| removed.contains(f.path().as_ref()))
            .map(|f| Action::Remove(f.remove_action(false)))
            .collect();
        actions.extend(capped.into_iter().map(|c| Action::Add(c.into_add())));

        let finalized = CommitBuilder::from(CommitProperties::default())
            .with_actions(actions)
            .build(
                Some(snapshot),
                table.log_store(),
                DeltaOperation::Optimize {
                    predicate: None,
                    target_size: 0,
                },
            )
            .await
            .map_err(|e| {
                if is_conflict(&e) {
                    CapError::Conflict(e)
                } else {
                    CapError::Other(delta_err("Failed to commit capping", e))
                }
            })?;
        Ok(finalized.version() as u64)
    }

    /// Check that a comb's store can create a file only if it does not exist,
    /// which Delta commits rest on. Harvest refuses a bucket that cannot.
    pub async fn require_conditional_put(&self, table: &DeltaTable) -> Result<()> {
        let store = table.log_store().object_store(None);
        let probe = StorePath::from(format!("_apiary_probe/{}", uuid::Uuid::new_v4()));
        let create = || PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let first = store
            .put_opts(&probe, PutPayload::from_static(b"1"), create())
            .await;
        let second = store
            .put_opts(&probe, PutPayload::from_static(b"2"), create())
            .await;
        let _ = store.delete(&probe).await;
        match (first, second) {
            (Ok(_), Err(object_store::Error::AlreadyExists { .. })) => Ok(()),
            _ => Err(ApiaryError::Config {
                message: format!(
                    "The harvest store at {} does not support conditional writes \
                     (create only if absent), which Delta commits need. Use AWS S3, \
                     Cloudflare R2 or MinIO",
                    self.root()
                ),
            }),
        }
    }

    /// Copy a Frame's capped Cells, oldest first, into its harvest table.
    ///
    /// `harvest` is the comb the harvest tables live on. Only capped Cells go,
    /// at most `max_bytes` per pass (always at least one Cell), and a Cell
    /// already in the harvest table is skipped, so a pass that died halfway is
    /// simply run again. The harvest table is created from `schema` on first use.
    pub async fn harvest(
        &self,
        site: &DeltaTable,
        harvest: &Comb,
        names: (&str, &str, &str),
        schema: &FrameSchema,
        partition_by: &[String],
        max_bytes: u64,
    ) -> Result<HarvestReport> {
        let (hive, box_name, frame) = names;
        let target = harvest
            .open_or_create_frame_table(hive, box_name, frame, schema, partition_by)
            .await?;

        let pending = self.unharvested(site, &target)?;
        let mut report = HarvestReport::default();
        if pending.is_empty() {
            return Ok(report);
        }
        harvest.require_conditional_put(&target).await?;

        let from = site.log_store().object_store(None);
        let to = target.log_store().object_store(None);
        let mut adds = Vec::new();
        for (index, cell) in pending.iter().enumerate() {
            if !adds.is_empty() && report.bytes + cell.bytes() > max_bytes {
                report.remaining = pending.len() - index;
                break;
            }
            let path = StorePath::from(cell.path());
            let data = from
                .get(&path)
                .await
                .map_err(|e| ApiaryError::storage(format!("Failed to read Cell {path}"), e))?
                .bytes()
                .await
                .map_err(|e| ApiaryError::storage(format!("Failed to read Cell {path}"), e))?;
            to.put(&path, PutPayload::from_bytes(data))
                .await
                .map_err(|e| {
                    ApiaryError::storage(format!("Failed to copy Cell {path} to the harvest"), e)
                })?;
            let mut add = cell.add().clone();
            add.data_change = true;
            report.bytes += cell.bytes();
            report.cells += 1;
            adds.push(Action::Add(add));
        }

        let snapshot = target
            .snapshot()
            .map_err(|e| delta_err("Failed to read harvest snapshot", e))?;
        CommitBuilder::from(CommitProperties::default())
            .with_actions(adds)
            .build(
                Some(snapshot),
                target.log_store(),
                DeltaOperation::Write {
                    mode: deltalake::protocol::SaveMode::Append,
                    partition_by: (!partition_by.is_empty()).then(|| partition_by.to_vec()),
                    predicate: None,
                },
            )
            .await
            .map_err(|e| delta_err("Failed to commit to the harvest table", e))?;
        debug!(
            cells = report.cells,
            bytes = report.bytes,
            "Harvested capped Cells"
        );
        Ok(report)
    }

    /// Capped Cells in the site table that the harvest table does not hold yet,
    /// oldest first.
    fn unharvested(&self, site: &DeltaTable, target: &DeltaTable) -> Result<Vec<Cell<Capped>>> {
        let held = harvested_paths(target)?;
        let snapshot = site
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?;
        #[allow(deprecated)]
        let mut capped: Vec<Cell<Capped>> = snapshot
            .log_data()
            .into_iter()
            .map(|f| f.add_action())
            .filter(|add| state_of(add) == Some(CellState::Capped) && !held.contains(&add.path))
            .map(Cell::capped_from_log)
            .collect();
        capped.sort_by(|a, b| {
            a.modified_ms()
                .cmp(&b.modified_ms())
                .then(a.path().cmp(b.path()))
        });
        Ok(capped)
    }

    /// Remove from the site table the capped Cells that are in the harvest and
    /// were capped more than `retention` ago. Returns how many.
    ///
    /// A Cell that is not in the harvest table is never removed, however old.
    /// The harvest table keeps every Cell it holds.
    pub async fn retire_harvested(
        &self,
        site: &DeltaTable,
        harvest: &DeltaTable,
        retention: Duration,
        now_ms: i64,
    ) -> Result<usize> {
        let held = harvested_paths(harvest)?;
        let cutoff = now_ms - retention.as_millis() as i64;
        let snapshot = site
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?;
        #[allow(deprecated)]
        let removes: Vec<Action> = snapshot
            .log_data()
            .into_iter()
            .filter(|f| {
                let add = f.add_action();
                state_of(&add) == Some(CellState::Capped)
                    && held.contains(&add.path)
                    && add.modification_time < cutoff
            })
            .map(|f| Action::Remove(f.remove_action(false)))
            .collect();
        if removes.is_empty() {
            return Ok(0);
        }
        let count = removes.len();
        CommitBuilder::from(CommitProperties::default())
            .with_actions(removes)
            .build(
                Some(snapshot),
                site.log_store(),
                DeltaOperation::Delete { predicate: None },
            )
            .await
            .map_err(|e| delta_err("Failed to retire harvested Cells", e))?;
        Ok(count)
    }

    /// Delete the files no table version needs any more: those removed by
    /// capping or retirement, and any left unreferenced by an abandoned write,
    /// once they are older than `grace`. The grace period keeps a query that
    /// opened an earlier version, and a write still in flight, safe.
    pub async fn clear(&self, table: &DeltaTable, grace: Duration) -> Result<usize> {
        let retention = chrono::Duration::from_std(grace).map_err(|e| ApiaryError::Config {
            message: format!("Invalid clearing grace period: {e}"),
        })?;
        let (_, metrics) = table
            .clone()
            .vacuum()
            .with_mode(deltalake::operations::vacuum::VacuumMode::Full)
            .with_retention_period(retention)
            .with_enforce_retention_duration(false)
            .await
            .map_err(|e| delta_err("Failed to clear the comb", e))?;
        Ok(metrics.files_deleted.len())
    }
}

/// The Arrow schema of a table's data files.
fn table_arrow_schema(table: &DeltaTable) -> Result<arrow::datatypes::SchemaRef> {
    Ok(RecordBatchWriter::for_table(table)
        .map_err(|e| delta_err("Failed to read the table schema", e))?
        .arrow_schema())
}

enum CapError {
    /// A user write touched the same Cells.
    Conflict(DeltaTableError),
    Other(ApiaryError),
}

fn is_conflict(e: &DeltaTableError) -> bool {
    matches!(e, DeltaTableError::Transaction { .. }) || format!("{e:?}").contains("Conflict")
}

/// Paths of the Cells a harvest table holds.
fn harvested_paths(target: &DeltaTable) -> Result<HashSet<String>> {
    Ok(target
        .snapshot()
        .map_err(|e| delta_err("Failed to read harvest snapshot", e))?
        .log_data()
        .into_iter()
        .map(|f| f.path().to_string())
        .collect())
}

/// Split cells (already ordered) into runs of up to `target` bytes, at least
/// one Cell each.
fn chunk(cells: Vec<Cell<Nectar>>, target: u64) -> Vec<Vec<Cell<Nectar>>> {
    let mut groups: Vec<Vec<Cell<Nectar>>> = Vec::new();
    let mut bytes = 0;
    for cell in cells {
        let fits = groups.last().is_some() && bytes + cell.bytes() <= target;
        if fits {
            bytes += cell.bytes();
        } else {
            bytes = cell.bytes();
            groups.push(Vec::new());
        }
        groups
            .last_mut()
            .expect("a group was just pushed")
            .push(cell);
    }
    groups
}

/// Read one Cell as a batch of the table's schema, with its partition columns
/// (which are in the path, not the file) filled back in.
async fn read_cell(
    store: &std::sync::Arc<dyn ObjectStore>,
    cell: &Cell<Nectar>,
    schema: &arrow::datatypes::SchemaRef,
    partition_by: &[String],
) -> Result<RecordBatch> {
    let path = StorePath::from(cell.path());
    let data: Bytes = store
        .get(&path)
        .await
        .map_err(|e| ApiaryError::storage(format!("Failed to read Cell {path}"), e))?
        .bytes()
        .await
        .map_err(|e| ApiaryError::storage(format!("Failed to read Cell {path}"), e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .and_then(|b| b.build())
        .map_err(|e| ApiaryError::Internal {
            message: format!("Failed to open Cell {path}: {e}"),
        })?;
    let batches: Vec<RecordBatch> =
        reader
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| ApiaryError::Internal {
                message: format!("Failed to read Cell {path}: {e}"),
            })?;
    let file_schema = match batches.first() {
        Some(b) => b.schema(),
        None => return Ok(RecordBatch::new_empty(schema.clone())),
    };
    let rows = concat_batches(&file_schema, &batches).map_err(|e| ApiaryError::Internal {
        message: format!("Failed to read Cell {path}: {e}"),
    })?;

    let mut fields: Vec<_> = rows.schema().fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = rows.columns().to_vec();
    for column in partition_by {
        let Some(field) = schema.field_with_name(column).ok() else {
            continue;
        };
        let value = cell.partition_values().get(column).cloned().flatten();
        let text: ArrayRef = std::sync::Arc::new(StringArray::from(vec![value; rows.num_rows()]));
        let typed = cast(&text, field.data_type()).map_err(|e| ApiaryError::Internal {
            message: format!("Failed to restore partition column '{column}': {e}"),
        })?;
        fields.push(std::sync::Arc::new(field.clone()));
        columns.push(typed);
    }
    let with_partitions = RecordBatch::try_new(
        std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
        columns,
    )
    .map_err(|e| ApiaryError::Internal {
        message: format!("Failed to read Cell {path}: {e}"),
    })?;
    conform_batch(&with_partitions, schema, partition_by)
}
