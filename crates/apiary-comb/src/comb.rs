//! The comb: every Frame is a Delta Lake table.
//!
//! A [`Comb`] is the root of a site's tables (a directory on the comb store, or
//! an S3 prefix) and the Delta storage options that go with it. Frames live at
//! `<root>/<hive>/<box>/<frame>/`, each with its own `_delta_log`, written
//! through `delta-rs`. Commits need no lock service and no leader: the next log
//! entry is created with a conditional put, so whichever writer creates it first
//! wins, and a loser whose changes do not conflict retries.
//!
//! Spark and Databricks can read what is stored here. Apiary owns the tables it
//! writes (see the design document on conditional-put interoperability).
//!
//! Each data file (a Cell) carries the tag [`STATE_TAG`] in its Delta `add`
//! action, recording whether it is nectar or capped. Other engines ignore tags.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Once};

use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionState;
use datafusion::logical_expr::cast;
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use deltalake::DeltaTable;
use deltalake::DeltaTableError;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, StructType, Transaction};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use tracing::{debug, info};
use url::Url;

use apiary_core::{ApiaryError, FrameSchema, Result};

use crate::local::expand_local_path;
use crate::s3::{extract_query_param, parse_s3_uri};
use crate::schema::{conform_batch, delta_schema};

/// The virtual column every Frame has, naming where a row is: `crop`, `comb`
/// or `harvested`. A Frame cannot declare a column of this name.
pub const STAGE_COLUMN: &str = "_stage";

/// The Delta `add` tag that records a Cell's ripeness.
pub const STATE_TAG: &str = "apiary.state";

/// How far a Cell has ripened. Other engines see only an ordinary Delta file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellState {
    /// Freshly committed data, not yet ripened into its final layout.
    Nectar,
    /// Ripened, sealed and immutable.
    Capped,
}

impl CellState {
    /// The tag value stored in the Delta log.
    pub fn as_str(self) -> &'static str {
        match self {
            CellState::Nectar => "nectar",
            CellState::Capped => "capped",
        }
    }

    /// Parse a tag value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "nectar" => Some(CellState::Nectar),
            "capped" => Some(CellState::Capped),
            _ => None,
        }
    }
}

/// A query session configured the way Apiary results are expected to look.
///
/// DataFusion reads Parquet strings and binary as `Utf8View` and `BinaryView`
/// by default. Results go to Python and to anything that reads Arrow IPC, where
/// view types are not universally supported, so this session returns plain
/// `Utf8` and `Binary`, as V1 did. Use it for every query over Frame tables.
pub fn query_session() -> SessionContext {
    let config = SessionConfig::new().set_bool(
        "datafusion.execution.parquet.schema_force_view_types",
        false,
    );
    SessionContext::new_with_config(config)
}

/// How many times a commit may lose the race for the next log entry and retry.
///
/// `delta-rs` retries immediately, without backoff, and its default of 15 is
/// not enough when several Nodes append to one Frame at once: a writer can be
/// starved while others keep arriving. Retrying is cheap, because the data
/// files are already written and only the log entry is attempted again, and a
/// blind append never conflicts with another append.
const COMMIT_RETRIES: usize = 100;

/// What one commit wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    /// The Delta table version the commit created.
    pub version: u64,
    /// Data files (Cells) added.
    pub cells: usize,
    /// Rows written.
    pub rows: u64,
    /// Bytes of data files added.
    pub bytes: u64,
}

/// The size of a Frame's current data.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FrameStats {
    /// Data files (Cells) in the current table version.
    pub cells: u64,
    /// Rows across those Cells.
    pub rows: u64,
    /// Bytes across those Cells.
    pub bytes: u64,
}

/// The root of a site's Frame tables.
#[derive(Clone, Debug)]
pub struct Comb {
    root: Url,
    storage_options: HashMap<String, String>,
}

impl Comb {
    /// Create a comb from a storage URI: `local://<path>`, a bare path (with
    /// `~` expanded), or `s3://bucket/prefix?region=..&endpoint=..`.
    ///
    /// S3 credentials come from the usual `AWS_*` environment variables. The
    /// store must support conditional writes (AWS S3, R2 and MinIO do).
    pub fn from_storage_uri(uri: &str) -> Result<Self> {
        if uri.starts_with("s3://") {
            return Self::from_s3_uri(uri);
        }

        let path = uri.strip_prefix("local://").unwrap_or(uri);
        let expanded = expand_local_path(path)?;
        Self::from_local_path(&expanded)
    }

    /// Create a comb rooted at a local directory (created if missing).
    pub fn from_local_path(path: &std::path::Path) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(|e| {
            ApiaryError::storage(format!("Failed to create comb directory {path:?}"), e)
        })?;
        let absolute = std::path::absolute(path).map_err(|e| {
            ApiaryError::storage(format!("Failed to resolve comb directory {path:?}"), e)
        })?;
        let root = Url::from_directory_path(&absolute).map_err(|()| ApiaryError::Config {
            message: format!("Comb directory is not a valid path: {absolute:?}"),
        })?;
        Ok(Self {
            root,
            storage_options: HashMap::new(),
        })
    }

    fn from_s3_uri(uri: &str) -> Result<Self> {
        register_s3_handlers();

        let (bucket, prefix) = parse_s3_uri(uri)?;
        let mut location = format!("s3://{bucket}/");
        if !prefix.is_empty() {
            location.push_str(prefix.trim_matches('/'));
            location.push('/');
        }
        let root = Url::parse(&location).map_err(|e| ApiaryError::Config {
            message: format!("Invalid S3 URI {uri}: {e}"),
        })?;

        // Delta commits rest on put-if-absent; make sure it is on.
        let mut storage_options = HashMap::new();
        storage_options.insert("aws_conditional_put".to_string(), "etag".to_string());

        if let Some(region) = extract_query_param(uri, "region") {
            storage_options.insert("AWS_REGION".to_string(), region);
        }
        if let Some(endpoint) = extract_query_param(uri, "endpoint") {
            storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint);
            storage_options.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
        }
        // Plain-HTTP endpoints (MinIO and other local replacements) also need
        // allow_http when the endpoint comes from the environment.
        if let Ok(env_endpoint) = std::env::var("AWS_ENDPOINT_URL")
            && env_endpoint.starts_with("http://")
        {
            storage_options.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
        }

        Ok(Self {
            root,
            storage_options,
        })
    }

    /// The root URL all Frame tables live under.
    pub fn root(&self) -> &Url {
        &self.root
    }

    /// The Delta storage options for this comb.
    pub fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    /// The URL of one Frame's table.
    pub fn frame_url(&self, hive: &str, box_name: &str, frame: &str) -> Result<Url> {
        let mut url = self.root.clone();
        url.path_segments_mut()
            .map_err(|()| ApiaryError::Config {
                message: format!("Comb root cannot hold tables: {}", self.root),
            })?
            .pop_if_empty()
            .extend([hive, box_name, frame])
            .push("");
        Ok(url)
    }

    /// Create a Frame's Delta table, or return the existing one.
    ///
    /// Idempotent and safe to race: two Nodes creating the same Frame both get
    /// a table. The stored schema is [`delta_schema`] of `schema`.
    pub async fn create_frame_table(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
        schema: &FrameSchema,
        partition_by: &[String],
    ) -> Result<DeltaTable> {
        if schema.fields.is_empty() {
            return Err(ApiaryError::Schema {
                message: format!(
                    "Frame {hive}.{box_name}.{frame} needs at least one column;                      declare its schema as a dict of column name to type"
                ),
            });
        }
        if let Some(reserved) = schema
            .fields
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(STAGE_COLUMN))
        {
            return Err(ApiaryError::Schema {
                message: format!(
                    "Column '{}' is reserved: every Frame has a virtual `{STAGE_COLUMN}`                      column that says where each row is (crop, comb or harvested)",
                    reserved.name
                ),
            });
        }
        let url = self.frame_url(hive, box_name, frame)?;
        let arrow_schema = delta_schema(schema);
        let kernel_schema: StructType =
            arrow_schema
                .as_ref()
                .try_into_kernel()
                .map_err(|e| ApiaryError::Schema {
                    message: format!("Frame schema cannot be stored in Delta Lake: {e}"),
                })?;

        for column in partition_by {
            if arrow_schema.index_of(column).is_err() {
                return Err(ApiaryError::Schema {
                    message: format!("Partition column '{column}' is not in the frame schema"),
                });
            }
        }

        let table = CreateBuilder::new()
            .with_location(url.as_str())
            .with_storage_options(self.storage_options.clone())
            .with_table_name(frame)
            .with_columns(kernel_schema.fields().cloned())
            .with_partition_columns(partition_by.iter().cloned())
            .with_save_mode(SaveMode::Ignore)
            .await
            .map_err(|e| {
                delta_err(
                    format!("Failed to create table {hive}.{box_name}.{frame}"),
                    e,
                )
            })?;

        info!(%url, version = ?table.version(), "Frame table ready");
        Ok(table)
    }

    /// Open a Frame's table, or `None` if it has not been created.
    pub async fn open_frame_table(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
    ) -> Result<Option<DeltaTable>> {
        let url = self.frame_url(hive, box_name, frame)?;
        match deltalake::open_table_with_storage_options(url, self.storage_options.clone()).await {
            Ok(table) => Ok(Some(table)),
            Err(DeltaTableError::NotATable(_) | DeltaTableError::InvalidTableLocation(_)) => {
                Ok(None)
            }
            Err(e) => Err(delta_err(
                format!("Failed to open table {hive}.{box_name}.{frame}"),
                e,
            )),
        }
    }

    /// Open a Frame's table, creating it first if it does not exist.
    pub async fn open_or_create_frame_table(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
        schema: &FrameSchema,
        partition_by: &[String],
    ) -> Result<DeltaTable> {
        match self.open_frame_table(hive, box_name, frame).await? {
            Some(table) => Ok(table),
            None => {
                self.create_frame_table(hive, box_name, frame, schema, partition_by)
                    .await
            }
        }
    }

    /// Append a batch to a Frame as new Cells in `state`.
    ///
    /// The batch is conformed to the table schema (see [`conform_batch`]).
    /// Appends from different Nodes do not conflict, so a lost commit race is
    /// retried by `delta-rs`. An empty batch commits nothing and returns the
    /// current version.
    pub async fn append(
        &self,
        table: &DeltaTable,
        batch: &RecordBatch,
        target_cell_size: u64,
        state: CellState,
    ) -> Result<Committed> {
        let adds = self
            .write_cells(table, batch, target_cell_size, state)
            .await?;
        self.commit(
            table,
            adds,
            Vec::new(),
            SaveMode::Append,
            batch.num_rows(),
            None,
        )
        .await
    }

    /// Deposit rows from a crop: an append that also records, in the same
    /// Delta commit, that the crop's segments up to `version` are now in the
    /// table.
    ///
    /// The record is a Delta application transaction `(app_id, version)`. If the
    /// Node dies after the commit but before it cleans up its crop, the next run
    /// reads the version back with [`deposited_version`](Self::deposited_version)
    /// and releases those segments instead of depositing them twice.
    pub async fn deposit(
        &self,
        table: &DeltaTable,
        batch: &RecordBatch,
        target_cell_size: u64,
        app_id: &str,
        version: u64,
    ) -> Result<Committed> {
        let adds = self
            .write_cells(table, batch, target_cell_size, CellState::Nectar)
            .await?;
        let transaction = Transaction::new(app_id, version as i64);
        self.commit(
            table,
            adds,
            Vec::new(),
            SaveMode::Append,
            batch.num_rows(),
            Some(transaction),
        )
        .await
    }

    /// The highest crop segment recorded as deposited under `app_id`, or 0.
    pub async fn deposited_version(&self, table: &DeltaTable, app_id: &str) -> Result<u64> {
        let version = table
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?
            .transaction_version(table.log_store().as_ref(), app_id)
            .await
            .map_err(|e| delta_err("Failed to read deposit records", e))?;
        Ok(version.unwrap_or(0).max(0) as u64)
    }

    /// Replace all of a Frame's data with a batch, in one commit that removes
    /// every current Cell and adds the new ones.
    ///
    /// Unlike a V1 overwrite, this conflicts with a concurrent writer instead of
    /// silently dropping its data.
    pub async fn overwrite(
        &self,
        table: &DeltaTable,
        batch: &RecordBatch,
        target_cell_size: u64,
        state: CellState,
    ) -> Result<Committed> {
        self.overwrite_superseding(table, batch, target_cell_size, state, None)
            .await
    }

    /// An [`overwrite`](Self::overwrite) that also records, in the same commit,
    /// that a crop's segments up to `version` are superseded: they will never be
    /// deposited, so rows still in the crop do not reappear after the overwrite.
    pub async fn overwrite_superseding(
        &self,
        table: &DeltaTable,
        batch: &RecordBatch,
        target_cell_size: u64,
        state: CellState,
        supersedes: Option<(&str, u64)>,
    ) -> Result<Committed> {
        let adds = self
            .write_cells(table, batch, target_cell_size, state)
            .await?;
        let removes: Vec<Action> = table
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?
            .log_data()
            .into_iter()
            .map(|file| Action::Remove(file.remove_action(true)))
            .collect();
        self.commit(
            table,
            adds,
            removes,
            SaveMode::Overwrite,
            batch.num_rows(),
            supersedes.map(|(app_id, version)| Transaction::new(app_id, version as i64)),
        )
        .await
    }

    /// Write the batch to data files and tag them. Nothing is committed yet.
    async fn write_cells(
        &self,
        table: &DeltaTable,
        batch: &RecordBatch,
        target_cell_size: u64,
        state: CellState,
    ) -> Result<Vec<deltalake::kernel::Add>> {
        let mut writer = RecordBatchWriter::for_table(table)
            .map_err(|e| delta_err("Failed to prepare table writer", e))?;
        if target_cell_size > 0 {
            writer = writer.with_target_file_size(target_cell_size);
        }

        let partition_by: Vec<String> = table
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?
            .metadata()
            .partition_columns()
            .to_vec();
        let conformed = conform_batch(batch, &writer.arrow_schema(), &partition_by)?;

        if conformed.num_rows() == 0 {
            return Ok(Vec::new());
        }
        writer
            .write(conformed)
            .await
            .map_err(|e| delta_err("Failed to write cells", e))?;
        let mut adds = writer
            .flush()
            .await
            .map_err(|e| delta_err("Failed to flush cells", e))?;

        for add in &mut adds {
            let tags = add.tags.get_or_insert_with(HashMap::new);
            tags.insert(STATE_TAG.to_string(), Some(state.as_str().to_string()));
        }
        Ok(adds)
    }

    async fn commit(
        &self,
        table: &DeltaTable,
        adds: Vec<deltalake::kernel::Add>,
        removes: Vec<Action>,
        mode: SaveMode,
        rows: usize,
        transaction: Option<Transaction>,
    ) -> Result<Committed> {
        let snapshot = table
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?;

        // An empty write with a transaction still commits: the transaction is
        // the point (a deposit record, or a crop being superseded).
        if adds.is_empty() && removes.is_empty() && transaction.is_none() {
            debug!("Empty write, nothing to commit");
            return Ok(Committed {
                version: snapshot.version(),
                cells: 0,
                rows: 0,
                bytes: 0,
            });
        }

        let cells = adds.len();
        let bytes: u64 = adds.iter().map(|a| a.size.max(0) as u64).sum();
        let partition_cols = snapshot.metadata().partition_columns().to_vec();
        let partition_by = (!partition_cols.is_empty()).then_some(partition_cols);

        let actions: Vec<Action> = adds.into_iter().map(Action::Add).chain(removes).collect();
        let properties = match transaction {
            Some(transaction) => {
                CommitProperties::default().with_application_transaction(transaction)
            }
            None => CommitProperties::default(),
        };
        let finalized = CommitBuilder::from(properties)
            .with_max_retries(COMMIT_RETRIES)
            .with_actions(actions)
            .build(
                Some(snapshot),
                table.log_store(),
                DeltaOperation::Write {
                    mode,
                    partition_by,
                    predicate: None,
                },
            )
            .await
            .map_err(|e| delta_err("Failed to commit to the delta log", e))?;

        Ok(Committed {
            version: finalized.version(),
            cells,
            rows: rows as u64,
            bytes,
        })
    }

    /// The size of a Frame's current data, from the Delta log alone.
    pub fn frame_stats(&self, table: &DeltaTable) -> Result<FrameStats> {
        let snapshot = table
            .snapshot()
            .map_err(|e| delta_err("Failed to read table snapshot", e))?;
        let mut stats = FrameStats::default();
        for file in snapshot.log_data() {
            stats.cells += 1;
            stats.bytes += file.size().max(0) as u64;
            stats.rows += file.num_records().unwrap_or(0) as u64;
        }
        Ok(stats)
    }

    /// A lazy scan of a Frame's table, for sessions configured like `state`.
    ///
    /// Used by the query catalogue, which resolves Frames on demand. The
    /// table's object store is registered with `state`'s runtime.
    pub async fn table_provider(
        &self,
        state: &SessionState,
        table: &DeltaTable,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        table_provider(state, table).await
    }

    /// Register a Frame's table with a query session under `name`.
    ///
    /// The table is scanned lazily, with partition pruning and file skipping
    /// from Delta statistics, rather than being loaded into memory first.
    pub async fn register_table(
        &self,
        ctx: &SessionContext,
        name: &str,
        table: &DeltaTable,
    ) -> Result<()> {
        let provider = table_provider(&ctx.state(), table).await?;
        ctx.register_table(name, provider).map_err(df_err)?;
        Ok(())
    }

    /// Read a Frame into one batch, optionally keeping only rows whose
    /// partition columns equal the given values. `None` if no rows match.
    pub async fn read(
        &self,
        table: &DeltaTable,
        partition_filter: Option<&HashMap<String, String>>,
    ) -> Result<Option<RecordBatch>> {
        let ctx = query_session();
        let provider = table_provider(&ctx.state(), table).await?;
        let mut frame = ctx.read_table(provider).map_err(df_err)?;

        if let Some(filter) = partition_filter {
            for (column, value) in filter {
                // Filter values arrive as strings, whatever the column type.
                frame = frame
                    .filter(cast(col(column.as_str()), DataType::Utf8).eq(lit(value.as_str())))
                    .map_err(df_err)?;
            }
        }

        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches = frame.collect().await.map_err(df_err)?;
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        if total == 0 {
            return Ok(None);
        }
        concat_batches(&schema, &batches)
            .map(Some)
            .map_err(|e| ApiaryError::Internal {
                message: format!("Failed to merge result batches: {e}"),
            })
    }
}

/// A scan of a Frame table for sessions configured like `state`.
///
/// The provider inherits the session's settings (in particular, no view
/// types, see [`query_session`]); without them it would use delta-rs defaults
/// and return `Utf8View`. The table's object store is registered with the
/// session's runtime, so scans can reach it.
async fn table_provider(
    state: &SessionState,
    table: &DeltaTable,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    table
        .update_datafusion_session(state)
        .map_err(|e| delta_err("Failed to prepare the query session", e))?;
    table
        .table_provider()
        .with_session(Arc::new(state.clone()))
        .await
        .map_err(df_err)
}

/// Register the S3 handlers with `delta-rs` (once per process).
fn register_s3_handlers() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| deltalake::aws::register_handlers(None));
}

fn delta_err(context: impl Into<String>, e: DeltaTableError) -> ApiaryError {
    ApiaryError::storage(context, e)
}

fn df_err(e: datafusion::error::DataFusionError) -> ApiaryError {
    ApiaryError::Internal {
        message: format!("Query error: {e}"),
    }
}

/// Resolve a `local://` path for callers that need the directory itself.
pub fn local_comb_path(uri: &str) -> Option<Result<PathBuf>> {
    if uri.starts_with("s3://") {
        return None;
    }
    let path = uri.strip_prefix("local://").unwrap_or(uri);
    Some(expand_local_path(path))
}
