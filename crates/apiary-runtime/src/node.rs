//! The Apiary node — a stateless compute instance in the swarm.
//!
//! [`ApiaryNode`] is the main runtime entry point. It initialises the
//! storage backend, detects system capacity, creates the bee pool,
//! starts the heartbeat writer and world view builder, and manages
//! the node lifecycle.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use tokio::sync::RwLock;
use tracing::info;

use apiary_comb::custom_store::{self, ObjectStoreBackend};
use apiary_comb::local::{LocalBackend, expand_local_path};
use apiary_comb::s3::S3Backend;
use apiary_comb::schema::{conform_batch, delta_schema};
use apiary_comb::{CapReport, CellState, Comb, Crop, HarvestReport, Recipe};
use apiary_core::config::NodeConfig;
use apiary_core::error::ApiaryError;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::storage::StorageBackend;
use apiary_core::{CommitGate, Env, FrameSchema, Result, WriteResult, check_clock};
use apiary_plan::{ApiaryQueryContext, QueryOptions};

use crate::behavioral::AbandonmentTracker;
use crate::budget::CommitBudget;
use crate::cache::CellCache;
use crate::deposit::{DepositReport, Depositor, open_or_create_table};
use crate::duties::{DutiesSettings, NodeDuties};
use crate::heartbeat::{
    HeartbeatLoad, HeartbeatWriter, LoadSource, NodeState, WorldView, WorldViewBuilder,
};
use crate::upkeep::{ClearReport, Upkeep, UpkeepSettings};
use apiary_colony::{
    BeeParams, Colony, ColonyConfig, NoThermal, SysfsThermal, TemperatureRegulation, ThermalSensor,
};

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
struct FrameSpec {
    /// The schema the Frame's table stores.
    schema: SchemaRef,
    partition_by: Vec<String>,
}

/// An Apiary compute node — the runtime for one machine in the swarm.
///
/// The node holds a reference to the storage backend and its configuration.
/// In solo mode it uses [`LocalBackend`]; in multi-node mode it uses
/// [`S3Backend`]. The node is otherwise stateless — all committed state
/// lives in object storage.
pub struct ApiaryNode {
    /// Node configuration including auto-detected capacity.
    pub config: NodeConfig,

    /// The shared storage backend (object storage or local filesystem).
    pub storage: Arc<dyn StorageBackend>,

    /// The comb: every Frame is a Delta table under this root.
    pub comb: Arc<Comb>,

    /// This Node's crop: rows ingested here and not yet deposited into the comb.
    pub crop: Arc<Crop>,

    /// Registry manager for DDL operations.
    pub registry: Arc<RegistryManager>,

    /// DataFusion-based SQL query context.
    pub query_ctx: Arc<ApiaryQueryContext>,

    /// This Node's Bees: one per core, each holding a role it chose.
    pub colony: Arc<Colony>,

    /// What the Bees do: queries, ripening, clearing and surveying.
    duties: Arc<NodeDuties>,

    /// Local cell cache with LRU eviction.
    pub cell_cache: Arc<CellCache>,

    /// Abandonment tracker for task failure handling.
    pub abandonment_tracker: Arc<AbandonmentTracker>,

    /// Heartbeat writer for this node.
    heartbeat_writer: Arc<HeartbeatWriter>,

    /// Shared world view (updated by the background builder).
    world_view: Arc<RwLock<WorldView>>,

    /// World view builder (kept alive for on-demand cleanup).
    #[allow(dead_code)]
    world_view_builder: Arc<WorldViewBuilder>,

    /// Moves the crop into the comb, on a cadence and at shutdown.
    depositor: Arc<Depositor>,

    /// Caps, harvests and clears the comb.
    upkeep: Arc<Upkeep>,

    /// Asked before every direct commit.
    commit_gate: CommitGate,

    /// Bytes ingested since the crop was last deposited.
    crop_bytes: Arc<AtomicU64>,

    /// What ingest has learned about each Frame, by Hive, Box and Frame name.
    frame_specs: RwLock<HashMap<(String, String, String), Arc<FrameSpec>>>,

    /// Cancellation channel to stop background tasks on shutdown.
    cancel_tx: tokio::sync::watch::Sender<bool>,

    /// Clock and seed this node runs under (the system clock in production,
    /// a virtual one in the observation hive).
    pub env: Env,
}

impl ApiaryNode {
    /// Record something this Node did, as an event on the `apiary::mark` target.
    /// Ordinary logging ignores them; the observation hive collects them into its
    /// trace, with the Node and the virtual time, so a run can be read back.
    fn mark(&self, kind: &str, detail: impl std::fmt::Display) {
        tracing::info!(target: "apiary::mark", node = %self.config.node_id, kind, "{detail}");
    }

    /// Start a new Apiary node with the given configuration.
    ///
    /// Initialises the appropriate storage backend based on `config.storage_uri`
    /// and logs the node's capacity. Runs on the system clock; see
    /// [`start_with_env`](Self::start_with_env) to supply another.
    pub async fn start(config: NodeConfig) -> Result<Self> {
        Self::start_with_env(config, Env::system()).await
    }

    /// Start a node under the given [`Env`]: every time read, sleep and seeded
    /// random choice goes through it.
    pub async fn start_with_env(config: NodeConfig, env: Env) -> Result<Self> {
        Self::start_with_gate(config, env, None).await
    }

    /// Start a node whose commits are gated by `gate` (see [`CommitGate`]). With
    /// none, a gate that only refuses a clock that cannot be right is used.
    pub async fn start_with_gate(
        config: NodeConfig,
        env: Env,
        gate: Option<CommitGate>,
    ) -> Result<Self> {
        let gate: CommitGate = gate.unwrap_or_else(|| {
            let clock = env.clock();
            Arc::new(move || check_clock(clock.now_utc(), None))
        });
        let storage: Arc<dyn StorageBackend> = if config.storage_uri.starts_with("s3://") {
            Arc::new(S3Backend::new(&config.storage_uri)?)
        } else if let Some(authority) = custom_store::drive_authority(&config.storage_uri) {
            // The site's drive on another Node, reached through the colony. Its
            // store was registered when the network started.
            let store =
                custom_store::lookup_store(&authority).ok_or_else(|| ApiaryError::Config {
                    message: format!(
                        "No drive is registered for {}: start the network first",
                        config.storage_uri
                    ),
                })?;
            Arc::new(ObjectStoreBackend::new(store))
        } else {
            // Parse local URI: "local://<path>" or treat as raw path
            let path = config
                .storage_uri
                .strip_prefix("local://")
                .unwrap_or(&config.storage_uri);

            let expanded = expand_local_path(path)?;

            Arc::new(LocalBackend::new(expanded).await?)
        };

        info!(
            node_id = %config.node_id,
            cores = config.cores,
            memory_mb = config.memory_bytes / (1024 * 1024),
            memory_per_bee_mb = config.memory_per_bee / (1024 * 1024),
            target_cell_size_mb = config.target_cell_size / (1024 * 1024),
            storage_uri = %config.storage_uri,
            "Apiary node started"
        );

        // Initialize registry (retry with backoff for transient S3 errors)
        let registry = Arc::new(RegistryManager::new(Arc::clone(&storage)).with_clock(env.clock()));
        {
            let max_retries: u32 = 10;
            let mut delay = Duration::from_secs(1);
            let mut last_err = None;
            for attempt in 1..=max_retries {
                match registry.load_or_create().await {
                    Ok(_) => {
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            attempt,
                            max_retries,
                            error = %e,
                            "Registry initialization failed, retrying"
                        );
                        last_err = Some(e);
                        if attempt < max_retries {
                            env.clock().sleep(delay).await;
                            delay = (delay * 2).min(Duration::from_secs(10));
                        }
                    }
                }
            }
            if let Some(e) = last_err {
                return Err(e);
            }
        }
        info!("Registry loaded");

        // Initialize query context
        // Every Frame is a Delta table under the comb
        let comb = Arc::new(Comb::from_storage_uri(&config.storage_uri)?);

        // The crop: where ingested rows land first, on this Node's disk. It
        // outlives restarts, so rows ingested before a crash are deposited by
        // the next run.
        let crop = Arc::new(Crop::open(config.crop_dir())?.with_sync(config.crop_sync));

        // One long-lived query session for the Node: a memory pool shared by
        // all queries, a spill directory, joins planned to fit a Bee, and
        // Frames that include the crop.
        let mut query_options = QueryOptions::from_node(&config);
        query_options.crop = Some(Arc::clone(&crop));
        let query_ctx = Arc::new(ApiaryQueryContext::with_options(
            Arc::clone(&comb),
            Arc::clone(&registry),
            config.node_id.clone(),
            query_options,
        )?);

        // Each Frame's commits (deposits and capping alike) are kept within a
        // budget per minute, so a streaming Frame's log does not grow without end.
        let budget = Arc::new(CommitBudget::new(config.commit_budget_per_min, env.clock()));
        let depositor = Arc::new(
            Depositor::new(
                Arc::clone(&comb),
                Arc::clone(&crop),
                Arc::clone(&registry),
                config.target_cell_size,
            )
            .with_budget(Arc::clone(&budget))
            .with_gate(Arc::clone(&gate)),
        );
        let crop_bytes = Arc::new(AtomicU64::new(0));
        // Whatever an earlier run left in the crop is deposited soon.
        if let Ok(left) = crop.pending_bytes() {
            crop_bytes.store(left, Ordering::Relaxed);
        }

        // Upkeep of the comb: capping, harvest (if the site has a harvest
        // store) and clearing. Ripener and Undertaker Bees do it.
        let harvest = match &config.harvest_uri {
            Some(uri) => Some(Arc::new(Comb::from_storage_uri(uri)?)),
            None => None,
        };
        let upkeep = Arc::new(
            Upkeep::new(
                Arc::clone(&comb),
                harvest,
                Arc::clone(&registry),
                UpkeepSettings {
                    target_cell_size: config.target_cell_size,
                    cap_max_age: config.cap_max_age,
                    harvest_batch_bytes: config.harvest_batch_bytes,
                    retention: config.retention,
                    clear_grace: config.clear_grace,
                },
                env.clock(),
            )
            .with_budget(Arc::clone(&budget))
            .with_gate(Arc::clone(&gate)),
        );

        // The Bees. A query is a Forager's Patch, run under that Bee's share of
        // the Node's memory pool. Until a query is split into one Patch per
        // partition, a Patch is a whole query over `cores` partitions, so a Bee's
        // share is that many partitions' worth.
        let duties = Arc::new(NodeDuties::new(
            env.clock(),
            DutiesSettings {
                crop_max_bytes: config.crop_max_bytes,
                deposit_interval: config.deposit_interval,
                survey_interval: config.cap_interval,
                harvest_interval: config.harvest_interval,
                clear_interval: config.clear_interval,
                query_limit: (config.cores * 64).max(64),
            },
            Arc::clone(&storage),
            Arc::clone(&crop),
            Arc::clone(&depositor),
            Arc::clone(&upkeep),
            Arc::clone(&crop_bytes),
        ));
        let thermal: Arc<dyn ThermalSensor> = match SysfsThermal::raspberry_pi() {
            Some(sensor) => Arc::new(sensor),
            None => Arc::new(NoThermal),
        };
        let mut bee_params = BeeParams::default();
        bee_params.thresholds.sigma = config.colony_diversity;
        let mut colony_config = ColonyConfig::new(
            config.node_id.as_str(),
            config.cores,
            (config.memory_per_bee as usize).saturating_mul(config.cores.max(1)),
            query_ctx.memory_pool(),
        );
        colony_config.params = bee_params;
        colony_config.thermal = thermal;
        let colony = Arc::new(Colony::start(
            colony_config,
            &env,
            Arc::clone(&duties) as _,
            None,
        ));
        info!(bees = config.cores, "Bees started");

        // Initialize cell cache
        let cache_dir = config.cache_dir.join("cells");
        let cell_cache = Arc::new(
            CellCache::new(cache_dir, config.max_cache_size, Arc::clone(&storage))
                .await?
                .with_clock(env.clock()),
        );
        info!(
            max_cache_mb = config.max_cache_size / (1024 * 1024),
            "Cell cache initialized"
        );

        // Initialize heartbeat writer
        let heartbeat_writer = Arc::new(
            HeartbeatWriter::new(
                Arc::clone(&storage),
                &config,
                Arc::new(ColonyLoad {
                    colony: Arc::clone(&colony),
                    duties: Arc::clone(&duties),
                }) as Arc<dyn LoadSource>,
                Arc::clone(&cell_cache),
            )
            .with_clock(env.clock()),
        );

        // Initialize world view builder
        let world_view_builder = Arc::new(
            WorldViewBuilder::new(
                Arc::clone(&storage),
                config.heartbeat_interval, // poll at same rate as heartbeat
                config.dead_threshold,
            )
            .with_clock(env.clock()),
        );
        let world_view = world_view_builder.world_view();

        // Write initial heartbeat and build initial world view synchronously
        // so that swarm_status() works immediately after start().
        heartbeat_writer.write_once().await?;
        world_view_builder.poll_once().await?;
        info!("Initial heartbeat written and world view built");

        // Create cancellation channel
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        // Start heartbeat writer background task
        {
            let writer = Arc::clone(&heartbeat_writer);
            let rx = cancel_rx.clone();
            tokio::spawn(async move {
                writer.run(rx).await;
            });
        }

        // Start world view builder background task
        {
            let builder = Arc::clone(&world_view_builder);
            let rx = cancel_rx.clone();
            tokio::spawn(async move {
                builder.run(rx).await;
            });
        }

        info!("Heartbeat and world view background tasks started");

        Ok(Self {
            config,
            storage,
            comb,
            crop,
            registry,
            query_ctx,
            colony,
            duties,
            cell_cache,
            abandonment_tracker: Arc::new(AbandonmentTracker::default()),
            heartbeat_writer,
            world_view,
            world_view_builder,
            depositor,
            upkeep,
            commit_gate: gate,
            crop_bytes,
            frame_specs: RwLock::new(HashMap::new()),
            cancel_tx,
            env,
        })
    }

    /// Gracefully shut down the node.
    ///
    /// Stops background tasks (heartbeat writer, world view builder),
    /// deletes the heartbeat file, and cleans up resources.
    pub async fn shutdown(&self) {
        info!(node_id = %self.config.node_id, "Apiary node shutting down");

        // Signal background tasks to stop
        let _ = self.cancel_tx.send(true);

        // The Bees finish the Patch in hand and stop.
        self.colony.shutdown().await;

        // Allow background tasks a moment to stop
        self.env.clock().sleep(Duration::from_millis(100)).await;

        // Deposit what is still in the crop, so a graceful stop leaves nothing
        // only on this Node's disk.
        match self.depositor.flush().await {
            Ok(report) => Depositor::log(&report),
            Err(e) => tracing::warn!(error = %e, "Failed to deposit the crop during shutdown"),
        }

        // Delete our heartbeat file (graceful departure)
        match self.heartbeat_writer.delete_heartbeat().await {
            Err(e) => {
                tracing::warn!(error = %e, "Failed to delete heartbeat during shutdown");
            }
            _ => {
                info!(node_id = %self.config.node_id, "Heartbeat deleted (graceful departure)");
            }
        }
    }

    /// Write data to a frame. This is the end-to-end write path:
    /// 1. Resolve the frame from the registry
    /// 2. Open its Delta table, creating it on first write
    /// 3. Conform the batch to the frame schema
    /// 4. Write Parquet cells, partitioned and sized to the Bee budget
    /// 5. Commit them to the Delta log as nectar
    pub async fn write_to_frame(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        batch: &RecordBatch,
    ) -> Result<WriteResult> {
        (self.commit_gate)()?;
        let start = self.env.clock().monotonic();

        let table = self.frame_table(hive, box_name, frame_name).await?;
        let committed = self
            .comb
            .append(
                &table,
                batch,
                self.config.target_cell_size,
                CellState::Nectar,
            )
            .await?;

        self.mark(
            "write",
            format_args!("{hive}.{box_name}.{frame_name} {} rows", batch.num_rows()),
        );
        Ok(self.write_result(committed, start).await)
    }

    /// Read data from a frame, optionally filtering by partition values.
    /// Returns all matching data as a merged RecordBatch, including rows still
    /// in the crop (without the `_stage` column).
    pub async fn read_from_frame(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        partition_filter: Option<&HashMap<String, String>>,
    ) -> Result<Option<RecordBatch>> {
        self.query_ctx
            .read_frame(hive, box_name, frame_name, partition_filter)
            .await
    }

    /// Land a batch in this Node's crop: queryable at once (`_stage = 'crop'`),
    /// deposited into the comb on the next deposit interval.
    ///
    /// The batch is checked against the Frame's schema here, so a bad batch is
    /// refused at the entrance and nothing is written. The segment is on disk,
    /// synced, before this returns. Unlike [`write_to_frame`](Self::write_to_frame),
    /// which commits to the comb before returning, the rows exist only on this
    /// Node until they are deposited.
    pub async fn ingest(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        batch: &RecordBatch,
    ) -> Result<IngestResult> {
        let spec = self.frame_spec(hive, box_name, frame_name).await?;
        let conformed = conform_batch(batch, &spec.schema, &spec.partition_by)?;
        let rows = conformed.num_rows() as u64;

        let log = self.crop.frame(hive, box_name, frame_name)?;
        let segment = tokio::task::spawn_blocking(move || log.append(&conformed))
            .await
            .map_err(|e| ApiaryError::Internal {
                message: format!("Ingest task failed: {e}"),
            })??;

        let bytes = segment.as_ref().map_or(0, |s| s.bytes);
        let pending = self.crop_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if pending >= self.config.crop_max_bytes {
            self.colony.wake();
        }
        self.mark(
            "ingest",
            format_args!("{hive}.{box_name}.{frame_name} {rows} rows"),
        );
        Ok(IngestResult {
            rows,
            segment: segment.map(|s| s.seq),
            crop_bytes: pending,
        })
    }

    /// Deposit everything in the crop into the comb now, rather than waiting
    /// for the interval.
    pub async fn flush_crop(&self) -> Result<DepositReport> {
        let result = self.depositor.flush().await;
        match &result {
            Ok(report) => self.mark("deposit", format_args!("{report:?}")),
            Err(e) => self.mark("deposit", format_args!("failed: {e}")),
        }
        result
    }

    /// The schema a Frame's table stores (what an incoming batch is checked
    /// against), from the registry the first time and from memory after.
    pub async fn frame_schema(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
    ) -> Result<SchemaRef> {
        Ok(Arc::clone(
            &self.frame_spec(hive, box_name, frame_name).await?.schema,
        ))
    }

    /// Cap all the nectar in every Frame now, however small or young, rather
    /// than waiting for the interval: merge it to the standard Cell size, ripen
    /// it with the Frame's recipe and seal it.
    pub async fn cap_frames(&self) -> Result<CapReport> {
        let result = self.upkeep.cap_all_now().await;
        match &result {
            Ok(report) => self.mark("cap", format_args!("{report:?}")),
            Err(e) => self.mark("cap", format_args!("failed: {e}")),
        }
        result
    }

    /// Harvest capped Cells to the harvest store now (one paced pass per Frame).
    /// Fails if the node has no `harvest_uri`.
    pub async fn harvest(&self) -> Result<HarvestReport> {
        let result = self.upkeep.harvest_all().await;
        match &result {
            Ok(report) => self.mark("harvest", format_args!("{report:?}")),
            Err(e) => self.mark("harvest", format_args!("failed: {e}")),
        }
        result
    }

    /// Clear now: retire harvested Cells past retention, then delete the files
    /// no table version needs.
    pub async fn clear_comb(&self) -> Result<ClearReport> {
        let result = self.upkeep.clear_all().await;
        match &result {
            Ok(report) => self.mark("clear", format_args!("{report:?}")),
            Err(e) => self.mark("clear", format_args!("failed: {e}")),
        }
        result
    }

    /// Set how a Frame ripens: the columns its Cells are sorted by, and the
    /// columns that identify a duplicate (the latest row wins). Stored in the
    /// Frame's table properties.
    pub async fn set_recipe(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        sort_by: Vec<String>,
        dedup_by: Vec<String>,
    ) -> Result<()> {
        self.upkeep
            .set_recipe(hive, box_name, frame_name, &Recipe { sort_by, dedup_by })
            .await
    }

    /// A Frame's ripening recipe.
    pub async fn recipe(&self, hive: &str, box_name: &str, frame_name: &str) -> Result<Recipe> {
        self.upkeep.recipe(hive, box_name, frame_name).await
    }

    /// What ingest needs to know about a Frame, from the registry the first
    /// time and from memory after.
    async fn frame_spec(
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
        if let Some(spec) = self.frame_specs.read().await.get(&key) {
            return Ok(Arc::clone(spec));
        }
        let frame = self.registry.get_frame(hive, box_name, frame_name).await?;
        let spec = Arc::new(FrameSpec {
            schema: delta_schema(&FrameSchema::from_json_value(&frame.schema)?),
            partition_by: frame.partition_by.clone(),
        });
        self.frame_specs
            .write()
            .await
            .insert(key, Arc::clone(&spec));
        Ok(spec)
    }

    /// Overwrite all data in a frame with new data, in one Delta commit that
    /// removes every existing cell and adds the new ones. Rows still in the
    /// crop are discarded too: the same commit records them as superseded.
    pub async fn overwrite_frame(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
        batch: &RecordBatch,
    ) -> Result<WriteResult> {
        (self.commit_gate)()?;
        let start = self.env.clock().monotonic();

        // No deposit may run between choosing what to supersede and committing.
        let _paused = self.depositor.pause().await;
        let table = self.frame_table(hive, box_name, frame_name).await?;

        let log = self.crop.frame_if_exists(hive, box_name, frame_name)?;
        let superseded = match &log {
            Some(log) => {
                let log = Arc::clone(log);
                tokio::task::spawn_blocking(move || log.pending())
                    .await
                    .map_err(|e| ApiaryError::Internal {
                        message: format!("Crop task failed: {e}"),
                    })??
                    .last()
                    .map(|s| s.seq)
            }
            None => None,
        };
        let app_id = self.crop.app_id();

        // Ripening yields to users, and a user's overwrite does not fail because a
        // capping commit got in first: the fence makes that a conflict, and the
        // overwrite is rebuilt from the table as capping left it. Only a rewrite
        // wins that way (a concurrent append is a different conflict, and is
        // refused), so nothing the user did not see is dropped.
        let mut table = table;
        let mut attempts = 0;
        let committed = loop {
            let outcome = self
                .comb
                .overwrite_superseding(
                    &table,
                    batch,
                    self.config.target_cell_size,
                    CellState::Nectar,
                    superseded.map(|seq| (app_id.as_str(), seq)),
                )
                .await;
            match outcome {
                Err(e) if attempts < 5 && format!("{e:?}").contains("ConcurrentTransaction") => {
                    attempts += 1;
                    table = self.frame_table(hive, box_name, frame_name).await?;
                }
                other => break other?,
            }
        };

        // The commit is confirmed: the crop may let go of what it superseded.
        if let (Some(log), Some(seq)) = (log, superseded) {
            tokio::task::spawn_blocking(move || log.release(seq))
                .await
                .map_err(|e| ApiaryError::Internal {
                    message: format!("Crop task failed: {e}"),
                })??;
        }

        Ok(self.write_result(committed, start).await)
    }

    /// Create the Delta table for a frame (called after create_frame in the
    /// registry). Safe to call again: an existing table is left as it is.
    pub async fn init_frame_table(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
    ) -> Result<()> {
        self.frame_table(hive, box_name, frame_name).await?;
        Ok(())
    }

    /// Open a frame's Delta table, creating it from the registry's schema if
    /// it has not been written to yet.
    async fn frame_table(
        &self,
        hive: &str,
        box_name: &str,
        frame_name: &str,
    ) -> Result<apiary_comb::DeltaTable> {
        open_or_create_table(&self.comb, &self.registry, hive, box_name, frame_name).await
    }

    async fn write_result(
        &self,
        committed: apiary_comb::Committed,
        start: std::time::Duration,
    ) -> WriteResult {
        let duration_ms = (self.env.clock().monotonic() - start).as_millis() as u64;
        let temperature = self.colony.temperature();
        WriteResult {
            version: committed.version,
            cells_written: committed.cells,
            rows_written: committed.rows,
            bytes_written: committed.bytes,
            duration_ms,
            temperature,
        }
    }

    /// Execute a SQL query and return results as RecordBatches.
    ///
    /// The query waits for a Forager: a Bee that has chosen to forage takes it
    /// up and runs it under its share of the Node's memory pool, so an operator
    /// that would take more is refused and spills. A Node with too many queries
    /// waiting refuses more.
    ///
    /// Supports:
    /// - Standard SQL (SELECT, GROUP BY, ORDER BY, etc.) over frames
    /// - Custom commands: USE HIVE, USE BOX, SHOW HIVES, SHOW BOXES, SHOW FRAMES, DESCRIBE
    /// - 3-part table names: hive.box.frame
    /// - 1-part names after USE HIVE / USE BOX
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.mark(
            "query",
            query.split_whitespace().collect::<Vec<_>>().join(" "),
        );
        let ctx = Arc::clone(&self.query_ctx);
        let text = query.to_string();
        self.forage(move |pool| async move { ctx.sql_in(&text, Some(pool)).await })
            .await
    }

    /// Execute a SQL query like [`sql`](Self::sql), keeping the result's schema
    /// and the rows each stage gave it (which is how a query with no rows still
    /// has columns).
    pub async fn sql_with_stages(&self, query: &str) -> Result<apiary_plan::QueryOutput> {
        let ctx = Arc::clone(&self.query_ctx);
        let text = query.to_string();
        self.forage(move |pool| async move { ctx.sql_with_stages_in(&text, Some(pool)).await })
            .await
    }

    /// Hand a query to the Bees and wait for the Forager that takes it up. It
    /// runs under that Bee's share of the Node's memory, and fails if the Node
    /// is overloaded or the query takes longer than the task timeout.
    async fn forage<T, F, Fut>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn datafusion::execution::memory_pool::MemoryPool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.duties.submit_query(Box::new(move |pool| {
            Box::pin(async move {
                let _ = tx.send(work(pool).await);
            })
        }))?;
        self.colony.wake();
        let clock = self.env.clock();
        tokio::select! {
            result = rx => result.map_err(|_| ApiaryError::Internal {
                message: "The query was dropped before it finished".into(),
            })?,
            () = clock.sleep(QUERY_TIMEOUT) => Err(ApiaryError::Internal {
                message: format!("The query took longer than {QUERY_TIMEOUT:?}"),
            }),
        }
    }

    /// Return the status of each bee.
    pub async fn bee_status(&self) -> Vec<BeeStatus> {
        self.colony
            .bees()
            .into_iter()
            .map(|b| BeeStatus {
                bee_id: b.id,
                state: if b.busy {
                    format!("busy({})", b.role.name())
                } else {
                    "idle".to_string()
                },
                role: b.role.name().to_string(),
                age: b.age,
                cooling: b.cooling,
                memory_used: b.reserved as u64,
                memory_budget: b.budget as u64,
            })
            .collect()
    }

    /// Return the current world view snapshot.
    pub async fn world_view(&self) -> WorldView {
        self.world_view.read().await.clone()
    }

    /// Return swarm status: a summary of all nodes visible to this node.
    pub async fn swarm_status(&self) -> SwarmStatus {
        let view = self.world_view.read().await;
        let mut nodes = Vec::new();

        for status in view.nodes.values() {
            nodes.push(SwarmNodeInfo {
                node_id: status.node_id.as_str().to_string(),
                state: match status.state {
                    NodeState::Alive => "alive".to_string(),
                    NodeState::Suspect => "suspect".to_string(),
                    NodeState::Dead => "dead".to_string(),
                },
                bees: status.heartbeat.load.bees_total,
                idle_bees: status.heartbeat.load.bees_idle,
                memory_pressure: status.heartbeat.load.memory_pressure,
                colony_temperature: status.heartbeat.load.colony_temperature,
            });
        }

        // Sort by node_id for deterministic output
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        let total_bees: usize = nodes.iter().map(|n| n.bees).sum();
        let total_idle_bees: usize = nodes.iter().map(|n| n.idle_bees).sum();

        SwarmStatus {
            nodes,
            total_bees,
            total_idle_bees,
        }
    }

    /// Return the current colony status: temperature and regulation state.
    pub async fn colony_status(&self) -> ColonyStatus {
        let temperature = self.colony.temperature();
        ColonyStatus {
            temperature,
            regulation: TemperatureRegulation::of(temperature).as_str().to_string(),
            setpoint: 0.5,
            roles: self
                .colony
                .role_counts()
                .into_iter()
                .map(|(role, n)| (role.name().to_string(), n))
                .collect(),
        }
    }
}

/// A Node's Bees as the heartbeat reports them.
struct ColonyLoad {
    colony: Arc<Colony>,
    duties: Arc<NodeDuties>,
}

impl LoadSource for ColonyLoad {
    fn load(&self) -> HeartbeatLoad {
        let bees = self.colony.bees();
        let busy = bees.iter().filter(|b| b.busy).count();
        let used: usize = bees.iter().map(|b| b.reserved).sum();
        let budget: usize = bees.iter().map(|b| b.budget).sum();
        HeartbeatLoad {
            bees_total: bees.len(),
            bees_busy: busy,
            bees_idle: bees.len() - busy,
            memory_pressure: if budget > 0 {
                used as f64 / budget as f64
            } else {
                0.0
            },
            queue_depth: self.duties.queries_waiting(),
            colony_temperature: self.colony.temperature(),
        }
    }
}

/// How long a query may take before the caller gives up on it.
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// A Bee as seen from outside.
#[derive(Clone, Debug)]
pub struct BeeStatus {
    /// The Bee's id.
    pub bee_id: String,
    /// `idle`, or `busy(<role>)`.
    pub state: String,
    /// The role it holds, or last held.
    pub role: String,
    /// Completed Patches.
    pub age: u64,
    /// Whether it has stopped claiming because the Node is hot.
    pub cooling: bool,
    /// Memory it holds now, in bytes.
    pub memory_used: u64,
    /// The most it may hold, in bytes.
    pub memory_budget: u64,
}

/// Summary of the swarm as seen by this node.
#[derive(Debug, Clone)]
pub struct SwarmStatus {
    /// Info for each known node.
    pub nodes: Vec<SwarmNodeInfo>,
    /// Total bees across all nodes.
    pub total_bees: usize,
    /// Total idle bees across all nodes.
    pub total_idle_bees: usize,
}

/// Info about a single node in the swarm.
#[derive(Debug, Clone)]
pub struct SwarmNodeInfo {
    pub node_id: String,
    pub state: String,
    pub bees: usize,
    pub idle_bees: usize,
    pub memory_pressure: f64,
    pub colony_temperature: f64,
}

/// Colony temperature and regulation status.
#[derive(Debug, Clone)]
pub struct ColonyStatus {
    /// Current colony temperature (0.0 to 1.0).
    pub temperature: f64,
    /// Regulation state: "cold", "ideal", "warm", "hot", or "critical".
    pub regulation: String,
    /// Temperature setpoint.
    pub setpoint: f64,
    /// How many Bees hold each role now.
    pub roles: Vec<(String, usize)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start_local_node() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("local://test");
        config.storage_uri = format!("local://{}", tmp.path().display());
        let node = ApiaryNode::start(config).await.unwrap();
        assert!(node.config.cores > 0);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn test_start_with_env_uses_the_injected_clock() {
        use apiary_core::ManualClock;
        use chrono::TimeZone;

        let origin = chrono::Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        let env = Env::new(Arc::new(ManualClock::new(origin)), 7);
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("local://test");
        config.storage_uri = format!("local://{}", tmp.path().display());

        // The manual clock never advances, so the node's heartbeat and world
        // view carry exactly its time and not the machine's. (No shutdown:
        // its settle delay would wait on the stopped clock.)
        let node = ApiaryNode::start_with_env(config, env).await.unwrap();
        let view = node.world_view().await;
        assert_eq!(view.updated_at, origin);
        let status = view.nodes.values().next().expect("own heartbeat");
        assert_eq!(status.heartbeat.timestamp, origin);
    }

    #[tokio::test]
    async fn test_start_with_raw_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("test");
        config.storage_uri = tmp.path().to_string_lossy().to_string();
        let node = ApiaryNode::start(config).await.unwrap();
        node.shutdown().await;
    }
}
