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

use apiary_comb::local::{LocalBackend, expand_local_path};
use apiary_comb::s3::S3Backend;
use apiary_comb::schema::{conform_batch, delta_schema};
use apiary_comb::{CapReport, CellState, Comb, Crop, HarvestReport, Recipe};
use apiary_core::config::NodeConfig;
use apiary_core::error::ApiaryError;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::storage::StorageBackend;
use apiary_core::{Env, FrameSchema, Result, WriteResult};
use apiary_plan::{ApiaryQueryContext, QueryOptions};

use crate::bee::{BeePool, BeeStatus};
use crate::behavioral::{AbandonmentTracker, ColonyThermometer};
use crate::cache::CellCache;
use crate::deposit::{DepositReport, Depositor, open_or_create_table};
use crate::heartbeat::{HeartbeatWriter, NodeState, WorldView, WorldViewBuilder};
use crate::upkeep::{ClearReport, Upkeep, UpkeepSettings};

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

    /// Pool of bees for isolated task execution.
    pub bee_pool: Arc<BeePool>,

    /// Local cell cache with LRU eviction.
    pub cell_cache: Arc<CellCache>,

    /// Colony thermometer for measuring system health.
    pub thermometer: ColonyThermometer,

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

    /// Wakes the deposit loop early when the crop grows large.
    deposit_wake: Arc<tokio::sync::Notify>,

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
        let storage: Arc<dyn StorageBackend> = if config.storage_uri.starts_with("s3://") {
            Arc::new(S3Backend::new(&config.storage_uri)?)
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
        let registry = Arc::new(RegistryManager::new(Arc::clone(&storage)));
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

        let depositor = Arc::new(Depositor::new(
            Arc::clone(&comb),
            Arc::clone(&crop),
            Arc::clone(&registry),
            config.target_cell_size,
        ));
        let deposit_wake = Arc::new(tokio::sync::Notify::new());
        let crop_bytes = Arc::new(AtomicU64::new(0));

        // Initialize bee pool
        let bee_pool = Arc::new(
            BeePool::new(&config).with_rng(env.rng(config.node_id.as_str(), BEE_POOL_STREAM)),
        );
        info!(bees = config.cores, "Bee pool initialized");

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
                Arc::clone(&bee_pool),
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

        // Start the deposit loop: it first deposits whatever an earlier run
        // left in the crop, then does so every interval, or sooner when the
        // crop grows large.
        {
            let depositor = Arc::clone(&depositor);
            let wake = Arc::clone(&deposit_wake);
            let crop_bytes = Arc::clone(&crop_bytes);
            let clock = env.clock();
            let interval = config.deposit_interval;
            let mut stop = cancel_rx.clone();
            tokio::spawn(async move {
                loop {
                    crop_bytes.store(0, Ordering::Relaxed);
                    match depositor.flush().await {
                        Ok(report) => Depositor::log(&report),
                        Err(e) => tracing::warn!(error = %e, "Crop deposit failed; will retry"),
                    }
                    tokio::select! {
                        _ = clock.sleep(interval) => {}
                        _ = wake.notified() => {}
                        _ = stop.wait_for(|stopped| *stopped) => break,
                    }
                }
            });
        }

        // Upkeep of the comb: capping, harvest (if the site has a harvest
        // store) and clearing, each on its own cadence.
        let harvest = match &config.harvest_uri {
            Some(uri) => Some(Arc::new(Comb::from_storage_uri(uri)?)),
            None => None,
        };
        let upkeep = Arc::new(Upkeep::new(
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
        ));
        {
            let upkeep = Arc::clone(&upkeep);
            spawn_periodic(
                env.clock(),
                config.cap_interval,
                cancel_rx.clone(),
                move || {
                    let upkeep = Arc::clone(&upkeep);
                    async move {
                        if let Err(e) = upkeep.cap_all().await {
                            tracing::warn!(error = %e, "Capping pass failed");
                        }
                    }
                },
            );
        }
        if upkeep.harvests() {
            let upkeep = Arc::clone(&upkeep);
            spawn_periodic(
                env.clock(),
                config.harvest_interval,
                cancel_rx.clone(),
                move || {
                    let upkeep = Arc::clone(&upkeep);
                    async move {
                        if let Err(e) = upkeep.harvest_all().await {
                            tracing::warn!(error = %e, "Harvest pass failed");
                        }
                    }
                },
            );
        }
        {
            let upkeep = Arc::clone(&upkeep);
            spawn_periodic(
                env.clock(),
                config.clear_interval,
                cancel_rx.clone(),
                move || {
                    let upkeep = Arc::clone(&upkeep);
                    async move {
                        if let Err(e) = upkeep.clear_all().await {
                            tracing::warn!(error = %e, "Clearing pass failed");
                        }
                    }
                },
            );
        }

        info!("Heartbeat, world view, deposit and upkeep background tasks started");

        Ok(Self {
            config,
            storage,
            comb,
            crop,
            registry,
            query_ctx,
            bee_pool,
            cell_cache,
            thermometer: ColonyThermometer::default(),
            abandonment_tracker: Arc::new(AbandonmentTracker::default()),
            heartbeat_writer,
            world_view,
            world_view_builder,
            depositor,
            upkeep,
            deposit_wake,
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
            self.deposit_wake.notify_one();
        }
        Ok(IngestResult {
            rows,
            segment: segment.map(|s| s.seq),
            crop_bytes: pending,
        })
    }

    /// Deposit everything in the crop into the comb now, rather than waiting
    /// for the interval.
    pub async fn flush_crop(&self) -> Result<DepositReport> {
        self.depositor.flush().await
    }

    /// Cap all the nectar in every Frame now, however small or young, rather
    /// than waiting for the interval: merge it to the standard Cell size, ripen
    /// it with the Frame's recipe and seal it.
    pub async fn cap_frames(&self) -> Result<CapReport> {
        self.upkeep.cap_all_now().await
    }

    /// Harvest capped Cells to the harvest store now (one paced pass per Frame).
    /// Fails if the node has no `harvest_uri`.
    pub async fn harvest(&self) -> Result<HarvestReport> {
        self.upkeep.harvest_all().await
    }

    /// Clear now: retire harvested Cells past retention, then delete the files
    /// no table version needs.
    pub async fn clear_comb(&self) -> Result<ClearReport> {
        self.upkeep.clear_all().await
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

        let committed = self
            .comb
            .overwrite_superseding(
                &table,
                batch,
                self.config.target_cell_size,
                CellState::Nectar,
                superseded.map(|seq| (app_id.as_str(), seq)),
            )
            .await?;

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
        let temperature = self.thermometer.measure(&self.bee_pool).await;
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
    /// The query is executed through the BeePool — assigned to an idle bee
    /// or queued if all bees are busy. Each bee runs in its own sealed
    /// chamber with memory budget and timeout enforcement.
    ///
    /// Supports:
    /// - Standard SQL (SELECT, GROUP BY, ORDER BY, etc.) over frames
    /// - Custom commands: USE HIVE, USE BOX, SHOW HIVES, SHOW BOXES, SHOW FRAMES, DESCRIBE
    /// - 3-part table names: hive.box.frame
    /// - 1-part names after USE HIVE / USE BOX
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let query_ctx = Arc::clone(&self.query_ctx);
        let query_owned = query.to_string();
        let rt_handle = tokio::runtime::Handle::current();

        let handle = self
            .bee_pool
            .submit(move || rt_handle.block_on(async { query_ctx.sql(&query_owned).await }))
            .await;

        handle.await.map_err(|e| ApiaryError::Internal {
            message: format!("Task join error: {e}"),
        })?
    }

    /// Return the status of each bee in the pool.
    pub async fn bee_status(&self) -> Vec<BeeStatus> {
        self.bee_pool.status().await
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
        let temperature = self.thermometer.measure(&self.bee_pool).await;
        let regulation = self.thermometer.regulation(temperature);

        ColonyStatus {
            temperature,
            regulation: regulation.as_str().to_string(),
            setpoint: self.thermometer.setpoint(),
        }
    }
}

/// Run `work` every `interval` until the node stops. The first run is one
/// interval after the start.
fn spawn_periodic<F, Fut>(
    clock: Arc<dyn apiary_core::Clock>,
    interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
    mut work: F,
) where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = clock.sleep(interval) => {}
                _ = stop.wait_for(|stopped| *stopped) => break,
            }
            work().await;
        }
    });
}

/// Stream index for the bee pool's random numbers, distinct from any Bee index.
const BEE_POOL_STREAM: u64 = u64::MAX;

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
