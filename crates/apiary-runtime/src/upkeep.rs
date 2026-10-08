//! Looking after the comb on a Node: capping, harvest and clearing.
//!
//! Each pass walks the Frames in the registry and applies one of the
//! [`Comb`]'s upkeep operations to each Frame's table. A Frame that fails is
//! skipped and the rest go ahead; the first error is returned afterwards.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use apiary_comb::{CapOptions, CapReport, Comb, HarvestReport, Recipe};
use apiary_core::registry_manager::RegistryManager;
use apiary_core::{ApiaryError, Clock, CommitGate, FrameSchema, Result};

/// What a clearing pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClearReport {
    /// Harvested Cells removed from the site tables (past retention).
    pub retired: usize,
    /// Files deleted from the drive.
    pub deleted: usize,
}

/// How a Node's upkeep behaves.
#[derive(Clone, Debug)]
pub struct UpkeepSettings {
    /// The standard Cell size.
    pub target_cell_size: u64,
    /// How old a part-filled group of nectar must be before it is capped anyway.
    pub cap_max_age: Duration,
    /// The most bytes one harvest pass copies per Frame.
    pub harvest_batch_bytes: u64,
    /// How long harvested Cells stay on the drive (`None`: for ever).
    pub retention: Option<Duration>,
    /// How old an unneeded file must be before it is deleted.
    pub clear_grace: Duration,
}

/// A Frame as the registry declares it.
struct FrameRef {
    hive: String,
    box_name: String,
    frame: String,
    schema: FrameSchema,
    partition_by: Vec<String>,
}

/// Caps, harvests and clears a Node's comb.
pub struct Upkeep {
    comb: Arc<Comb>,
    harvest: Option<Arc<Comb>>,
    registry: Arc<RegistryManager>,
    settings: UpkeepSettings,
    clock: Arc<dyn Clock>,
    gate: Option<CommitGate>,
}

impl Upkeep {
    /// Create the upkeep for a comb. `harvest` is the comb the harvest tables
    /// live on, if the site harvests.
    pub fn new(
        comb: Arc<Comb>,
        harvest: Option<Arc<Comb>>,
        registry: Arc<RegistryManager>,
        settings: UpkeepSettings,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            comb,
            harvest,
            registry,
            settings,
            clock,
            gate: None,
        }
    }

    /// Ask `gate` before any pass that commits.
    pub fn with_gate(mut self, gate: CommitGate) -> Self {
        self.gate = Some(gate);
        self
    }

    fn check_gate(&self) -> Result<()> {
        match &self.gate {
            Some(gate) => gate(),
            None => Ok(()),
        }
    }

    /// Whether this Node harvests.
    pub fn harvests(&self) -> bool {
        self.harvest.is_some()
    }

    fn now_ms(&self) -> i64 {
        self.clock.now_utc().timestamp_millis()
    }

    async fn frames(&self) -> Result<Vec<FrameRef>> {
        let registry = self.registry.load_or_create().await?;
        let mut frames = Vec::new();
        for (hive_name, hive) in &registry.hives {
            for (box_name, box_) in &hive.boxes {
                for (frame_name, frame) in &box_.frames {
                    frames.push(FrameRef {
                        hive: hive_name.clone(),
                        box_name: box_name.clone(),
                        frame: frame_name.clone(),
                        schema: FrameSchema::from_json_value(&frame.schema)?,
                        partition_by: frame.partition_by.clone(),
                    });
                }
            }
        }
        frames.sort_by(|a, b| {
            (&a.hive, &a.box_name, &a.frame).cmp(&(&b.hive, &b.box_name, &b.frame))
        });
        Ok(frames)
    }

    /// Cap the nectar of every Frame by the usual policy: a group is capped
    /// once it is half a standard Cell, or its oldest Cell is `cap_max_age` old.
    pub async fn cap_all(&self) -> Result<CapReport> {
        self.cap_with(self.settings.cap_max_age).await
    }

    /// Cap all the nectar of every Frame, however small or young.
    pub async fn cap_all_now(&self) -> Result<CapReport> {
        self.cap_with(Duration::ZERO).await
    }

    async fn cap_with(&self, max_age: Duration) -> Result<CapReport> {
        self.check_gate()?;
        let options = CapOptions {
            target_cell_size: self.settings.target_cell_size,
            max_age,
            now_ms: self.now_ms(),
        };
        let mut total = CapReport::default();
        let mut first_error = None;
        for f in self.frames().await? {
            let outcome = async {
                match self
                    .comb
                    .open_frame_table(&f.hive, &f.box_name, &f.frame)
                    .await?
                {
                    Some(table) => self.comb.cap(&table, &options).await,
                    None => Ok(CapReport::default()),
                }
            }
            .await;
            match outcome {
                Ok(report) => total.add(&report),
                Err(e) => {
                    warn!(frame = %name(&f), error = %e, "Capping failed; will retry");
                    first_error.get_or_insert(e);
                }
            }
        }
        first_error.map_or(Ok(total), Err)
    }

    /// Harvest the capped Cells of every Frame: one pass each, up to the
    /// configured byte budget.
    pub async fn harvest_all(&self) -> Result<HarvestReport> {
        self.check_gate()?;
        let harvest = self.harvest.as_ref().ok_or_else(|| ApiaryError::Config {
            message: "This node has no harvest store; set harvest_uri".into(),
        })?;
        let mut total = HarvestReport::default();
        let mut first_error = None;
        for f in self.frames().await? {
            let outcome = async {
                let Some(site) = self
                    .comb
                    .open_frame_table(&f.hive, &f.box_name, &f.frame)
                    .await?
                else {
                    return Ok(HarvestReport::default());
                };
                self.comb
                    .harvest(
                        &site,
                        harvest,
                        (&f.hive, &f.box_name, &f.frame),
                        &f.schema,
                        &f.partition_by,
                        self.settings.harvest_batch_bytes,
                    )
                    .await
            }
            .await;
            match outcome {
                Ok(report) => total.add(&report),
                Err(e) => {
                    warn!(frame = %name(&f), error = %e, "Harvest failed; will retry");
                    first_error.get_or_insert(e);
                }
            }
        }
        if total.cells > 0 {
            info!(
                cells = total.cells,
                bytes = total.bytes,
                remaining = total.remaining,
                "Harvested"
            );
        }
        first_error.map_or(Ok(total), Err)
    }

    /// Retire harvested Cells past retention (if configured) and delete the
    /// files no table version needs.
    pub async fn clear_all(&self) -> Result<ClearReport> {
        self.check_gate()?;
        let mut total = ClearReport::default();
        let mut first_error = None;
        for f in self.frames().await? {
            let outcome = async {
                let Some(site) = self
                    .comb
                    .open_frame_table(&f.hive, &f.box_name, &f.frame)
                    .await?
                else {
                    return Ok(ClearReport::default());
                };
                let mut report = ClearReport::default();
                if let (Some(retention), Some(harvest)) =
                    (self.settings.retention, self.harvest.as_ref())
                    && let Some(target) = harvest
                        .open_frame_table(&f.hive, &f.box_name, &f.frame)
                        .await?
                {
                    report.retired = self
                        .comb
                        .retire_harvested(&site, &target, retention, self.now_ms())
                        .await?;
                }
                // Retiring committed, so look at the table as it is now.
                let site = self
                    .comb
                    .open_frame_table(&f.hive, &f.box_name, &f.frame)
                    .await?
                    .unwrap_or(site);
                report.deleted = self.comb.clear(&site, self.settings.clear_grace).await?;
                Ok::<_, ApiaryError>(report)
            }
            .await;
            match outcome {
                Ok(report) => {
                    total.retired += report.retired;
                    total.deleted += report.deleted;
                }
                Err(e) => {
                    warn!(frame = %name(&f), error = %e, "Clearing failed; will retry");
                    first_error.get_or_insert(e);
                }
            }
        }
        first_error.map_or(Ok(total), Err)
    }

    /// Set a Frame's ripening recipe, creating its table if needed.
    pub async fn set_recipe(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
        recipe: &Recipe,
    ) -> Result<()> {
        let table =
            crate::deposit::open_or_create_table(&self.comb, &self.registry, hive, box_name, frame)
                .await?;
        self.comb.set_recipe(&table, recipe).await?;
        Ok(())
    }

    /// A Frame's ripening recipe.
    pub async fn recipe(&self, hive: &str, box_name: &str, frame: &str) -> Result<Recipe> {
        let table =
            crate::deposit::open_or_create_table(&self.comb, &self.registry, hive, box_name, frame)
                .await?;
        self.comb.recipe(&table)
    }
}

fn name(f: &FrameRef) -> String {
    format!("{}.{}.{}", f.hive, f.box_name, f.frame)
}
