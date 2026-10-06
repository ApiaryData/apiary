//! The query catalogue: Hive, Box and Frame resolved natively by DataFusion.
//!
//! DataFusion resolves `hive.box.frame` through a catalogue list, a catalogue
//! (the Hive) and a schema (the Box). This catalogue answers those lookups from
//! the registry and the comb, on demand, so every Node keeps one long-lived
//! session and no query pre-registers tables.
//!
//! Lookups are lazy because the registry lives in the comb store, which a
//! synchronous trait method cannot await. Listing methods that DataFusion
//! calls synchronously (`catalog_names`, `schema_names`, `table_names`)
//! therefore return nothing; `SHOW HIVES`, `SHOW BOXES` and `SHOW FRAMES` read
//! the registry directly instead.
//!
//! Names are matched against the registry exactly first, then
//! case-insensitively, because DataFusion lower-cases unquoted identifiers.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::SessionState;
use tracing::info;

use apiary_comb::Comb;
use apiary_comb::schema::delta_schema;
use apiary_core::registry_manager::RegistryManager;
use apiary_core::{ApiaryError, FrameSchema};

/// Default catalogue name when no Hive has been selected with `USE HIVE`.
pub(crate) const NO_HIVE: &str = "_no_hive_selected";

/// Default schema name when no Box has been selected with `USE BOX`.
pub(crate) const NO_BOX: &str = "_no_box_selected";

tokio::task_local! {
    /// Where the catalogue records time spent opening tables, for the
    /// `APIARY_TIMING` output. Set around one query by [`record_table_io`].
    static TABLE_IO: TableIo;
}

/// Time spent opening Frame tables during one query.
#[derive(Default)]
pub(crate) struct TableIo {
    /// Reading each table's Delta log.
    pub file_discovery: Cell<Duration>,
    /// Building each table's scan (file index and statistics).
    pub metadata_read: Cell<Duration>,
}

/// Run `fut`, recording table-open time into `io`.
pub(crate) async fn record_table_io<F: std::future::Future>(
    io: TableIo,
    fut: F,
) -> (F::Output, TableIo) {
    // `scope` hands the value back only through the closure, so move the
    // result out alongside it.
    TABLE_IO
        .scope(io, async {
            let out = fut.await;
            let io = TABLE_IO.with(|io| TableIo {
                file_discovery: Cell::new(io.file_discovery.get()),
                metadata_read: Cell::new(io.metadata_read.get()),
            });
            (out, io)
        })
        .await
}

fn add_io(select: impl Fn(&TableIo) -> &Cell<Duration>, elapsed: Duration) {
    let _ = TABLE_IO.try_with(|io| {
        let cell = select(io);
        cell.set(cell.get() + elapsed);
    });
}

/// Wrap an Apiary error so it can travel through DataFusion and be recovered
/// by [`into_apiary_error`].
pub(crate) fn external(e: ApiaryError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Recover an [`ApiaryError`] that was raised inside the catalogue, or wrap
/// any other DataFusion error with `context`.
pub(crate) fn into_apiary_error(e: DataFusionError, context: &str) -> ApiaryError {
    match e {
        DataFusionError::External(inner) => match inner.downcast::<ApiaryError>() {
            Ok(apiary) => *apiary,
            Err(other) => ApiaryError::Internal {
                message: format!("{context}: {other}"),
            },
        },
        DataFusionError::Context(_, inner) => into_apiary_error(*inner, context),
        DataFusionError::Diagnostic(_, inner) => into_apiary_error(*inner, context),
        other => ApiaryError::Internal {
            message: format!("{context}: {other}"),
        },
    }
}

/// What the catalogue needs to resolve and open Frames.
pub(crate) struct CatalogShared {
    pub registry: Arc<RegistryManager>,
    pub comb: Arc<Comb>,
    /// A session state with the node's runtime and settings, used to build
    /// table scans and to register table object stores with the runtime.
    pub scan_state: SessionState,
}

/// The catalogue list: every name is a Hive, resolved lazily.
pub(crate) struct ApiaryCatalogList {
    shared: Arc<CatalogShared>,
}

impl ApiaryCatalogList {
    pub(crate) fn new(shared: Arc<CatalogShared>) -> Self {
        Self { shared }
    }
}

impl std::fmt::Debug for ApiaryCatalogList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiaryCatalogList").finish_non_exhaustive()
    }
}

impl CatalogProviderList for ApiaryCatalogList {
    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // Hives are created through the registry, never through SQL.
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(HiveCatalog {
            hive: name.to_string(),
            shared: Arc::clone(&self.shared),
        }))
    }
}

/// A Hive, as a DataFusion catalogue.
struct HiveCatalog {
    hive: String,
    shared: Arc<CatalogShared>,
}

impl std::fmt::Debug for HiveCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HiveCatalog")
            .field("hive", &self.hive)
            .finish_non_exhaustive()
    }
}

impl CatalogProvider for HiveCatalog {
    fn schema_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(BoxSchema {
            hive: self.hive.clone(),
            box_name: name.to_string(),
            shared: Arc::clone(&self.shared),
        }))
    }
}

/// A Box, as a DataFusion schema. Its tables are Frames.
struct BoxSchema {
    hive: String,
    box_name: String,
    shared: Arc<CatalogShared>,
}

impl std::fmt::Debug for BoxSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxSchema")
            .field("hive", &self.hive)
            .field("box", &self.box_name)
            .finish_non_exhaustive()
    }
}

/// The registry's name for `wanted`: an exact match, else a unique
/// case-insensitive one.
fn resolve_name<'a, T>(map: &'a HashMap<String, T>, wanted: &str) -> Option<&'a str> {
    if let Some((key, _)) = map.get_key_value(wanted) {
        return Some(key.as_str());
    }
    let mut matches = map.keys().filter(|k| k.eq_ignore_ascii_case(wanted));
    match (matches.next(), matches.next()) {
        (Some(only), None) => Some(only.as_str()),
        _ => None,
    }
}

#[async_trait]
impl SchemaProvider for BoxSchema {
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn table_exist(&self, _name: &str) -> bool {
        // Frames are created through the registry, so SQL never needs to ask
        // (CREATE is blocked); existence cannot be checked synchronously.
        false
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let reference = format!("{}.{}.{}", self.hive, self.box_name, name);

        if self.hive == NO_HIVE {
            return Err(external(ApiaryError::Resolution {
                path: name.to_string(),
                reason: "No hive selected. Use 3-part name (hive.box.frame) or run USE HIVE first."
                    .into(),
            }));
        }
        if self.box_name == NO_BOX {
            return Err(external(ApiaryError::Resolution {
                path: name.to_string(),
                reason: "No box selected. Use 3-part name or run USE BOX first.".into(),
            }));
        }

        // Resolve the names against the registry.
        let registry = self
            .shared
            .registry
            .load_or_create()
            .await
            .map_err(external)?;
        let hive_name = resolve_name(&registry.hives, &self.hive).ok_or_else(|| {
            external(ApiaryError::EntityNotFound {
                entity_type: "Hive".into(),
                name: self.hive.clone(),
            })
        })?;
        let hive = &registry.hives[hive_name];
        let box_name = resolve_name(&hive.boxes, &self.box_name).ok_or_else(|| {
            external(ApiaryError::EntityNotFound {
                entity_type: "Box".into(),
                name: format!("{}.{}", self.hive, self.box_name),
            })
        })?;
        let box_ = &hive.boxes[box_name];
        let frame_name = resolve_name(&box_.frames, name).ok_or_else(|| {
            external(ApiaryError::EntityNotFound {
                entity_type: "Frame".into(),
                name: reference.clone(),
            })
        })?;
        let frame = &box_.frames[frame_name];

        // Open the table: read its Delta log.
        let opened_at = Instant::now();
        let table = self
            .shared
            .comb
            .open_frame_table(hive_name, box_name, frame_name)
            .await
            .map_err(external)?;
        add_io(|io| &io.file_discovery, opened_at.elapsed());

        // Build the scan: file index and statistics.
        let scan_at = Instant::now();
        let provider: Arc<dyn TableProvider> = match table {
            Some(table) => {
                let provider = self
                    .shared
                    .comb
                    .table_provider(&self.shared.scan_state, &table)
                    .await
                    .map_err(external)?;
                info!(frame = %reference, version = ?table.version(), "Frame table resolved");
                provider
            }
            None => {
                // A registered frame that has never been written is empty,
                // with the schema it was created with.
                let schema =
                    delta_schema(&FrameSchema::from_json_value(&frame.schema).map_err(external)?);
                let empty = RecordBatch::new_empty(schema);
                Arc::new(MemTable::try_new(empty.schema(), vec![vec![empty]])?)
            }
        };
        add_io(|io| &io.metadata_read, scan_at.elapsed());

        Ok(Some(provider))
    }
}
