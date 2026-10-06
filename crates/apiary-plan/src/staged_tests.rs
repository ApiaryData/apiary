//! Tests of Frames as queries see them: comb and crop together, with `_stage`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use apiary_comb::local::LocalBackend;
use apiary_comb::schema::{conform_batch, delta_schema};
use apiary_comb::{Comb, Crop};
use apiary_core::registry_manager::RegistryManager;
use apiary_core::{FrameSchema, NodeId, StorageBackend};

use crate::{ApiaryQueryContext, QueryOptions, ROWS_FROM_COMB, ROWS_FROM_CROP};

const CELL: u64 = 64 * 1024 * 1024;

struct Env {
    comb: Arc<Comb>,
    registry: Arc<RegistryManager>,
    crop: Arc<Crop>,
    schema: FrameSchema,
    _dir: TempDir,
}

fn schema_json() -> serde_json::Value {
    serde_json::json!({"region": "string", "temp": "float64"})
}

async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(dir.path().to_path_buf()).await.unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(backend);
    let registry = Arc::new(RegistryManager::new(storage));
    registry.load_or_create().await.unwrap();
    registry.create_hive("farm").await.unwrap();
    registry.create_box("farm", "field").await.unwrap();
    registry
        .create_frame("farm", "field", "sensors", schema_json(), vec![])
        .await
        .unwrap();
    Env {
        comb: Arc::new(Comb::from_local_path(dir.path()).unwrap()),
        registry,
        crop: Arc::new(
            Crop::open(dir.path().join("crop"))
                .unwrap()
                .with_sync(false),
        ),
        schema: FrameSchema::from_json_value(&schema_json()).unwrap(),
        _dir: dir,
    }
}

impl Env {
    fn context(&self) -> ApiaryQueryContext {
        ApiaryQueryContext::with_options(
            Arc::clone(&self.comb),
            Arc::clone(&self.registry),
            NodeId::from("test"),
            QueryOptions {
                crop: Some(Arc::clone(&self.crop)),
                ..QueryOptions::default()
            },
        )
        .unwrap()
    }

    /// Land a batch in the crop, as ingest does.
    fn ingest(&self, batch: &RecordBatch) {
        let conformed = conform_batch(batch, &delta_schema(&self.schema), &[]).unwrap();
        self.crop
            .frame("farm", "field", "sensors")
            .unwrap()
            .append(&conformed)
            .unwrap();
    }

    /// Deposit the first pending segment (or all of them) into the comb.
    /// With `release` false, stop before cleaning up: a crash in that window.
    async fn deposit(&self, all: bool, release: bool) {
        let frame = self.crop.frame("farm", "field", "sensors").unwrap();
        let mut pending = frame.pending().unwrap();
        if !all {
            pending.truncate(1);
        }
        let Some(last) = pending.last().map(|s| s.seq) else {
            return;
        };
        let batches = frame.read(&pending).unwrap();
        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        let table = self
            .comb
            .open_or_create_frame_table("farm", "field", "sensors", &self.schema, &[])
            .await
            .unwrap();
        self.comb
            .deposit(&table, &batch, CELL, &self.crop.app_id(), last)
            .await
            .unwrap();
        if release {
            frame.release(last).unwrap();
        }
    }
}

fn readings(region: &str, n: usize) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec![region; n])) as ArrayRef,
        ),
        (
            "temp",
            Arc::new(Float64Array::from(vec![20.0; n])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn first_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn count(ctx: &ApiaryQueryContext, sql: &str) -> i64 {
    first_i64(&ctx.sql(sql).await.unwrap())
}

const ALL: &str = "SELECT count(*) FROM farm.field.sensors";

/// A query that has to read the data: `count(*)` alone is answered from table
/// statistics without scanning, so it reads no rows from any stage.
const SCAN: &str = "SELECT sum(temp) FROM farm.field.sensors";

fn first_f64(batches: &[RecordBatch]) -> f64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn crop_rows_are_queryable_at_once_and_flagged() {
    let env = env().await;
    let ctx = env.context();
    env.ingest(&readings("north", 3));

    // No Delta table exists yet; the rows are only in the crop.
    assert_eq!(count(&ctx, ALL).await, 3);
    let stages = ctx
        .sql("SELECT _stage, count(*) AS n FROM farm.field.sensors GROUP BY _stage")
        .await
        .unwrap();
    assert_eq!(stages[0].num_rows(), 1);
    let name = stages[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert_eq!(name, "crop");
}

#[tokio::test]
async fn a_frame_shows_both_stages_until_the_crop_is_deposited() {
    let env = env().await;
    let ctx = env.context();
    env.ingest(&readings("north", 2));
    env.deposit(true, true).await;
    env.ingest(&readings("south", 3));

    let rows = ctx
        .sql("SELECT _stage, count(*) AS n FROM farm.field.sensors GROUP BY _stage ORDER BY _stage")
        .await
        .unwrap();
    let stages = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = rows[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(stages.value(0), "comb");
    assert_eq!(counts.value(0), 2);
    assert_eq!(stages.value(1), "crop");
    assert_eq!(counts.value(1), 3);

    env.deposit(true, true).await;
    assert_eq!(count(&ctx, ALL).await, 5);
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) FROM farm.field.sensors WHERE _stage = 'crop'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn every_frame_has_a_stage_column_last() {
    let env = env().await;
    env.ingest(&readings("north", 1));
    let out = env
        .context()
        .sql("SELECT * FROM farm.field.sensors")
        .await
        .unwrap();
    let names: Vec<_> = out[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["region", "temp", "_stage"]);
}

#[tokio::test]
async fn results_report_how_many_rows_each_stage_gave() {
    let env = env().await;
    let ctx = env.context();
    env.ingest(&readings("north", 4));
    env.deposit(true, true).await;
    env.ingest(&readings("south", 3));

    let output = ctx.sql_with_stages(SCAN).await.unwrap();
    assert_eq!(first_f64(&output.batches), 20.0 * 7.0);
    assert_eq!(output.stages.comb, 4);
    assert_eq!(output.stages.crop, 3);

    // The counts also travel in the schema of every result batch.
    let metadata = output.batches[0].schema().metadata().clone();
    assert_eq!(metadata.get(ROWS_FROM_CROP).map(String::as_str), Some("3"));
    assert_eq!(metadata.get(ROWS_FROM_COMB).map(String::as_str), Some("4"));
}

#[tokio::test]
async fn filtering_on_stage_skips_the_other_stage() {
    let env = env().await;
    let ctx = env.context();
    env.ingest(&readings("north", 4));
    env.deposit(true, true).await;
    env.ingest(&readings("south", 3));

    let comb_only = ctx
        .sql_with_stages("SELECT count(temp) FROM farm.field.sensors WHERE _stage = 'comb'")
        .await
        .unwrap();
    assert_eq!(first_i64(&comb_only.batches), 4);
    assert_eq!(comb_only.stages.crop, 0, "the crop was not read");

    let crop_only = ctx
        .sql_with_stages("SELECT count(temp) FROM farm.field.sensors WHERE _stage = 'crop'")
        .await
        .unwrap();
    assert_eq!(first_i64(&crop_only.batches), 3);
    assert_eq!(crop_only.stages.comb, 0, "the comb was not read");
}

#[tokio::test]
async fn rows_are_not_double_counted_in_the_crash_window() {
    // The deposit committed to the table, but the Node died before releasing
    // its segments: the rows are in both places.
    let env = env().await;
    let ctx = env.context();
    env.ingest(&readings("north", 4));
    env.deposit(true, false).await;
    assert_eq!(
        env.crop
            .frame("farm", "field", "sensors")
            .unwrap()
            .pending()
            .unwrap()
            .len(),
        1
    );

    assert_eq!(count(&ctx, ALL).await, 4, "each row counts once");
    let output = ctx.sql_with_stages(SCAN).await.unwrap();
    assert_eq!(first_f64(&output.batches), 20.0 * 4.0);
    assert_eq!(output.stages.comb, 4);
    assert_eq!(
        output.stages.crop, 0,
        "the deposited segment is not read twice"
    );
}

#[tokio::test]
async fn read_frame_includes_the_crop_but_not_the_stage_column() {
    let env = env().await;
    let ctx = env.context();
    env.ingest(&readings("north", 2));
    env.deposit(true, true).await;
    env.ingest(&readings("south", 3));

    let all = ctx
        .read_frame("farm", "field", "sensors", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(all.num_rows(), 5);
    let names: Vec<String> = all
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["region", "temp"]);

    let mut filter = HashMap::new();
    filter.insert("region".to_string(), "south".to_string());
    let south = ctx
        .read_frame("farm", "field", "sensors", Some(&filter))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(south.num_rows(), 3, "the filter applies to crop rows too");

    filter.insert("region".to_string(), "west".to_string());
    assert!(
        ctx.read_frame("farm", "field", "sensors", Some(&filter))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_query_never_loses_or_repeats_a_row_while_the_crop_is_being_deposited() {
    // 20 segments of 10 rows. One task deposits them one at a time while the
    // main task counts the frame over and over. Every count must be exactly 200:
    // the loader has to retry whenever a deposit lands underneath it.
    let env = Arc::new(env().await);
    let ctx = env.context();
    for _ in 0..20 {
        env.ingest(&readings("north", 10));
    }
    assert_eq!(count(&ctx, ALL).await, 200);

    let depositor = {
        let env = Arc::clone(&env);
        tokio::spawn(async move {
            for _ in 0..20 {
                env.deposit(false, true).await;
                tokio::task::yield_now().await;
            }
        })
    };

    let mut seen = 0;
    while !depositor.is_finished() || seen < 5 {
        assert_eq!(count(&ctx, ALL).await, 200, "query {seen}");
        seen += 1;
    }
    depositor.await.unwrap();
    assert_eq!(count(&ctx, ALL).await, 200);
    assert!(
        env.crop
            .frame("farm", "field", "sensors")
            .unwrap()
            .pending()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn querying_creates_no_crop_directories() {
    let env = env().await;
    let ctx = env.context();
    assert_eq!(count(&ctx, ALL).await, 0);
    assert!(env.crop.frames().unwrap().is_empty());
}

#[tokio::test]
async fn joins_over_frames_with_crop_rows_still_use_hash_joins() {
    let env = env().await;
    env.registry
        .create_frame(
            "farm",
            "field",
            "regions",
            serde_json::json!({"region": "string", "code": "int64"}),
            vec![],
        )
        .await
        .unwrap();
    env.ingest(&readings("north", 3));
    env.deposit(true, true).await;
    env.ingest(&readings("south", 2));

    let ctx = env.context();
    let out = ctx
        .sql(
            "EXPLAIN SELECT s.region FROM farm.field.sensors s \
             JOIN farm.field.regions r ON s.region = r.region",
        )
        .await
        .unwrap();
    let text: String = out
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n");
    let physical = text.split("physical_plan").last().unwrap();
    assert!(
        !physical.contains("SortMergeJoinExec"),
        "statistics must survive the staged view:\n{text}"
    );
}

#[tokio::test]
async fn a_crop_ahead_of_its_table_still_answers_queries() {
    // The crop has released segments the table does not know about, as if the
    // comb had been wiped or restored from a backup. Retrying cannot fix that;
    // queries must still work, from what the crop and the table have.
    let env = env().await;
    env.ingest(&readings("north", 2));
    env.deposit(true, true).await; // the crop is now released up to 1

    let wiped = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(wiped.path().to_path_buf()).await.unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(backend);
    let registry = Arc::new(RegistryManager::new(storage));
    registry.load_or_create().await.unwrap();
    registry.create_hive("farm").await.unwrap();
    registry.create_box("farm", "field").await.unwrap();
    registry
        .create_frame("farm", "field", "sensors", schema_json(), vec![])
        .await
        .unwrap();
    let ctx = ApiaryQueryContext::with_options(
        Arc::new(Comb::from_local_path(wiped.path()).unwrap()),
        registry,
        NodeId::from("test"),
        QueryOptions {
            crop: Some(Arc::clone(&env.crop)),
            ..QueryOptions::default()
        },
    )
    .unwrap();

    // Nothing is pending and the wiped table is empty: an empty answer, not an error.
    assert_eq!(count(&ctx, ALL).await, 0);

    // New rows ingested now are still served.
    env.ingest(&readings("south", 3));
    assert_eq!(count(&ctx, ALL).await, 3);
}
