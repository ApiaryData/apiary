//! Integration tests: ripening on deposit, capping in the comb, harvest to a
//! second store, retirement and clearing, through a running Node.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use apiary_comb::Comb;
use apiary_core::config::NodeConfig;
use apiary_runtime::ApiaryNode;

const HOUR: Duration = Duration::from_secs(3600);

/// A node whose upkeep runs only when told to.
async fn start(dir: &Path, harvest: Option<&Path>) -> ApiaryNode {
    let mut config = base_config(dir, harvest);
    config.cap_interval = HOUR;
    config.harvest_interval = HOUR;
    config.clear_interval = HOUR;
    ApiaryNode::start(config).await.expect("Node should start")
}

fn base_config(dir: &Path, harvest: Option<&Path>) -> NodeConfig {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", dir.join("site").display());
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = dir.join("cache");
    config.deposit_interval = HOUR;
    config.crop_max_bytes = u64::MAX;
    // Cap whatever is nectar, and clear at once.
    config.cap_max_age = Duration::ZERO;
    config.clear_grace = Duration::ZERO;
    config.harvest_uri = harvest.map(|p| format!("local://{}", p.display()));
    config
}

async fn create_frame(node: &ApiaryNode) {
    let _ = node.registry.create_hive("farm").await;
    let _ = node.registry.create_box("farm", "field").await;
    node.registry
        .create_frame(
            "farm",
            "field",
            "readings",
            serde_json::json!({"id": "int64", "val": "string"}),
            vec![],
        )
        .await
        .unwrap();
}

fn batch(ids: Vec<i64>, val: &str) -> RecordBatch {
    let n = ids.len();
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
        ("val", Arc::new(StringArray::from(vec![val; n])) as ArrayRef),
    ])
    .unwrap()
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

async fn site_ids(node: &ApiaryNode) -> Vec<i64> {
    match node
        .read_from_frame("farm", "field", "readings", None)
        .await
        .unwrap()
    {
        Some(b) => ids(&b),
        None => Vec::new(),
    }
}

async fn harvested_ids(harvest: &Path) -> Vec<i64> {
    let comb = Comb::from_local_path(harvest).unwrap();
    let Some(table) = comb
        .open_frame_table("farm", "field", "readings")
        .await
        .unwrap()
    else {
        return Vec::new();
    };
    match comb.read(&table, None).await.unwrap() {
        Some(b) => ids(&b),
        None => Vec::new(),
    }
}

fn parquet_files(dir: &Path) -> usize {
    std::fs::read_dir(dir.join("site/farm/field/readings"))
        .map(|d| {
            d.filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".parquet")
            })
            .count()
        })
        .unwrap_or(0)
}

#[tokio::test]
async fn data_ripens_caps_harvests_and_clears() {
    let tmp = tempfile::TempDir::new().unwrap();
    let harvest = tmp.path().join("harvest");
    let node = start(tmp.path(), Some(&harvest)).await;
    create_frame(&node).await;
    node.set_recipe(
        "farm",
        "field",
        "readings",
        vec!["id".into()],
        vec!["id".into()],
    )
    .await
    .unwrap();
    let recipe = node.recipe("farm", "field", "readings").await.unwrap();
    assert_eq!(recipe.sort_by, vec!["id"]);
    assert_eq!(recipe.dedup_by, vec!["id"]);

    // Out of order, with a duplicate (id 2 arrives again, later, as "new").
    node.ingest("farm", "field", "readings", &batch(vec![5, 2, 9], "old"))
        .await
        .unwrap();
    node.flush_crop().await.unwrap();
    node.ingest("farm", "field", "readings", &batch(vec![7, 2, 1], "new"))
        .await
        .unwrap();
    node.flush_crop().await.unwrap();
    assert_eq!(
        parquet_files(tmp.path()),
        2,
        "two deposits, two nectar Cells"
    );

    // Nothing is capped yet, so nothing is harvested.
    assert_eq!(harvested_ids(&harvest).await, Vec::<i64>::new());
    assert_eq!(node.harvest().await.unwrap().cells, 0);

    // Capping merges them, sorts, and the latest "2" wins.
    let capped = node.cap_frames().await.unwrap();
    assert_eq!(capped.nectar_cells, 2);
    assert_eq!(capped.capped_cells, 1);
    assert_eq!(capped.rows, 5);
    assert_eq!(site_ids(&node).await, vec![1, 2, 5, 7, 9]);
    let vals = node
        .sql("SELECT val FROM farm.field.readings WHERE id = 2")
        .await
        .unwrap();
    let vals = vals[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert_eq!(vals, "new");

    // Harvest copies the capped Cell, once.
    let first = node.harvest().await.unwrap();
    assert_eq!(first.cells, 1);
    assert_eq!(harvested_ids(&harvest).await, vec![1, 2, 5, 7, 9]);
    assert_eq!(node.harvest().await.unwrap().cells, 0);

    // Clearing deletes the nectar files capping replaced; no retention is
    // set, so the capped Cell stays on the drive.
    let cleared = node.clear_comb().await.unwrap();
    assert_eq!(cleared.retired, 0);
    assert_eq!(cleared.deleted, 2);
    assert_eq!(parquet_files(tmp.path()), 1);
    assert_eq!(site_ids(&node).await, vec![1, 2, 5, 7, 9]);

    node.shutdown().await;
}

#[tokio::test]
async fn harvest_without_a_harvest_store_is_refused() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start(tmp.path(), None).await;
    create_frame(&node).await;
    let err = node.harvest().await.expect_err("no harvest_uri");
    assert!(err.to_string().contains("harvest"), "{err}");
    node.shutdown().await;
}

#[tokio::test]
async fn retention_retires_only_what_the_harvest_holds() {
    let tmp = tempfile::TempDir::new().unwrap();
    let harvest = tmp.path().join("harvest");
    let mut config = base_config(tmp.path(), Some(&harvest));
    config.cap_interval = HOUR;
    config.harvest_interval = HOUR;
    config.clear_interval = HOUR;
    config.retention = Some(Duration::ZERO);
    let node = ApiaryNode::start(config).await.unwrap();
    create_frame(&node).await;

    node.ingest("farm", "field", "readings", &batch(vec![1, 2], "a"))
        .await
        .unwrap();
    node.flush_crop().await.unwrap();
    node.cap_frames().await.unwrap();

    // Capped but not harvested: it never leaves the drive.
    let cleared = node.clear_comb().await.unwrap();
    assert_eq!(cleared.retired, 0);
    assert_eq!(site_ids(&node).await, vec![1, 2]);

    // Harvested and past retention: it leaves the drive, and the harvest keeps it.
    node.harvest().await.unwrap();
    let cleared = node.clear_comb().await.unwrap();
    assert_eq!(cleared.retired, 1);
    assert_eq!(site_ids(&node).await, Vec::<i64>::new());
    assert_eq!(harvested_ids(&harvest).await, vec![1, 2]);

    node.shutdown().await;
}

#[tokio::test]
async fn background_upkeep_caps_and_harvests_on_its_own() {
    let tmp = tempfile::TempDir::new().unwrap();
    let harvest = tmp.path().join("harvest");
    let mut config = base_config(tmp.path(), Some(&harvest));
    config.cap_interval = Duration::from_millis(100);
    config.harvest_interval = Duration::from_millis(100);
    config.clear_interval = Duration::from_millis(100);
    // A zero grace could delete a file a concurrent write has not committed yet.
    config.clear_grace = HOUR;
    let node = ApiaryNode::start(config).await.unwrap();
    create_frame(&node).await;

    node.ingest("farm", "field", "readings", &batch(vec![3, 1, 2], "a"))
        .await
        .unwrap();
    node.flush_crop().await.unwrap();

    let mut got = Vec::new();
    for _ in 0..200 {
        got = harvested_ids(&harvest).await;
        if !got.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    got.sort();
    assert_eq!(
        got,
        vec![1, 2, 3],
        "capped and harvested with no call from us"
    );
    node.shutdown().await;
}
