//! Integration tests: ingest lands in the crop, queries see it at once, and the
//! deposit loop moves it into the comb without losing or repeating a row.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use apiary_core::config::NodeConfig;
use apiary_core::{ApiaryError, FrameSchema};
use apiary_runtime::ApiaryNode;

const HOUR: Duration = Duration::from_secs(3600);

async fn start(dir: &Path, deposit_interval: Duration, crop_max_bytes: u64) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", dir.display());
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = dir.join("cache");
    config.deposit_interval = deposit_interval;
    config.crop_max_bytes = crop_max_bytes;
    ApiaryNode::start(config).await.expect("Node should start")
}

/// A node that deposits only when told to.
async fn start_manual(dir: &Path) -> ApiaryNode {
    start(dir, HOUR, u64::MAX).await
}

async fn create_frame(node: &ApiaryNode, schema: serde_json::Value) {
    let _ = node.registry.create_hive("farm").await;
    let _ = node.registry.create_box("farm", "field").await;
    node.registry
        .create_frame("farm", "field", "readings", schema, vec![])
        .await
        .unwrap();
}

fn plain_schema() -> serde_json::Value {
    serde_json::json!({"region": "string", "n": "int64"})
}

fn batch(region: &str, values: std::ops::Range<i64>) -> RecordBatch {
    let n = values.clone().count();
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec![region; n])) as ArrayRef,
        ),
        (
            "n",
            Arc::new(Int64Array::from_iter_values(values)) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn count(node: &ApiaryNode, filter: &str) -> i64 {
    let sql = format!("SELECT count(n) FROM farm.field.readings {filter}");
    node.sql(&sql).await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn in_crop(node: &ApiaryNode) -> i64 {
    count(node, "WHERE _stage = 'crop'").await
}

async fn in_comb(node: &ApiaryNode) -> i64 {
    count(node, "WHERE _stage = 'comb'").await
}

/// Wait until `check` holds, or fail after ten seconds.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..200 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test]
async fn ingested_rows_are_queryable_at_once_then_move_to_the_comb() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_manual(tmp.path()).await;
    create_frame(&node, plain_schema()).await;

    let result = node
        .ingest("farm", "field", "readings", &batch("north", 0..3))
        .await
        .unwrap();
    assert_eq!(result.rows, 3);
    assert_eq!(result.segment, Some(1));

    // Queryable before any deposit, and flagged as not yet shipped.
    assert_eq!(count(&node, "").await, 3);
    assert_eq!(in_crop(&node).await, 3);
    assert_eq!(in_comb(&node).await, 0);

    let report = node.flush_crop().await.unwrap();
    assert_eq!((report.frames, report.segments, report.rows), (1, 1, 3));

    assert_eq!(count(&node, "").await, 3, "no row lost or repeated");
    assert_eq!(in_crop(&node).await, 0);
    assert_eq!(in_comb(&node).await, 3);
    assert_eq!(node.crop.pending_bytes().unwrap(), 0);
    node.shutdown().await;
}

#[tokio::test]
async fn results_say_how_many_rows_came_from_each_stage() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_manual(tmp.path()).await;
    create_frame(&node, plain_schema()).await;
    node.ingest("farm", "field", "readings", &batch("north", 0..4))
        .await
        .unwrap();
    node.flush_crop().await.unwrap();
    node.ingest("farm", "field", "readings", &batch("south", 4..6))
        .await
        .unwrap();

    let out = node
        .sql("SELECT sum(n) FROM farm.field.readings")
        .await
        .unwrap();
    let metadata = out[0].schema().metadata().clone();
    assert_eq!(
        metadata.get("apiary.rows.comb").map(String::as_str),
        Some("4")
    );
    assert_eq!(
        metadata.get("apiary.rows.crop").map(String::as_str),
        Some("2")
    );
    node.shutdown().await;
}

#[tokio::test]
async fn read_from_frame_includes_rows_still_in_the_crop() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_manual(tmp.path()).await;
    create_frame(&node, plain_schema()).await;
    node.ingest("farm", "field", "readings", &batch("north", 0..3))
        .await
        .unwrap();

    let read = node
        .read_from_frame("farm", "field", "readings", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.num_rows(), 3);
    assert_eq!(read.num_columns(), 2, "no _stage column");
    node.shutdown().await;
}

#[tokio::test]
async fn the_deposit_interval_moves_the_crop_on_its_own() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start(tmp.path(), Duration::from_millis(150), u64::MAX).await;
    create_frame(&node, plain_schema()).await;
    node.ingest("farm", "field", "readings", &batch("north", 0..5))
        .await
        .unwrap();

    eventually("the rows reach the comb", || async {
        in_comb(&node).await == 5
    })
    .await;
    assert_eq!(in_crop(&node).await, 0);
    node.shutdown().await;
}

#[tokio::test]
async fn a_large_crop_deposits_before_the_interval() {
    let tmp = tempfile::TempDir::new().unwrap();
    // An hour-long interval, but any ingest is over the one-byte limit.
    let node = start(tmp.path(), HOUR, 1).await;
    create_frame(&node, plain_schema()).await;
    // Let the loop's first (empty) pass finish and start waiting.
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.ingest("farm", "field", "readings", &batch("north", 0..5))
        .await
        .unwrap();
    eventually("the size trigger deposits early", || async {
        in_comb(&node).await == 5
    })
    .await;
    node.shutdown().await;
}

#[tokio::test]
async fn rows_ingested_before_a_crash_are_deposited_by_the_next_run() {
    let tmp = tempfile::TempDir::new().unwrap();
    {
        let node = start_manual(tmp.path()).await;
        create_frame(&node, plain_schema()).await;
        node.ingest("farm", "field", "readings", &batch("north", 0..4))
            .await
            .unwrap();
        // The Node dies here: no shutdown, nothing deposited.
    }

    let node = start_manual(tmp.path()).await;
    eventually("the new run deposits the old crop", || async {
        in_comb(&node).await == 4
    })
    .await;
    assert_eq!(count(&node, "").await, 4, "each row exactly once");
    assert_eq!(in_crop(&node).await, 0);
    node.shutdown().await;
}

#[tokio::test]
async fn a_deposit_that_committed_but_was_not_released_is_not_repeated() {
    let tmp = tempfile::TempDir::new().unwrap();
    {
        let node = start_manual(tmp.path()).await;
        create_frame(&node, plain_schema()).await;
        node.ingest("farm", "field", "readings", &batch("north", 0..4))
            .await
            .unwrap();

        // Commit the deposit by hand and stop before releasing the segments:
        // the Node dies between the commit and the cleanup.
        let log = node.crop.frame("farm", "field", "readings").unwrap();
        let pending = log.pending().unwrap();
        let last = pending.last().unwrap().seq;
        let batches = log.read(&pending).unwrap();
        let rows = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let schema = FrameSchema::from_json_value(&plain_schema()).unwrap();
        let table = node
            .comb
            .create_frame_table("farm", "field", "readings", &schema, &[])
            .await
            .unwrap();
        node.comb
            .deposit(&table, &rows, 64 * 1024 * 1024, &node.crop.app_id(), last)
            .await
            .unwrap();
        assert_eq!(log.pending().unwrap().len(), 1, "still in the crop");
    }

    let node = start_manual(tmp.path()).await;
    // The rows are in both places on disk; queries count them once, and the
    // next deposit releases the segment rather than committing it again.
    assert_eq!(count(&node, "").await, 4);
    eventually("the leftover segment is released", || async {
        node.crop.pending_bytes().unwrap() == 0
    })
    .await;
    assert_eq!(count(&node, "").await, 4, "not deposited twice");
    assert_eq!(in_comb(&node).await, 4);
    node.shutdown().await;
}

#[tokio::test]
async fn a_graceful_shutdown_leaves_nothing_only_on_the_nodes_disk() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_manual(tmp.path()).await;
    create_frame(&node, plain_schema()).await;
    node.ingest("farm", "field", "readings", &batch("north", 0..3))
        .await
        .unwrap();
    node.shutdown().await;
    assert_eq!(node.crop.pending_bytes().unwrap(), 0);

    // Another Node, with its own crop, finds the rows in the comb.
    let other_dir = tempfile::TempDir::new().unwrap();
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmp.path().display());
    config.cache_dir = other_dir.path().join("cache");
    config.deposit_interval = HOUR;
    let other = ApiaryNode::start(config).await.unwrap();
    assert_eq!(in_comb(&other).await, 3);
    other.shutdown().await;
}

#[tokio::test]
async fn a_bad_batch_is_refused_at_the_entrance_and_nothing_is_written() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_manual(tmp.path()).await;
    create_frame(
        &node,
        serde_json::json!({"fields": [
            {"name": "region", "data_type": "string", "nullable": false},
            {"name": "n", "data_type": "int64"}
        ]}),
    )
    .await;

    let missing_region =
        RecordBatch::try_from_iter(vec![("n", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let err = node
        .ingest("farm", "field", "readings", &missing_region)
        .await
        .unwrap_err();
    assert!(matches!(err, ApiaryError::Schema { .. }), "{err:?}");
    assert_eq!(node.crop.pending_bytes().unwrap(), 0);
    node.shutdown().await;
}

#[tokio::test]
async fn ingesting_into_an_unknown_frame_is_entity_not_found() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_manual(tmp.path()).await;
    let err = node
        .ingest("nope", "nope", "nope", &batch("north", 0..1))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiaryError::EntityNotFound { .. }), "{err:?}");
    node.shutdown().await;
}

#[tokio::test]
async fn an_overwrite_discards_the_rows_still_in_the_crop() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_manual(tmp.path()).await;
    create_frame(&node, plain_schema()).await;
    node.ingest("farm", "field", "readings", &batch("north", 0..3))
        .await
        .unwrap();

    node.overwrite_frame("farm", "field", "readings", &batch("south", 100..101))
        .await
        .unwrap();
    assert_eq!(count(&node, "").await, 1, "the crop rows are gone");

    // They must not come back when the crop is next deposited.
    node.flush_crop().await.unwrap();
    assert_eq!(count(&node, "").await, 1);
    assert_eq!(node.crop.pending_bytes().unwrap(), 0);

    // And ingest still works afterwards.
    node.ingest("farm", "field", "readings", &batch("north", 200..202))
        .await
        .unwrap();
    assert_eq!(count(&node, "").await, 3);
    node.flush_crop().await.unwrap();
    assert_eq!(count(&node, "").await, 3);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ingest_and_deposit_lose_and_repeat_nothing() {
    let tmp = tempfile::TempDir::new().unwrap();
    // A short interval, so deposits run while ingest is under way.
    let node = Arc::new(start(tmp.path(), Duration::from_millis(40), u64::MAX).await);
    create_frame(&node, plain_schema()).await;

    let writers = 6;
    let per_writer = 20;
    let mut handles = Vec::new();
    for w in 0..writers {
        let node = Arc::clone(&node);
        handles.push(tokio::spawn(async move {
            for i in 0..per_writer {
                let id = (w * per_writer + i) as i64;
                node.ingest("farm", "field", "readings", &batch("north", id..id + 1))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }

    // Count while it runs: the total only ever grows to the final figure and
    // never exceeds it, whichever stage the rows are in.
    let total = (writers * per_writer) as i64;
    let mut last = 0;
    while handles.iter().any(|h| !h.is_finished()) {
        let now = count(&node, "").await;
        assert!(now >= last && now <= total, "saw {now} after {last}");
        last = now;
    }
    for handle in handles {
        handle.await.unwrap();
    }

    node.flush_crop().await.unwrap();
    assert_eq!(count(&node, "").await, total);
    assert_eq!(in_comb(&node).await, total);
    let distinct = node
        .sql("SELECT count(DISTINCT n) FROM farm.field.readings")
        .await
        .unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(distinct, total, "no row repeated");
    node.shutdown().await;
}
