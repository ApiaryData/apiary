//! A Node with no trustworthy clock keeps ingesting but will not commit.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use chrono::TimeZone;

use apiary_core::config::NodeConfig;
use apiary_core::{ApiaryError, Env, ManualClock};
use apiary_runtime::ApiaryNode;

fn batch(ids: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![("id", Arc::new(Int64Array::from(ids)) as ArrayRef)]).unwrap()
}

#[tokio::test]
async fn a_node_that_booted_without_network_time_ingests_but_does_not_commit_until_it_has_it() {
    // A Pi that booted with no idea of the time: its clock reads 1970.
    let epoch = chrono::Utc.timestamp_opt(5, 0).unwrap();
    let clock = Arc::new(ManualClock::new(epoch));
    let env = Env::new(clock.clone(), 1);

    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = NodeConfig::detect("local://x");
    config.storage_uri = format!("local://{}", tmp.path().join("site").display());
    config.cache_dir = tmp.path().join("cache");
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.deposit_interval = Duration::from_secs(3600);
    config.crop_max_bytes = u64::MAX;
    let node = ApiaryNode::start_with_env(config, env).await.unwrap();
    node.registry.create_hive("h").await.unwrap();
    node.registry.create_box("h", "b").await.unwrap();
    node.registry
        .create_frame("h", "b", "f", serde_json::json!({"id": "int64"}), vec![])
        .await
        .unwrap();

    // Ingest needs no wall time: the crop is ordered by segment number.
    node.ingest("h", "b", "f", &batch(vec![1, 2, 3]))
        .await
        .unwrap();
    let out = node.sql("SELECT count(id) FROM h.b.f").await.unwrap();
    assert_eq!(
        out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3,
        "the rows are queryable from the crop"
    );

    // Every way of committing is refused, and says why.
    for outcome in [
        node.flush_crop().await.err(),
        node.write_to_frame("h", "b", "f", &batch(vec![9]))
            .await
            .err(),
        node.overwrite_frame("h", "b", "f", &batch(vec![9]))
            .await
            .err(),
        node.cap_frames().await.err(),
    ] {
        let err = outcome.expect("a commit with a clock at 1970 is refused");
        assert!(matches!(err, ApiaryError::Clock { .. }), "{err}");
        assert!(err.to_string().contains("1970"), "{err}");
    }
    assert!(
        !tmp.path().join("site/h/b/f/_delta_log").exists()
            || std::fs::read_dir(tmp.path().join("site/h/b/f/_delta_log"))
                .unwrap()
                .count()
                <= 1,
        "nothing was committed to the log"
    );

    // The clock is set (network time arrives). The crop ships.
    clock.advance(Duration::from_secs(60 * 365 * 86_400));
    let report = node.flush_crop().await.unwrap();
    assert_eq!(report.rows, 3);
    let out = node
        .sql("SELECT count(id) FROM h.b.f WHERE _stage = 'comb'")
        .await
        .unwrap();
    assert_eq!(
        out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}
