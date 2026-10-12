//! Standing queries: rollups that run over every batch ingested for a Frame and
//! ship their partial aggregates into another Frame.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use apiary_core::config::NodeConfig;
use apiary_runtime::{ApiaryNode, StandingQuery};

async fn start(dir: &std::path::Path) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", dir.display());
    config.cores = 2;
    config.memory_per_bee = 128 * 1024 * 1024;
    config.cache_dir = dir.join("cache");
    config.deposit_interval = Duration::from_secs(3600);
    ApiaryNode::start(config).await.expect("the Node starts")
}

fn readings(region: &str, n: usize, temp: f64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec![region; n])) as ArrayRef,
        ),
        (
            "temp",
            Arc::new(Float64Array::from(vec![temp; n])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn lines(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|c| array_value_to_string(c, row).unwrap())
                .collect();
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

async fn make_frames(node: &ApiaryNode) {
    node.registry.create_hive("farm").await.unwrap();
    node.registry.create_box("farm", "field").await.unwrap();
    node.registry
        .create_frame(
            "farm",
            "field",
            "sensors",
            serde_json::json!({"region": "string", "temp": "float64"}),
            vec![],
        )
        .await
        .unwrap();
    node.registry
        .create_frame(
            "farm",
            "field",
            "rollups",
            serde_json::json!({"region": "string", "n": "int64", "temp_sum": "float64"}),
            vec![],
        )
        .await
        .unwrap();
}

fn by_region() -> StandingQuery {
    StandingQuery {
        name: "by_region".into(),
        hive: "farm".into(),
        box_name: "field".into(),
        source: "sensors".into(),
        target: "rollups".into(),
        sql: "SELECT region, count(*) AS n, sum(temp) AS temp_sum FROM batch GROUP BY region"
            .into(),
    }
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..400 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollup_runs_over_each_batch_and_its_partials_combine_when_queried() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path()).await;
    make_frames(&node).await;
    node.register_standing(by_region()).await.unwrap();

    for (region, n, temp) in [
        ("north", 100, 10.0),
        ("south", 50, 20.0),
        ("north", 300, 10.0),
        ("south", 150, 20.0),
    ] {
        node.ingest("farm", "field", "sensors", &readings(region, n, temp))
            .await
            .unwrap();
    }
    eventually("the rollups to run", || node.standing_counts().ran == 4).await;
    let counts = node.standing_counts();
    assert_eq!(
        (counts.queued, counts.failed, counts.skipped, counts.rows),
        (4, 0, 0, 4)
    );

    // Four batches gave four partial rows; combined they are the final answer.
    let partials = node
        .sql("SELECT count(*) FROM farm.field.rollups")
        .await
        .unwrap();
    assert_eq!(lines(&partials), vec!["4"]);
    let rolled = lines(
        &node
            .sql("SELECT region, sum(n), sum(temp_sum) FROM farm.field.rollups GROUP BY region")
            .await
            .unwrap(),
    );
    let direct = lines(
        &node
            .sql("SELECT region, count(*), sum(temp) FROM farm.field.sensors GROUP BY region")
            .await
            .unwrap(),
    );
    assert_eq!(rolled, direct);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standing_query_is_checked_when_registered_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let node = start(dir.path()).await;
        make_frames(&node).await;

        let mut bad = by_region();
        bad.sql = "SELECT nonsense FROM batch".into();
        assert!(
            node.register_standing(bad).await.is_err(),
            "it does not plan"
        );
        let mut missing = by_region();
        missing.target = "nowhere".into();
        assert!(node.register_standing(missing).await.is_err());
        let mut circular = by_region();
        circular.target = "sensors".into();
        assert!(node.register_standing(circular).await.is_err());
        assert!(node.standing_queries().is_empty());

        node.register_standing(by_region()).await.unwrap();
        node.shutdown().await;
    }

    // A new Node on the same comb has it, and runs it.
    let node = start(dir.path()).await;
    assert_eq!(node.standing_queries(), vec![by_region()]);
    node.ingest("farm", "field", "sensors", &readings("east", 10, 5.0))
        .await
        .unwrap();
    eventually("the rollup to run", || node.standing_counts().ran == 1).await;

    node.remove_standing("farm", "field", "by_region")
        .await
        .unwrap();
    node.ingest("farm", "field", "sensors", &readings("east", 10, 5.0))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        node.standing_counts().queued,
        1,
        "removed, so not queued again"
    );
    node.shutdown().await;
}
