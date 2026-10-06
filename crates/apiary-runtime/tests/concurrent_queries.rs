//! Integration test: a Node runs queries concurrently on one shared session.
//!
//! Until phase 1b every query took a lock on the query context, so a Node ran
//! one SQL statement at a time. The session is now shared, with one memory
//! pool and spill directory for the whole Node.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use apiary_core::config::NodeConfig;
use apiary_runtime::ApiaryNode;

async fn start_test_node(tmpdir: &std::path::Path) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmpdir.display());
    config.cores = 4;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = tmpdir.join("cache");
    ApiaryNode::start(config).await.expect("Node should start")
}

async fn write_readings(node: &ApiaryNode, rows: i64) {
    node.registry.create_hive("farm").await.unwrap();
    node.registry.create_box("farm", "field").await.unwrap();
    node.registry
        .create_frame(
            "farm",
            "field",
            "readings",
            serde_json::json!({"region": "string", "n": "int64"}),
            vec![],
        )
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("n", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| if i % 2 == 0 { "north" } else { "south" }),
            )),
            Arc::new(Int64Array::from_iter_values(0..rows)),
        ],
    )
    .unwrap();
    node.write_to_frame("farm", "field", "readings", &batch)
        .await
        .unwrap();
}

fn first_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_many_queries_run_concurrently() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = Arc::new(start_test_node(tmp.path()).await);
    write_readings(&node, 1_000).await;

    let mut handles = Vec::new();
    for i in 0..16 {
        let node = Arc::clone(&node);
        handles.push(tokio::spawn(async move {
            let (sql, expected) = match i % 3 {
                0 => ("SELECT count(*) FROM farm.field.readings", 1_000),
                1 => (
                    "SELECT count(*) FROM farm.field.readings WHERE region = 'north'",
                    500,
                ),
                _ => ("SELECT sum(n) FROM farm.field.readings", 499_500),
            };
            (first_i64(&node.sql(sql).await.unwrap()), expected)
        }));
    }
    for handle in handles {
        let (got, expected) = handle.await.unwrap();
        assert_eq!(got, expected);
    }

    node.shutdown().await;
}

#[tokio::test]
async fn test_node_has_a_spill_directory_under_its_cache() {
    let tmp = tempfile::TempDir::new().unwrap();
    let node = start_test_node(tmp.path()).await;
    assert!(
        tmp.path().join("cache").join("spill").is_dir(),
        "queries spill under the Node's cache directory"
    );
    node.shutdown().await;
}
