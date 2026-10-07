//! Shared fixtures for the entrance's integration tests.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;

use apiary_core::config::NodeConfig;
use apiary_entrance::{Guard, SetAside};
use apiary_runtime::ApiaryNode;

/// A running Node with a `farm.field.readings (id int64, temp float64)` Frame,
/// and a Guard in front of it.
pub struct Fixture {
    pub _tmp: tempfile::TempDir,
    pub node: Arc<ApiaryNode>,
    pub guard: Guard,
}

pub async fn fixture() -> Fixture {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", tmp.path().join("site").display());
    config.cores = 2;
    config.memory_per_bee = 64 * 1024 * 1024;
    config.cache_dir = tmp.path().join("cache");
    config.deposit_interval = Duration::from_secs(3600);
    config.crop_max_bytes = u64::MAX;
    let aside = SetAside::open(config.set_aside_dir()).unwrap();
    let node = Arc::new(ApiaryNode::start(config).await.unwrap());
    node.registry.create_hive("farm").await.unwrap();
    node.registry.create_box("farm", "field").await.unwrap();
    node.registry
        .create_frame(
            "farm",
            "field",
            "readings",
            serde_json::json!({"id": "int64", "temp": "float64"}),
            vec![],
        )
        .await
        .unwrap();
    let guard = Guard::new(Arc::clone(&node), aside);
    Fixture {
        _tmp: tmp,
        node,
        guard,
    }
}

/// Two rows that fit the Frame.
pub fn good() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "temp",
            Arc::new(Float64Array::from(vec![20.5, 21.0])) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// A batch with a column the Frame does not have.
pub fn with_unknown_column() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![3])) as ArrayRef),
        (
            "humidity",
            Arc::new(Float64Array::from(vec![0.4])) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// How many rows the Frame holds, crop and comb together.
pub async fn rows(node: &ApiaryNode) -> i64 {
    let out = node
        .sql("SELECT count(id) FROM farm.field.readings")
        .await
        .unwrap();
    out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
