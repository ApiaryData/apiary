//! The ONNX backend, against a real ONNX Runtime and a real model.
//!
//! The runtime is not part of the build (the `onnx` feature loads it from a shared
//! library), so this test runs only where it is given one: set `ORT_DYLIB_PATH` to
//! the library and `APIARY_ONNX_MODEL` to a model with a `[rows, 2]` float input
//! and a `[rows, 1]` output computing `2*a + 0.5*b + 1`. `scripts/onnx-test.sh`
//! builds both in a container and runs it.

#![cfg(feature = "onnx")]

use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use apiary_comb::local::LocalBackend;
use apiary_core::StorageBackend;
use apiary_plan::models::{Models, predict_udf};

#[tokio::test]
async fn an_onnx_model_runs_through_predict() {
    let (Ok(_), Ok(path)) = (
        std::env::var("ORT_DYLIB_PATH"),
        std::env::var("APIARY_ONNX_MODEL"),
    ) else {
        eprintln!("skipped: ORT_DYLIB_PATH and APIARY_ONNX_MODEL are not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn StorageBackend> =
        Arc::new(LocalBackend::new(dir.path().to_path_buf()).await.unwrap());
    let models = Models::new(Arc::clone(&store));
    let file = std::fs::read(path).unwrap();
    assert_eq!(
        models.register("lin", "onnx", file.into()).await.unwrap(),
        1
    );
    models.ensure("lin").await.unwrap();

    let ctx = SessionContext::new();
    ctx.register_udf(predict_udf(Arc::clone(&models)));
    let batch = RecordBatch::try_from_iter(vec![
        (
            "name",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
        ),
        (
            "x",
            Arc::new(Float64Array::from(vec![Some(1.0), Some(3.0), None])) as _,
        ),
        ("y", Arc::new(Float64Array::from(vec![1.0, 4.0, 9.0])) as _),
    ])
    .unwrap();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let out = ctx
        .sql("SELECT name, predict('lin', x, y) AS p FROM t ORDER BY name")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let p = out[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // 2*1 + 0.5*1 + 1, 2*3 + 0.5*4 + 1, and a missing feature gives no prediction.
    assert!((p.value(0) - 3.5).abs() < 1e-5);
    assert!((p.value(1) - 9.0).abs() < 1e-5);
    assert!(p.is_null(2));
    assert_eq!(models.counts().rows, 3);
}
