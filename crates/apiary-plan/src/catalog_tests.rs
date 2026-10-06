//! Tests of the query catalogue and session against real Frame tables.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use apiary_comb::local::LocalBackend;
use apiary_comb::{CellState, Comb};
use apiary_core::registry_manager::RegistryManager;
use apiary_core::{ApiaryError, FrameSchema, NodeId, StorageBackend};

use crate::{ApiaryQueryContext, QueryOptions};

struct Env {
    comb: Arc<Comb>,
    registry: Arc<RegistryManager>,
    _dir: TempDir,
}

async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(dir.path().to_path_buf()).await.unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(backend);
    let registry = Arc::new(RegistryManager::new(storage));
    registry.load_or_create().await.unwrap();
    let comb = Arc::new(Comb::from_local_path(dir.path()).unwrap());
    Env {
        comb,
        registry,
        _dir: dir,
    }
}

impl Env {
    fn context(&self) -> ApiaryQueryContext {
        ApiaryQueryContext::new(Arc::clone(&self.comb), Arc::clone(&self.registry))
    }

    fn context_with(&self, options: QueryOptions) -> ApiaryQueryContext {
        ApiaryQueryContext::with_options(
            Arc::clone(&self.comb),
            Arc::clone(&self.registry),
            NodeId::from("test"),
            options,
        )
        .unwrap()
    }

    /// Register a frame and, if given, write a batch into it.
    async fn frame(
        &self,
        hive: &str,
        box_name: &str,
        name: &str,
        schema: serde_json::Value,
        data: Option<RecordBatch>,
    ) {
        // Hives and boxes may already exist
        let _ = self.registry.create_hive(hive).await;
        let _ = self.registry.create_box(hive, box_name).await;
        self.registry
            .create_frame(hive, box_name, name, schema.clone(), vec![])
            .await
            .unwrap();
        if let Some(batch) = data {
            let frame_schema = FrameSchema::from_json_value(&schema).unwrap();
            let table = self
                .comb
                .create_frame_table(hive, box_name, name, &frame_schema, &[])
                .await
                .unwrap();
            self.comb
                .append(&table, &batch, 64 * 1024 * 1024, CellState::Nectar)
                .await
                .unwrap();
        }
    }
}

fn sensors() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec!["north", "south", "north"])) as ArrayRef,
        ),
        (
            "temp",
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn regions() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec!["north", "south"])) as ArrayRef,
        ),
        ("code", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
    ])
    .unwrap()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn first_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn standard_env() -> Env {
    let env = env().await;
    env.frame(
        "farm",
        "field",
        "sensors",
        serde_json::json!({"region": "string", "temp": "float64"}),
        Some(sensors()),
    )
    .await;
    env
}

#[tokio::test]
async fn three_part_names_resolve() {
    let env = standard_env().await;
    let out = env
        .context()
        .sql("SELECT count(*) FROM farm.field.sensors")
        .await
        .unwrap();
    assert_eq!(first_i64(&out), 3);
}

#[tokio::test]
async fn shorter_names_resolve_after_use() {
    let env = standard_env().await;
    let ctx = env.context();

    ctx.sql("USE HIVE farm").await.unwrap();
    let two = ctx.sql("SELECT count(*) FROM field.sensors").await.unwrap();
    assert_eq!(first_i64(&two), 3);

    ctx.sql("USE BOX field").await.unwrap();
    let one = ctx.sql("SELECT count(*) FROM sensors").await.unwrap();
    assert_eq!(first_i64(&one), 3);
}

#[tokio::test]
async fn unqualified_names_without_use_explain_what_to_do() {
    let env = standard_env().await;
    let err = env
        .context()
        .sql("SELECT count(*) FROM sensors")
        .await
        .unwrap_err();
    assert!(matches!(err, ApiaryError::Resolution { .. }), "{err:?}");
    assert!(err.to_string().contains("USE HIVE"), "{err}");

    let ctx = env.context();
    ctx.sql("USE HIVE farm").await.unwrap();
    let err = ctx.sql("SELECT count(*) FROM sensors").await.unwrap_err();
    assert!(err.to_string().contains("USE BOX"), "{err}");
}

#[tokio::test]
async fn missing_hives_boxes_and_frames_are_entity_not_found() {
    let env = standard_env().await;
    let ctx = env.context();
    for (sql, kind) in [
        ("SELECT * FROM nope.field.sensors", "Hive"),
        ("SELECT * FROM farm.nope.sensors", "Box"),
        ("SELECT * FROM farm.field.nope", "Frame"),
    ] {
        match ctx.sql(sql).await.unwrap_err() {
            ApiaryError::EntityNotFound { entity_type, .. } => assert_eq!(entity_type, kind),
            other => panic!("{sql}: expected EntityNotFound, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn names_match_the_registry_case_insensitively() {
    let env = env().await;
    env.frame(
        "Production",
        "Sensors",
        "Temperature",
        serde_json::json!({"region": "string", "temp": "float64"}),
        Some(sensors()),
    )
    .await;
    let ctx = env.context();
    // DataFusion lower-cases unquoted names; the registry has capitals.
    let out = ctx
        .sql("SELECT count(*) FROM production.sensors.temperature")
        .await
        .unwrap();
    assert_eq!(first_i64(&out), 3);
    let out = ctx
        .sql("SELECT count(*) FROM Production.Sensors.Temperature")
        .await
        .unwrap();
    assert_eq!(first_i64(&out), 3);
}

#[tokio::test]
async fn a_registered_frame_that_was_never_written_is_empty() {
    let env = env().await;
    env.frame(
        "farm",
        "field",
        "empty",
        serde_json::json!({"region": "string", "temp": "float64"}),
        None,
    )
    .await;
    let out = env
        .context()
        .sql("SELECT region, temp FROM farm.field.empty")
        .await
        .unwrap();
    assert_eq!(rows(&out), 0);
}

#[tokio::test]
async fn statements_that_would_change_shared_state_are_refused() {
    let env = standard_env().await;
    let ctx = env.context();
    for sql in [
        "CREATE TABLE t AS SELECT 1",
        "COPY (SELECT 1) TO '/tmp/apiary-should-not-exist.csv'",
        "SET datafusion.execution.batch_size = 1",
        "RESET datafusion.execution.batch_size",
    ] {
        let err = ctx.sql(sql).await.unwrap_err();
        assert!(
            matches!(err, ApiaryError::Unsupported { .. }),
            "{sql}: {err:?}"
        );
    }
}

#[tokio::test]
async fn queries_run_concurrently_on_one_shared_session() {
    let env = standard_env().await;
    let ctx = Arc::new(env.context());

    let mut handles = Vec::new();
    for i in 0..16 {
        let ctx = Arc::clone(&ctx);
        handles.push(tokio::spawn(async move {
            let sql = if i % 2 == 0 {
                "SELECT count(*) FROM farm.field.sensors"
            } else {
                "SELECT count(*) FROM farm.field.sensors WHERE region = 'north'"
            };
            (i, first_i64(&ctx.sql(sql).await.unwrap()))
        }));
    }
    for handle in handles {
        let (i, n) = handle.await.unwrap();
        assert_eq!(n, if i % 2 == 0 { 3 } else { 2 }, "query {i}");
    }
}

#[tokio::test]
async fn use_state_is_shared_by_every_caller_of_the_node() {
    let env = standard_env().await;
    let ctx = Arc::new(env.context());
    ctx.sql("USE HIVE farm").await.unwrap();
    ctx.sql("USE BOX field").await.unwrap();

    // Another task sees the selection made by the first.
    let other = Arc::clone(&ctx);
    let n = tokio::spawn(async move {
        first_i64(&other.sql("SELECT count(*) FROM sensors").await.unwrap())
    })
    .await
    .unwrap();
    assert_eq!(n, 3);
}

#[tokio::test]
async fn data_written_after_the_session_started_is_visible() {
    let env = standard_env().await;
    let ctx = env.context();
    assert_eq!(
        first_i64(
            &ctx.sql("SELECT count(*) FROM farm.field.sensors")
                .await
                .unwrap()
        ),
        3
    );

    // The session is long-lived, but each query opens the table afresh.
    let table = env
        .comb
        .open_frame_table("farm", "field", "sensors")
        .await
        .unwrap()
        .unwrap();
    env.comb
        .append(&table, &sensors(), 64 * 1024 * 1024, CellState::Nectar)
        .await
        .unwrap();
    assert_eq!(
        first_i64(
            &ctx.sql("SELECT count(*) FROM farm.field.sensors")
                .await
                .unwrap()
        ),
        6
    );
}

// ---- join policy against real Frame tables ------------------------------

const FRAME_JOIN: &str = "SELECT s.region, r.code, s.temp FROM farm.field.sensors s \
                          JOIN farm.field.regions r ON s.region = r.region";

async fn join_env() -> Env {
    let env = standard_env().await;
    env.frame(
        "farm",
        "field",
        "regions",
        serde_json::json!({"region": "string", "code": "int64"}),
        Some(regions()),
    )
    .await;
    env
}

async fn plan_of(ctx: &ApiaryQueryContext, sql: &str) -> String {
    let out = ctx.sql(&format!("EXPLAIN {sql}")).await.unwrap();
    let mut text = String::new();
    for batch in &out {
        for column in 0..batch.num_columns() {
            if let Some(strings) = batch.column(column).as_any().downcast_ref::<StringArray>() {
                for row in 0..strings.len() {
                    text.push_str(strings.value(row));
                    text.push('\n');
                }
            }
        }
    }
    text
}

#[tokio::test]
async fn delta_tables_report_sizes_so_small_joins_stay_hash_joins() {
    let env = join_env().await;
    let ctx = env.context();
    let plan = plan_of(&ctx, FRAME_JOIN).await;
    // If Delta tables gave no statistics, every join would count as unknown
    // and be replaced. The physical plan is the last one in the EXPLAIN.
    let physical = plan.split("physical_plan").last().unwrap();
    assert!(physical.contains("HashJoinExec"), "{plan}");
    assert!(!physical.contains("SortMergeJoinExec"), "{plan}");
}

#[tokio::test]
async fn a_small_bee_turns_frame_joins_into_sort_merge_joins_with_the_same_answer() {
    let env = join_env().await;
    let roomy = env.context();
    let cramped = env.context_with(QueryOptions {
        memory_per_bee: 64,
        ..QueryOptions::default()
    });

    let plan = plan_of(&cramped, FRAME_JOIN).await;
    let physical = plan.split("physical_plan").last().unwrap();
    assert!(physical.contains("SortMergeJoinExec"), "{plan}");

    let sql = format!("SELECT * FROM ({FRAME_JOIN}) ORDER BY temp");
    let a = roomy.sql(&sql).await.unwrap();
    let b = cramped.sql(&sql).await.unwrap();
    assert_eq!(rows(&a), 3);
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&a)
            .unwrap()
            .to_string(),
        arrow::util::pretty::pretty_format_batches(&b)
            .unwrap()
            .to_string()
    );
}

#[tokio::test]
async fn results_use_plain_string_types() {
    let env = standard_env().await;
    let out = env
        .context()
        .sql("SELECT region FROM farm.field.sensors")
        .await
        .unwrap();
    assert_eq!(
        *out[0].column(0).data_type(),
        arrow::datatypes::DataType::Utf8
    );
}

#[tokio::test]
async fn node_options_create_the_spill_directory() {
    let env = standard_env().await;
    let spill = env._dir.path().join("spill");
    let _ctx = env.context_with(QueryOptions {
        memory_pool_bytes: 64 * 1024 * 1024,
        spill_dir: Some(spill.clone()),
        ..QueryOptions::default()
    });
    assert!(spill.is_dir());
}
