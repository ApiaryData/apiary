//! A query cut into Patches and run through an exchange gives the rows it gives
//! when it runs whole.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use apiary_comb::local::LocalBackend;
use apiary_core::StorageBackend;

use crate::StagedOptions;
use crate::exchange::{Exchange, StoreExchange};
use crate::staged_tests::{Env, env};
use crate::stages::{self, Handoff};

fn rows(region: &str, n: usize, offset: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec![region; n])) as ArrayRef,
        ),
        (
            "temp",
            Arc::new(Float64Array::from(
                (0..n as i64)
                    .map(|i| (offset + i % 50) as f64)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// A Frame with several deposited files and some rows still in the crop.
async fn farm() -> Env {
    let env = env().await;
    for (region, offset) in [("north", 0), ("south", 10), ("east", 20)] {
        env.ingest(&rows(region, 4000, offset));
        env.deposit(true, true).await;
    }
    env.ingest(&rows("west", 1500, 30));
    env.ingest(&rows("north", 700, 5));
    env
}

async fn exchange() -> (Arc<dyn Exchange>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn StorageBackend> =
        Arc::new(LocalBackend::new(dir.path().to_path_buf()).await.unwrap());
    (Arc::new(StoreExchange::new(store)), dir)
}

/// Every row as text, sorted: results compared without regard to order.
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

const QUERIES: [&str; 12] = [
    "SELECT count(*), sum(temp), min(temp), max(temp) FROM farm.field.sensors",
    "SELECT region, count(*), sum(temp) FROM farm.field.sensors GROUP BY region",
    "SELECT region, avg(temp) FROM farm.field.sensors WHERE temp > 15 GROUP BY region HAVING count(*) > 10",
    "SELECT _stage, count(*) FROM farm.field.sensors GROUP BY _stage",
    "SELECT DISTINCT region FROM farm.field.sensors",
    "SELECT region, temp FROM farm.field.sensors WHERE region = 'east' AND temp < 25",
    "SELECT region, temp FROM farm.field.sensors ORDER BY temp DESC, region LIMIT 20",
    "SELECT a.region, b.region, count(*) FROM farm.field.sensors a JOIN farm.field.sensors b ON a.temp = b.temp WHERE a.temp < 3 GROUP BY a.region, b.region",
    "SELECT region, temp, row_number() OVER (PARTITION BY region ORDER BY temp) AS r FROM farm.field.sensors WHERE temp < 2",
    "SELECT region FROM farm.field.sensors WHERE temp = 0 UNION ALL SELECT region FROM farm.field.sensors WHERE temp = 49",
    "SELECT count(*) FROM (SELECT region, sum(temp) s FROM farm.field.sensors GROUP BY region) WHERE s > 100000",
    "SELECT region FROM farm.field.sensors WHERE temp IN (SELECT max(temp) FROM farm.field.sensors)",
];

#[tokio::test]
async fn patches_give_the_rows_the_whole_query_gives() {
    let env = farm().await;
    let ctx = env.context();
    for sql in QUERIES {
        let whole = lines(&ctx.sql(sql).await.unwrap());
        for round_trip in [false, true] {
            let (exchange, _dir) = exchange().await;
            let options = StagedOptions {
                round_trip,
                ..StagedOptions::default()
            };
            let staged = lines(&ctx.sql_staged(sql, exchange, options).await.unwrap());
            assert_eq!(staged, whole, "{sql} (round trip: {round_trip})");
        }
    }
}

#[tokio::test]
async fn a_query_is_cut_at_its_repartitions_and_merges() {
    let env = farm().await;
    let ctx = env.context();
    let planned = ctx
        .plan_sql(
            "SELECT region, sum(temp) FROM farm.field.sensors GROUP BY region",
            None,
        )
        .await
        .unwrap();
    let staged = stages::cut(planned.physical).unwrap();

    // Partial aggregation, then a hash split, then the final aggregate and its merge.
    let handoffs: Vec<Handoff> = staged.stages.iter().map(|s| s.handoff).collect();
    assert!(
        matches!(handoffs[0], Handoff::Split(n) if n > 1),
        "{handoffs:?}"
    );
    assert_eq!(handoffs.last(), Some(&Handoff::Result));
    assert!(staged.stages.len() >= 3, "{handoffs:?}");

    // Stages come in an order that runs each after the stages it reads.
    for stage in &staged.stages {
        assert!(stage.inputs().iter().all(|i| *i < stage.id), "{stage:?}");
    }
    // The crop's rows are held in this Node's memory, so a stage reading them is pinned.
    assert!(staged.stages.iter().any(|s| s.is_pinned()));
    assert_eq!(staged.held.len(), 1);
    // Every stage's boundary survives the codec; only pinned ones need this Node.
    let record = crate::codec::encode_plan("q", "node", &staged).unwrap();
    let (_, back) = crate::codec::decode_plan(&record, &planned.task_ctx).unwrap();
    assert_eq!(back.stages.len(), staged.stages.len());
    for (a, b) in staged.stages.iter().zip(&back.stages) {
        assert_eq!(a.patches(), b.patches());
        assert_eq!(a.handoff, b.handoff);
    }
}

#[tokio::test]
async fn a_patch_is_completed_once_whoever_finishes_it() {
    use crate::exchange::{Completed, PatchKey};
    use crate::patch::{PatchEnv, run_patch};

    let env = farm().await;
    let ctx = env.context();
    let planned = ctx
        .plan_sql(
            "SELECT region, sum(temp) FROM farm.field.sensors GROUP BY region",
            None,
        )
        .await
        .unwrap();
    let mut staged = stages::cut(planned.physical).unwrap();
    let held = std::mem::take(&mut staged.held);
    let (exchange, _dir) = exchange().await;
    let patch_env = PatchEnv::new(
        "q1",
        "node",
        Arc::clone(&exchange),
        ctx.scan_source(None).unwrap(),
        held,
    );
    let task = patch_env.task_context(None).unwrap();

    // Two attempts at the first Patch of the first stage: the first to finish wins.
    let stage = &staged.stages[0];
    let first = run_patch(stage, 0, &patch_env, Arc::clone(&task), "a")
        .await
        .unwrap();
    let second = run_patch(stage, 0, &patch_env, Arc::clone(&task), "b")
        .await
        .unwrap();
    assert_eq!(first.completed, Completed::Won);
    let Completed::Lost(winner) = second.completed else {
        panic!("a second attempt must lose");
    };
    assert_eq!(winner.attempt, "a");
    assert_eq!(first.rows, second.rows);

    // The record names the winner, and the loser left no files behind.
    let key = PatchKey::new("q1", stage.id, 0);
    let done = exchange.completion(&key).await.unwrap().unwrap();
    assert_eq!(done.attempt, "a");
    assert!(exchange.get(&key, &done, 0, 0).await.is_ok());
    assert!(
        exchange
            .get(
                &key,
                &crate::exchange::Completion {
                    attempt: "b".into(),
                    ..done.clone()
                },
                0,
                0
            )
            .await
            .is_err(),
        "the losing attempt's output was removed"
    );
}

#[tokio::test]
async fn a_stage_that_runs_before_its_inputs_say_so() {
    use crate::exchange::PatchKey;
    use crate::patch::{PatchEnv, run_patch};

    let env = farm().await;
    let ctx = env.context();
    let planned = ctx
        .plan_sql(
            "SELECT region, sum(temp) FROM farm.field.sensors GROUP BY region",
            None,
        )
        .await
        .unwrap();
    let mut staged = stages::cut(planned.physical).unwrap();
    let held = std::mem::take(&mut staged.held);
    let (exchange, _dir) = exchange().await;
    let patch_env = PatchEnv::new("q2", "node", exchange, ctx.scan_source(None).unwrap(), held);
    let task = patch_env.task_context(None).unwrap();

    // Stage 1 reads stage 0, which has not run.
    let err = run_patch(&staged.stages[1], 0, &patch_env, task, "x")
        .await
        .unwrap_err();
    assert!(crate::patch::is_input_not_ready(&err), "{err}");
    let _ = PatchKey::new("q2", 0, 0);
    let _ = Int64Array::from(vec![1]);
}

/// Joins planned to fit a small Bee are partitioned or sort-merge, never a build
/// side collected whole: both are cut into Patches and must still agree.
#[tokio::test]
async fn joins_planned_for_a_small_bee_agree_when_run_as_patches() {
    use apiary_core::NodeId;
    use datafusion::physical_plan::displayable;

    use crate::{ApiaryQueryContext, QueryOptions};

    let env = farm().await;
    let ctx = ApiaryQueryContext::with_options(
        Arc::clone(&env.comb),
        Arc::clone(&env.registry),
        NodeId::from("small"),
        QueryOptions {
            crop: Some(Arc::clone(&env.crop)),
            memory_per_bee: 64 * 1024,
            target_partitions: 4,
            ..QueryOptions::default()
        },
    )
    .unwrap();

    let sql = "SELECT a.region, count(*), sum(b.temp) \
               FROM farm.field.sensors a JOIN farm.field.sensors b ON a.temp = b.temp \
               WHERE a.temp < 5 GROUP BY a.region";
    let planned = ctx.plan_sql(sql, None).await.unwrap();
    let shape = displayable(planned.physical.as_ref())
        .indent(false)
        .to_string();
    assert!(
        !shape.contains("CollectLeft"),
        "the policy should not collect a build side whole:\n{shape}"
    );
    let staged = stages::cut(planned.physical).unwrap();
    assert!(staged.stages.len() >= 4, "{shape}");

    let whole = lines(&ctx.sql(sql).await.unwrap());
    let (exchange, _dir) = exchange().await;
    let patched = lines(
        &ctx.sql_staged(sql, exchange, StagedOptions::default())
            .await
            .unwrap(),
    );
    assert_eq!(patched, whole, "{shape}");
}

/// A filter that prunes away every file leaves a scan with nothing to read; it is
/// still a Patch that any Node can run, and it gives no rows.
#[tokio::test]
async fn a_scan_pruned_to_nothing_is_still_a_patch() {
    let env = farm().await;
    let ctx = env.context();
    for sql in [
        "SELECT region, temp FROM farm.field.sensors WHERE temp > 1000",
        "SELECT count(*), sum(temp) FROM farm.field.sensors WHERE temp > 1000",
    ] {
        let whole = lines(&ctx.sql(sql).await.unwrap());
        let (exchange, _dir) = exchange().await;
        let staged = lines(
            &ctx.sql_staged(sql, exchange, StagedOptions::default())
                .await
                .unwrap(),
        );
        assert_eq!(staged, whole, "{sql}");
    }
}

// ---------------------------------------------------------------------------
// Model functions
// ---------------------------------------------------------------------------

async fn with_model(ctx: &crate::ApiaryQueryContext) {
    let model = serde_json::json!({"weights": [2.0, 0.5], "bias": 1.0});
    ctx.models()
        .register("scorer", "linear", model.to_string().into())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_model_function_gives_the_same_rows_whole_and_as_patches() {
    let env = farm().await;
    let ctx = env.context();
    with_model(&ctx).await;
    for sql in [
        "SELECT region, sum(predict('scorer', temp, temp * 2.0)) FROM farm.field.sensors GROUP BY region",
        "SELECT region, temp, predict('scorer', temp, 4.0) AS score FROM farm.field.sensors WHERE temp < 3 ORDER BY temp, region LIMIT 12",
    ] {
        let whole = lines(&ctx.sql(sql).await.unwrap());
        assert!(!whole.is_empty());
        let (exchange, _dir) = exchange().await;
        let staged = lines(
            &ctx.sql_staged(sql, exchange, StagedOptions::default())
                .await
                .unwrap(),
        );
        assert_eq!(staged, whole, "{sql}");
    }
    // 2*temp + 0.5*(temp*2) + 1, summed.
    let one = lines(&ctx.sql("SELECT predict('scorer', 3.0, 4.0)").await.unwrap());
    assert_eq!(one, vec!["9.0"]);
}

#[tokio::test]
async fn a_model_function_refuses_what_it_cannot_run() {
    let env = farm().await;
    let ctx = env.context();
    with_model(&ctx).await;
    let missing = ctx
        .sql("SELECT predict('nowhere', temp, temp) FROM farm.field.sensors")
        .await;
    assert!(missing.is_err());
    let arity = ctx
        .sql("SELECT predict('scorer', temp) FROM farm.field.sensors")
        .await;
    assert!(arity.is_err(), "a model of two features given one");
    let named = ctx
        .sql("SELECT predict(region, temp, temp) FROM farm.field.sensors")
        .await;
    assert!(named.is_err(), "the model must be named by a literal");
    // A file that will not load is refused when it is registered.
    assert!(
        ctx.models()
            .register("bad", "linear", "not json".into())
            .await
            .is_err()
    );
    assert!(
        ctx.models().register("x", "onnx", "".into()).await.is_err(),
        "no backend for onnx in this build"
    );
}

#[tokio::test]
async fn a_new_version_of_a_model_is_picked_up_by_the_next_query() {
    let env = farm().await;
    let ctx = env.context();
    with_model(&ctx).await;
    let sql = "SELECT predict('scorer', 1.0, 1.0)";
    assert_eq!(lines(&ctx.sql(sql).await.unwrap()), vec!["3.5"]);
    let v2 = serde_json::json!({"weights": [10.0, 0.0], "bias": 0.0});
    let version = ctx
        .models()
        .register("scorer", "linear", v2.to_string().into())
        .await
        .unwrap();
    assert_eq!(version, 2);
    assert_eq!(lines(&ctx.sql(sql).await.unwrap()), vec!["10.0"]);
    assert_eq!(ctx.models().counts().fetched, 2);
}

// ---------------------------------------------------------------------------
// More data than a Bee may hold
// ---------------------------------------------------------------------------

/// Joins over more data than one Bee may hold, run as Patches under a share of memory
/// far smaller than the data: every Patch writes its output as it goes and every join
/// is planned to fit, so nothing fails for want of memory, and the answer is the
/// answer the query gives with all the memory it likes.
#[tokio::test]
async fn joins_over_more_data_than_a_bee_may_hold_complete_as_patches() {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

    use crate::{ApiaryQueryContext, QueryOptions};
    use apiary_core::{FieldDef, FrameSchema, NodeId};

    let env = env().await;
    env.registry.create_box("farm", "dim").await.unwrap();
    for (frame, schema) in [
        (
            "events",
            serde_json::json!({"id": "int64", "grp": "int64", "v": "float64"}),
        ),
        (
            "groups",
            serde_json::json!({"grp": "int64", "name": "string"}),
        ),
    ] {
        env.registry
            .create_frame("farm", "dim", frame, schema, vec![])
            .await
            .unwrap();
    }
    let field = |name: &str, t: &str| FieldDef {
        name: name.into(),
        data_type: t.into(),
        nullable: false,
    };
    let events = env
        .comb
        .create_frame_table(
            "farm",
            "dim",
            "events",
            &FrameSchema {
                fields: vec![
                    field("id", "int64"),
                    field("grp", "int64"),
                    field("v", "float64"),
                ],
            },
            &[],
        )
        .await
        .unwrap();
    let groups = env
        .comb
        .create_frame_table(
            "farm",
            "dim",
            "groups",
            &FrameSchema {
                fields: vec![field("grp", "int64"), field("name", "string")],
            },
            &[],
        )
        .await
        .unwrap();
    const ROWS: i64 = 1_200_000;
    for part in 0..4 {
        let ids: Vec<i64> = (part * ROWS / 4..(part + 1) * ROWS / 4).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("grp", DataType::Int64, false),
                Field::new("v", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(
                    ids.iter().map(|i| i % 1000).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    ids.iter().map(|i| *i as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        env.comb
            .append(
                &events,
                &batch,
                64 * 1024 * 1024,
                apiary_comb::CellState::Nectar,
            )
            .await
            .unwrap();
    }
    let names: Vec<String> = (0..1000).map(|g| format!("group-{g}")).collect();
    let dim = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from((0..1000).collect::<Vec<_>>())),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();
    env.comb
        .append(
            &groups,
            &dim,
            64 * 1024 * 1024,
            apiary_comb::CellState::Nectar,
        )
        .await
        .unwrap();

    // A Node with a Bee that may hold little: joins are planned to fit it.
    let small = ApiaryQueryContext::with_options(
        Arc::clone(&env.comb),
        Arc::clone(&env.registry),
        NodeId::from("small"),
        QueryOptions {
            memory_pool_bytes: 48 * 1024 * 1024,
            memory_per_bee: 12 * 1024 * 1024,
            target_partitions: 4,
            ..QueryOptions::default()
        },
    )
    .unwrap();
    let big = ApiaryQueryContext::with_options(
        Arc::clone(&env.comb),
        Arc::clone(&env.registry),
        NodeId::from("big"),
        QueryOptions {
            target_partitions: 4,
            ..QueryOptions::default()
        },
    )
    .unwrap();

    for sql in [
        "SELECT g.name, count(*), sum(e.v) FROM farm.dim.events e JOIN farm.dim.groups g \
         ON e.grp = g.grp GROUP BY g.name",
        "SELECT count(*), sum(a.v + b.v) FROM farm.dim.events a JOIN farm.dim.events b \
         ON a.id = b.id WHERE a.grp < 50",
    ] {
        let whole = lines(&big.sql(sql).await.unwrap());
        // Each Patch runs under 12 MB, a fraction of what the data would take in memory.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(12 * 1024 * 1024));
        let (exchange, _dir) = exchange().await;
        let patched = small
            .sql_staged(
                sql,
                exchange,
                StagedOptions {
                    pool: Some(pool),
                    // One Bee, one Patch at a time, under its own share.
                    concurrency: 1,
                    ..StagedOptions::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(lines(&patched), whole, "{sql}");
    }
}
