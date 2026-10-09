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
    assert!(exchange.get(&key, &done, 0).await.is_ok());
    assert!(
        exchange
            .get(
                &key,
                &crate::exchange::Completion {
                    attempt: "b".into(),
                    ..done.clone()
                },
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
