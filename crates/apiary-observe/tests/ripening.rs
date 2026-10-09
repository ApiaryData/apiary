//! Phase 4 gate, the comb half: ripening yields to users, and a streaming Frame's
//! commit rate stays within its budget.
//!
//! Both run the real Node, with its real Bees choosing roles, on a simulated
//! bucket and a virtual clock.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use apiary_core::rng::SeededRng;
use apiary_observe::{Sim, Trace};
use apiary_runtime::ApiaryNode;
use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;

fn ints(values: impl IntoIterator<Item = i64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "n",
        Arc::new(Int64Array::from_iter_values(values)) as ArrayRef,
    )])
    .unwrap()
}

async fn first_int(node: &ApiaryNode, sql: &str) -> i64 {
    node.sql(sql).await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn node_on(sim: &Sim, store: &str, dir: &std::path::Path) -> ApiaryNode {
    let mut config = sim.node_config("ripener", store, &dir.join("cache"));
    config.deposit_interval = Duration::from_secs(3600);
    config.crop_max_bytes = u64::MAX;
    ApiaryNode::start_with_env(config, sim.env()).await.unwrap()
}

async fn make_frame(node: &ApiaryNode) {
    node.registry.create_hive("farm").await.unwrap();
    node.registry.create_box("farm", "field").await.unwrap();
    node.registry
        .create_frame(
            "farm",
            "field",
            "readings",
            serde_json::json!({"n": "int64"}),
            vec![],
        )
        .await
        .unwrap();
}

/// What a race between capping and a user's overwrite came to.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// The overwrite committed: the Frame holds exactly the user's rows. `yielded`
    /// is whether capping gave way to it (its commit would have conflicted).
    UserWon { cap_committed: bool },
    /// The overwrite was refused as a conflict: the Frame still holds exactly the
    /// rows it had, capped or not.
    UserRefused { capped: bool },
}

/// Four appends make four nectar Cells. Capping them and a user's overwrite then
/// race, the overwrite starting `delay` into the cap.
async fn cap_races_overwrite(sim: Sim, store: String, delay: Duration) -> Outcome {
    let dir = tempfile::TempDir::new().unwrap();
    let bucket = sim.store(&store);
    bucket.set_faults(|f| {
        f.latency = Duration::from_millis(5);
        f.jitter = Duration::from_millis(30);
    });
    let node = node_on(&sim, &store, dir.path()).await;
    make_frame(&node).await;
    for i in 0..4 {
        node.write_to_frame("farm", "field", "readings", &ints(i * 10..i * 10 + 10))
            .await
            .unwrap();
    }
    let before_sum = first_int(&node, "SELECT sum(n) FROM farm.field.readings").await;
    assert_eq!(before_sum, (0..40).sum::<i64>());

    let user_rows = ints(1000..1005);
    let (cap, overwrite) = tokio::join!(node.cap_frames(), async {
        sim.sleep(delay).await;
        node.overwrite_frame("farm", "field", "readings", &user_rows)
            .await
    });
    let capped = cap.map(|r| r.commits > 0).unwrap_or(false);

    let count = first_int(&node, "SELECT count(n) FROM farm.field.readings").await;
    let sum = first_int(&node, "SELECT sum(n) FROM farm.field.readings").await;
    let old = first_int(
        &node,
        "SELECT count(n) FROM farm.field.readings WHERE n < 1000",
    )
    .await;
    node.shutdown().await;

    match overwrite {
        Ok(_) => {
            assert_eq!(
                count, 5,
                "the user's overwrite left exactly the user's rows"
            );
            assert_eq!(sum, (1000..1005).sum::<i64>());
            assert_eq!(old, 0, "no capping commit brought the old rows back");
            Outcome::UserWon {
                cap_committed: capped,
            }
        }
        Err(e) => {
            assert!(
                e.to_string().contains("commit"),
                "the overwrite was refused loudly: {e}"
            );
            assert_eq!(count, 40, "a refused overwrite changed nothing");
            assert_eq!(sum, before_sum, "and capping lost no data");
            Outcome::UserRefused { capped }
        }
    }
}

#[test]
fn capping_never_overrides_a_concurrent_user_write() {
    let mut outcomes = BTreeSet::new();
    for seed in 0..60u64 {
        // The overwrite lands somewhere between the start of the cap and well
        // after its end, a different place each seed.
        let delay = Duration::from_millis(sim_delay(seed));
        let store = format!("race-{seed}");
        let run = Sim::run(seed, |sim| cap_races_overwrite(sim, store, delay));
        println!("seed {seed:2} delay {delay:?}: {:?}", run.value);
        outcomes.insert(format!("{:?}", run.value));
    }
    // The sweep reached both orders: capping first (the user is told to retry) and
    // the user first (capping yields). Either way the invariants held.
    assert!(
        outcomes.len() >= 2,
        "the seeds explored both races: {outcomes:?}"
    );
}

/// A delay of 0 to 400 ms drawn from the seed, independent of the Node's streams.
fn sim_delay(seed: u64) -> u64 {
    apiary_core::rng::StdSeededRng::from_seed(seed.wrapping_mul(7919)).next_below(400)
}

/// The times, since the run began, that commits landed in a Frame's log.
fn commit_times(trace: &Trace, frame_path: &str) -> Vec<Duration> {
    trace
        .events()
        .into_iter()
        .filter(|e| {
            e.kind == "store.put"
                && e.detail.starts_with(frame_path)
                && e.detail.contains("/_delta_log/")
                && e.detail.contains(".json Create ok")
                // The Frame's creation is a one-off, not a deposit or a cap.
                && !e.detail.contains("/00000000000000000000.json")
        })
        .map(|e| e.at)
        .collect()
}

/// The most commits in any sliding minute.
fn busiest_minute(times: &[Duration]) -> usize {
    times
        .iter()
        .map(|t| {
            times
                .iter()
                .filter(|u| **u <= *t && t.saturating_sub(**u) < Duration::from_secs(60))
                .count()
        })
        .max()
        .unwrap_or(0)
}

/// A sensor stream: a small batch every 100 ms for three minutes, with the Node's
/// deposit cadence set far faster than the budget allows.
async fn stream(sim: Sim, store: String, budget: u32) -> (i64, i64) {
    let dir = tempfile::TempDir::new().unwrap();
    let bucket = sim.store(&store);
    bucket.set_faults(|f| f.latency = Duration::from_millis(10));
    let mut config = sim.node_config("streamer", &store, &dir.path().join("cache"));
    config.deposit_interval = Duration::from_secs(2);
    config.cap_interval = Duration::from_secs(20);
    config.cap_max_age = Duration::from_secs(30);
    config.crop_max_bytes = u64::MAX;
    config.commit_budget_per_min = budget;
    let node = ApiaryNode::start_with_env(config, sim.env()).await.unwrap();
    make_frame(&node).await;

    let mut sent = 0i64;
    for tick in 0..1800 {
        node.ingest("farm", "field", "readings", &ints([tick as i64]))
            .await
            .unwrap();
        sent += 1;
        sim.sleep(Duration::from_millis(100)).await;
    }
    // Let the last deposits land, then count what the comb and the crop hold.
    sim.sleep(Duration::from_secs(120)).await;
    let total = first_int(&node, "SELECT count(n) FROM farm.field.readings").await;
    node.shutdown().await;
    (sent, total)
}

#[test]
fn a_streaming_frames_commit_rate_stays_within_its_budget() {
    const BUDGET: u32 = 6;
    let budgeted = Sim::run(1, |sim| stream(sim, "budgeted".into(), BUDGET));
    let (sent, total) = budgeted.value;
    assert_eq!(total, sent, "every row is there exactly once");

    let times = commit_times(&budgeted.trace, "farm/field/readings");
    let busiest = busiest_minute(&times);
    println!(
        "budgeted: {} commits in {:?}, busiest minute {busiest}",
        times.len(),
        budgeted.elapsed
    );
    assert!(
        busiest <= BUDGET as usize,
        "no minute saw more than {BUDGET} commits to the Frame, saw {busiest}: {times:?}"
    );
    // The Frame is not starved either: it was committed to throughout.
    assert!(times.len() >= 10, "{} commits", times.len());

    // Without a budget the same cadence commits far more often.
    let free = Sim::run(1, |sim| stream(sim, "unbudgeted".into(), 10_000));
    assert_eq!(free.value.1, free.value.0);
    let busiest_free = busiest_minute(&commit_times(&free.trace, "farm/field/readings"));
    assert!(
        busiest_free > BUDGET as usize * 2,
        "the budget is what held it back: unbudgeted busiest minute {busiest_free}"
    );
}
