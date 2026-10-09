//! Phase 3 gate, first half: the solitary engine runs unchanged against the
//! simulated store and clock, and a run replays exactly from its seed.
//!
//! The Node here is the production `ApiaryNode`. Nothing in it is stubbed: it is
//! given the simulation's environment (virtual clock, seed) and a comb root that
//! happens to be a simulated bucket.

use std::sync::Arc;
use std::time::Duration;

use apiary_observe::{Sim, Trace};
use apiary_runtime::ApiaryNode;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

fn batch(values: std::ops::Range<i64>) -> RecordBatch {
    let n = values.clone().count();
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec!["north"; n])) as ArrayRef,
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

/// What a scenario reports, so tests can compare runs.
#[derive(Debug, PartialEq, Eq)]
struct Report {
    in_comb_before_outage: i64,
    flush_during_outage_failed: bool,
    crop_held_during_outage: bool,
    total_after_recovery: i64,
    in_comb_after_recovery: i64,
    objects: usize,
}

/// One Node on a slow, unreliable bucket: ingest and ship some rows, lose the
/// bucket for a while, keep ingesting, get it back, and ship the rest.
async fn scenario(sim: Sim, store_name: &'static str) -> Report {
    let dir = tempfile::TempDir::new().unwrap();
    let store = sim.store(store_name);
    store.set_faults(|f| {
        f.latency = Duration::from_millis(20);
        f.jitter = Duration::from_millis(30);
    });
    let mut config = sim.node_config("solo", store_name, &dir.path().join("cache"));
    config.deposit_interval = Duration::from_secs(3600);
    config.crop_max_bytes = u64::MAX;
    let node = ApiaryNode::start_with_env(config, sim.env())
        .await
        .expect("the Node starts on a simulated store");

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

    node.ingest("farm", "field", "readings", &batch(0..10))
        .await
        .unwrap();
    node.flush_crop().await.unwrap();
    let in_comb_before_outage = count(&node, "WHERE _stage = 'comb'").await;

    // The bucket goes away. Ingest carries on into the crop; shipping fails and
    // loses nothing. (A query needs the comb's table, so it waits for the bucket.)
    store.set_down(true);
    node.ingest("farm", "field", "readings", &batch(10..25))
        .await
        .unwrap();
    let flush_during_outage_failed = node.flush_crop().await.is_err();
    let crop_held_during_outage = node.crop.pending_bytes().unwrap() > 0;

    // It comes back, and the held rows ship.
    store.set_down(false);
    sim.sleep(Duration::from_secs(30)).await;
    node.flush_crop().await.unwrap();

    let report = Report {
        in_comb_before_outage,
        flush_during_outage_failed,
        crop_held_during_outage,
        total_after_recovery: count(&node, "").await,
        in_comb_after_recovery: count(&node, "WHERE _stage = 'comb'").await,
        objects: store.object_count().await,
    };
    node.shutdown().await;
    report
}

#[test]
fn the_engine_runs_unchanged_on_a_simulated_store() {
    let run = Sim::run(7, |sim| scenario(sim, "solo-a"));
    let r = &run.value;
    assert_eq!(r.in_comb_before_outage, 10);
    assert!(r.flush_during_outage_failed, "an outage stops the deposit");
    assert!(r.crop_held_during_outage, "the crop held what ingest took");
    assert_eq!(r.total_after_recovery, 25, "no row lost or repeated");
    assert_eq!(r.in_comb_after_recovery, 25, "all of it shipped");
    assert!(r.objects > 0);
    // The run cost virtual time (latency, the outage wait) and no real time to speak of.
    assert!(run.elapsed >= Duration::from_secs(30), "{:?}", run.elapsed);
    assert!(!run.trace.of_kind("store.put").is_empty());
    assert!(
        run.trace
            .of_kind("store.")
            .iter()
            .any(|e| e.detail.contains("refused: the store is down")),
        "the outage shows in the trace"
    );
}

fn digest_of(seed: u64, store: &'static str) -> (u64, Trace) {
    let run = Sim::run(seed, |sim| scenario(sim, store));
    (run.trace.digest(), run.trace)
}

#[test]
fn a_run_replays_exactly_from_its_seed() {
    let (first, trace_a) = digest_of(11, "solo-b");
    let (second, trace_b) = digest_of(11, "solo-b");
    let (a, b) = (trace_a.events(), trace_b.events());
    if let Some(i) = (0..a.len().min(b.len())).find(|i| a[*i] != b[*i]) {
        panic!(
            "the runs diverge at event {i} of {} and {}:
  first:  {:?}
  second: {:?}
  before: {:?}",
            a.len(),
            b.len(),
            a[i],
            b[i],
            &a[i.saturating_sub(3)..i]
        );
    }
    assert_eq!(a.len(), b.len(), "one run is a prefix of the other");
    assert_eq!(first, second);
    assert!(
        trace_a.len() > 50,
        "the trace is substantial: {}",
        trace_a.len()
    );

    // Another seed draws other latencies, so the run differs.
    let (other, _) = digest_of(12, "solo-b");
    assert_ne!(first, other);
}

/// How many seeds the sweep tries: `SIM_SEEDS=500 cargo test` widens it.
fn seeds() -> u64 {
    std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20)
}

#[test]
fn the_outage_scenario_holds_across_seeds_and_each_replays() {
    for seed in 0..seeds() {
        // A distinct bucket per seed, as the registry is process-wide.
        let name: &'static str = Box::leak(format!("sweep-{seed}").into_boxed_str());
        let first = Sim::run(seed, |sim| scenario(sim, name));
        let r = &first.value;
        assert_eq!(
            r.total_after_recovery, 25,
            "seed {seed}: no row lost or repeated"
        );
        assert_eq!(
            r.in_comb_after_recovery, 25,
            "seed {seed}: all of it shipped"
        );
        assert!(r.flush_during_outage_failed, "seed {seed}");
        let again = Sim::run(seed, |sim| scenario(sim, name));
        let (a, b) = (first.trace.events(), again.trace.events());
        if let Some(i) = (0..a.len().min(b.len())).find(|i| a[*i] != b[*i]) {
            panic!(
                "seed {seed} does not replay: diverges at event {i} of {} and {}:
  first:  {:?}
  second: {:?}
  before: {:?}",
                a.len(),
                b.len(),
                a[i],
                b[i],
                &a[i.saturating_sub(2)..i]
            );
        }
        assert_eq!(
            a.len(),
            b.len(),
            "seed {seed}: one run is a prefix of the other"
        );
        assert_eq!(
            first.trace.digest(),
            again.trace.digest(),
            "seed {seed} does not replay: rerun with SIM_SEEDS={}",
            seed + 1
        );
    }
}
