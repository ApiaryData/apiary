//! The V1 multi-node, node-failure and chaos tests, as simulator tests.
//!
//! The same questions (do Nodes find each other through the shared bucket, is a
//! lost Node noticed, does the data outlive its writers) asked on a virtual clock
//! and a simulated bucket. A "lost Node" here is one cut off from the bucket
//! alone: it stops heartbeating and the rest of the colony has to notice. Each
//! scenario costs no real waiting, and replays exactly from its seed.

use std::sync::Arc;
use std::time::Duration;

use apiary_core::NodeId;
use apiary_observe::{Sim, SimStore};
use apiary_runtime::ApiaryNode;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

struct Member {
    node: ApiaryNode,
    /// This Node's own way into the shared bucket.
    link: SimStore,
    _cache: tempfile::TempDir,
}

/// Start the Node called `name` on its own view of the shared `bucket`.
async fn start(sim: &Sim, bucket: &SimStore, name: &str) -> Member {
    let link = sim.store_view(bucket, &format!("link-{name}"));
    link.set_faults(|f| f.latency = Duration::from_millis(5));
    let cache = tempfile::TempDir::new().unwrap();
    let mut config = sim.node_config(name, &format!("link-{name}"), &cache.path().join("cache"));
    config.heartbeat_interval = Duration::from_millis(500);
    config.dead_threshold = Duration::from_secs(5);
    config.deposit_interval = Duration::from_secs(3600);
    config.crop_max_bytes = u64::MAX;
    let node = ApiaryNode::start_with_env(config, sim.env())
        .await
        .expect("the Node starts");
    Member {
        node,
        link,
        _cache: cache,
    }
}

async fn states(observer: &ApiaryNode) -> Vec<(String, String)> {
    let mut nodes: Vec<_> = observer
        .swarm_status()
        .await
        .nodes
        .into_iter()
        .map(|n| (n.node_id, n.state))
        .collect();
    nodes.sort();
    nodes
}

fn state_of(states: &[(String, String)], id: &NodeId) -> String {
    states
        .iter()
        .find(|(n, _)| n == id.as_str())
        .map_or_else(|| "unseen".to_string(), |(_, s)| s.clone())
}

async fn first_int(node: &ApiaryNode, sql: &str) -> i64 {
    node.sql(sql).await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn make_frame(node: &ApiaryNode, hive: &str, frame: &str) {
    node.registry.create_hive(hive).await.unwrap();
    node.registry.create_box(hive, "data").await.unwrap();
    node.registry
        .create_frame(
            hive,
            "data",
            frame,
            serde_json::json!({"x": "int64"}),
            vec![],
        )
        .await
        .unwrap();
}

fn ints(values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
}

#[test]
fn nodes_find_each_other_through_the_shared_bucket() {
    Sim::run(1, |sim| async move {
        let bucket = sim.store("bucket-a");
        let (a, b) = (
            start(&sim, &bucket, "a").await,
            start(&sim, &bucket, "b").await,
        );
        sim.sleep(Duration::from_secs(2)).await;
        for member in [&a, &b] {
            let seen = states(&member.node).await;
            assert!(seen.len() >= 2, "{seen:?}");
            assert!(seen.iter().all(|(_, s)| s == "alive"), "{seen:?}");
        }
        a.node.shutdown().await;
        b.node.shutdown().await;
    });
}

/// Three Nodes; one loses its link to the bucket for a while; the others notice,
/// and it is welcomed back when the link returns. Returns what each saw.
async fn lost_and_found(sim: Sim, bucket_name: &str) -> (String, String, String) {
    let bucket = sim.store(bucket_name);
    let a = start(&sim, &bucket, "a").await;
    let b = start(&sim, &bucket, "b").await;
    let c = start(&sim, &bucket, "c").await;
    sim.sleep(Duration::from_secs(3)).await;
    let c_id = c.node.config.node_id.clone();

    // c loses its link. Its heartbeats stop; a and b wait out the dead threshold.
    c.link.set_down(true);
    sim.sleep(Duration::from_secs(10)).await;
    let while_cut = state_of(&states(&a.node).await, &c_id);
    let b_agrees = state_of(&states(&b.node).await, &c_id);

    // The link returns.
    c.link.set_down(false);
    sim.sleep(Duration::from_secs(5)).await;
    let after = state_of(&states(&a.node).await, &c_id);

    for member in [a, b, c] {
        member.node.shutdown().await;
    }
    assert_eq!(while_cut, b_agrees, "both survivors agree");
    (while_cut, after, b_agrees)
}

#[test]
fn a_node_cut_off_from_the_bucket_is_noticed_and_welcomed_back() {
    let run = Sim::run(2, |sim| lost_and_found(sim, "bucket-b"));
    let (while_cut, after, _) = run.value;
    assert_eq!(while_cut, "dead", "the survivors declare it dead");
    assert_eq!(after, "alive", "and see it again when its link returns");
    assert!(
        run.trace
            .of_kind("store.")
            .iter()
            .any(|e| e.node == "store:link-c" && e.detail.contains("the store is down")),
        "its refused requests are in the trace"
    );
}

#[test]
fn data_survives_the_loss_of_its_writers() {
    // The chaos test: three Nodes, writes, Nodes lost one by one.
    Sim::run(3, |sim| async move {
        let bucket = sim.store("bucket-c");
        let n0 = start(&sim, &bucket, "n0").await;
        let n1 = start(&sim, &bucket, "n1").await;
        let n2 = start(&sim, &bucket, "n2").await;
        sim.sleep(Duration::from_secs(2)).await;

        make_frame(&n0.node, "chaos", "values").await;
        n0.node
            .write_to_frame(
                "chaos",
                "data",
                "values",
                &ints(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            )
            .await
            .unwrap();

        n1.node.shutdown().await;
        sim.sleep(Duration::from_millis(200)).await;
        for member in [&n0, &n2] {
            let count = first_int(&member.node, "SELECT COUNT(*) FROM chaos.data.values").await;
            assert_eq!(count, 10, "the data survives n1");
        }

        n0.node.shutdown().await;
        sim.sleep(Duration::from_millis(200)).await;
        let total = first_int(&n2.node, "SELECT SUM(x) FROM chaos.data.values").await;
        assert_eq!(total, 55, "and n0, with two Nodes down");
        n2.node.shutdown().await;
    });
}

#[test]
fn a_new_node_sees_the_history_of_a_dead_one() {
    Sim::run(4, |sim| async move {
        let bucket = sim.store("bucket-d");
        let first = start(&sim, &bucket, "first").await;
        make_frame(&first.node, "history", "log").await;
        first
            .node
            .write_to_frame("history", "data", "log", &ints(&[100, 200, 300]))
            .await
            .unwrap();
        first.node.shutdown().await;

        let second = start(&sim, &bucket, "second").await;
        assert_eq!(
            first_int(&second.node, "SELECT COUNT(*) FROM history.data.log").await,
            3
        );
        second.node.shutdown().await;
    });
}

#[test]
fn ingest_and_deposit_are_marked_in_the_trace() {
    let run = Sim::run(5, |sim| async move {
        let bucket = sim.store("bucket-e");
        let member = start(&sim, &bucket, "marked").await;
        member.node.registry.create_hive("farm").await.unwrap();
        member
            .node
            .registry
            .create_box("farm", "data")
            .await
            .unwrap();
        member
            .node
            .registry
            .create_frame(
                "farm",
                "data",
                "r",
                serde_json::json!({"region": "string", "x": "int64"}),
                vec![],
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "region",
                Arc::new(StringArray::from(vec!["n", "s"])) as Arc<dyn arrow::array::Array>,
            ),
            (
                "x",
                Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn arrow::array::Array>,
            ),
        ])
        .unwrap();
        member
            .node
            .ingest("farm", "data", "r", &batch)
            .await
            .unwrap();
        member.node.flush_crop().await.unwrap();
        let id = member.node.config.node_id.as_str().to_string();
        member.node.shutdown().await;
        id
    });
    let id = run.value;
    let marks: Vec<_> = run
        .trace
        .events()
        .into_iter()
        .filter(|e| e.node == id)
        .collect();
    let kinds: Vec<&str> = marks.iter().map(|e| e.kind.as_str()).collect();
    assert_eq!(kinds, ["ingest", "deposit"], "{marks:?}");
    assert!(marks[0].detail.contains("farm.data.r 2 rows"));
    assert!(marks[1].detail.contains("rows: 2"), "{}", marks[1].detail);
    assert!(marks[1].at > marks[0].at, "the deposit takes virtual time");
}

#[test]
fn a_run_with_a_lost_node_replays_exactly() {
    let a = Sim::run(9, |sim| lost_and_found(sim, "bucket-f"));
    let b = Sim::run(9, |sim| lost_and_found(sim, "bucket-f"));
    let (ea, eb) = (a.trace.events(), b.trace.events());
    if let Some(i) = (0..ea.len().min(eb.len())).find(|i| ea[*i] != eb[*i]) {
        panic!(
            "the runs diverge at event {i} of {} and {}:\n  first:  {:?}\n  second: {:?}",
            ea.len(),
            eb.len(),
            ea[i],
            eb[i]
        );
    }
    assert_eq!(ea.len(), eb.len());
    assert_eq!(a.value, b.value);
    assert_eq!(a.elapsed, b.elapsed);
}

/// How many seeds the sweeps try: `SIM_SEEDS=500 cargo test` widens them.
fn seeds() -> u64 {
    std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20)
}

#[test]
fn a_lost_node_is_noticed_and_every_seed_replays() {
    for seed in 0..seeds() {
        let name: &'static str = Box::leak(format!("sweep-{seed}").into_boxed_str());
        let first = Sim::run(seed, |sim| lost_and_found(sim, name));
        let (while_cut, after, _) = &first.value;
        assert_eq!(while_cut, "dead", "seed {seed}");
        assert_eq!(after, "alive", "seed {seed}");
        let again = Sim::run(seed, |sim| lost_and_found(sim, name));
        assert_eq!(
            first.trace.digest(),
            again.trace.digest(),
            "seed {seed} does not replay"
        );
    }
}
