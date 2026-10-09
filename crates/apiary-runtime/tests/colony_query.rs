//! Integration test: a query run across a colony of real Nodes.
//!
//! Two Nodes share one comb, are admitted to the same colony over the in-memory
//! network, and gossip a dance floor. A query asked of one is planned there, cut
//! into Patches, advertised by dances, and run by whichever Bees pick the Patches
//! up, on either Node, under their share of memory. Its answer is the answer the
//! query gives when it runs whole.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use apiary_core::SystemClock;
use apiary_core::config::NodeConfig;
use apiary_floor::{Floor, GossipConfig, GossipFloor};
use apiary_forage::ForageConfig;
use apiary_net::{
    ApiaryKey, Caps, MemNetwork, Mesh, MeshConfig, NodeKey, PeerAddr, Protocol, RevocationStore,
    Token, TokenSpec, Transport, Trust,
};
use apiary_runtime::ApiaryNode;

const APIARY: &str = "farm";

struct Member {
    node: Arc<ApiaryNode>,
    floor: Arc<GossipFloor>,
    mesh: Mesh,
    id: apiary_net::NodeId,
}

async fn start_node(dir: &std::path::Path, name: &str) -> Arc<ApiaryNode> {
    let mut config = NodeConfig::detect("local://test");
    config.storage_uri = format!("local://{}", dir.display());
    config.cores = 2;
    config.memory_per_bee = 128 * 1024 * 1024;
    config.cache_dir = dir.join(format!("cache_{name}"));
    config.heartbeat_interval = Duration::from_millis(500);
    config.dead_threshold = Duration::from_secs(5);
    config.deposit_interval = Duration::from_secs(3600);
    Arc::new(ApiaryNode::start(config).await.expect("the Node starts"))
}

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

/// Every row as text, sorted.
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

async fn join(
    apiary: &ApiaryKey,
    net: &MemNetwork,
    node: Arc<ApiaryNode>,
    forage: ForageConfig,
) -> Member {
    let key = NodeKey::generate();
    let spec = TokenSpec {
        apiary: APIARY.into(),
        colony: "barn".into(),
        caps: Caps::ALL,
        lifetime_secs: 3600,
        node: None,
        bootstrap: vec![],
        relay: None,
    };
    let token = apiary.issue(&spec, chrono::Utc::now().timestamp());
    let transport: Arc<dyn Transport> = Arc::new(net.join(key.id()));
    let mesh = Mesh::new(
        transport,
        MeshConfig {
            trust: Trust {
                apiary: APIARY.into(),
                key: apiary.public(),
            },
            token: Token::parse(&token).unwrap(),
            site: Some("barn".into()),
            clock: SystemClock::shared(),
        },
        Arc::new(RevocationStore::open(None, apiary.public())),
    );
    mesh.start();
    let floor = Arc::new(GossipFloor::start(
        mesh.clone(),
        SystemClock::shared(),
        GossipConfig::new("barn"),
    ));
    node.join_colony(key.id(), Arc::clone(&floor) as Arc<dyn Floor>, forage);
    Member {
        node,
        floor,
        mesh,
        id: key.id(),
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
async fn a_query_is_run_across_two_real_nodes_and_gives_the_whole_answer() {
    let dir = tempfile::tempdir().unwrap();
    let apiary = ApiaryKey::generate();
    let net = MemNetwork::new();

    // No query is too small to share, so the test exercises the colony's path.
    let share_everything = ForageConfig {
        local_bytes: 0,
        ..ForageConfig::default()
    };
    let a = join(
        &apiary,
        &net,
        start_node(dir.path(), "a").await,
        share_everything.clone(),
    )
    .await;
    let b = join(
        &apiary,
        &net,
        start_node(dir.path(), "b").await,
        share_everything,
    )
    .await;
    a.mesh
        .connect(&PeerAddr::id_only(b.id), Protocol::Control)
        .await
        .expect("the Nodes are admitted to each other");

    // A Frame with several files in the comb.
    a.node.registry.create_hive("farm").await.unwrap();
    a.node.registry.create_box("farm", "field").await.unwrap();
    a.node
        .registry
        .create_frame(
            "farm",
            "field",
            "sensors",
            serde_json::json!({"region": "string", "temp": "float64"}),
            vec![],
        )
        .await
        .unwrap();
    for (region, offset) in [("north", 0), ("south", 10), ("east", 20), ("west", 30)] {
        a.node
            .write_to_frame("farm", "field", "sensors", &rows(region, 20_000, offset))
            .await
            .unwrap();
    }

    eventually("each Node to see the other's presence", || {
        a.node.colony_peers() >= 1 && b.node.colony_peers() >= 1
    })
    .await;

    let queries = [
        "SELECT region, count(*), sum(temp) FROM farm.field.sensors GROUP BY region",
        "SELECT count(*), min(temp), max(temp) FROM farm.field.sensors WHERE temp > 5",
        "SELECT region, temp FROM farm.field.sensors ORDER BY temp DESC, region LIMIT 25",
        "SELECT x.region, count(*) FROM farm.field.sensors x JOIN farm.field.sensors y \
         ON x.temp = y.temp WHERE x.temp < 2 GROUP BY x.region",
    ];
    for sql in queries {
        let whole = lines(&a.node.query_ctx.sql(sql).await.unwrap());
        // Asked of A, it runs in the colony: planned on A, Patches taken by either.
        let across = lines(&a.node.sql(sql).await.unwrap());
        assert_eq!(across, whole, "{sql}");
    }

    // Both Nodes did some of the work between them.
    let (fa, fb) = (
        a.node.colony_forage().unwrap().counts(),
        b.node.colony_forage().unwrap().counts(),
    );
    assert_eq!(fa.planned, queries.len() as u64, "{fa:?}");
    assert_eq!(fb.planned, 0);
    assert!(
        fa.won + fb.won > 0 && fa.failed == 0 && fb.failed == 0,
        "{fa:?} {fb:?}"
    );
    eprintln!("A {fa:?}\nB {fb:?}");

    // What going through the colony costs a small query.
    for sql in [
        "SELECT count(*) FROM farm.field.sensors",
        "SELECT region, sum(temp) FROM farm.field.sensors GROUP BY region",
    ] {
        let t = std::time::Instant::now();
        for _ in 0..10 {
            a.node.query_ctx.sql(sql).await.unwrap();
        }
        let whole = t.elapsed() / 10;
        let t = std::time::Instant::now();
        for _ in 0..10 {
            a.node.sql(sql).await.unwrap();
        }
        eprintln!(
            "{sql}: whole {whole:?}, across the colony {:?}",
            t.elapsed() / 10
        );
    }

    // A query with no rows still has its columns.
    let empty = a
        .node
        .sql_with_stages("SELECT region, temp FROM farm.field.sensors WHERE temp > 1000")
        .await
        .unwrap();
    assert_eq!(empty.schema.fields().len(), 2);
    assert_eq!(empty.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

    // A command is not a query: it runs where it is asked.
    let hives = a.node.sql("SHOW HIVES").await.unwrap();
    assert_eq!(hives[0].num_rows(), 1);

    let _ = (&a.floor, &b.floor);
    a.node.shutdown().await;
    b.node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_small_query_stays_on_the_node_that_was_asked() {
    let dir = tempfile::tempdir().unwrap();
    let apiary = ApiaryKey::generate();
    let net = MemNetwork::new();
    let a = join(
        &apiary,
        &net,
        start_node(dir.path(), "a").await,
        ForageConfig::default(),
    )
    .await;
    let b = join(
        &apiary,
        &net,
        start_node(dir.path(), "b").await,
        ForageConfig::default(),
    )
    .await;
    a.mesh
        .connect(&PeerAddr::id_only(b.id), Protocol::Control)
        .await
        .unwrap();
    a.node.registry.create_hive("farm").await.unwrap();
    a.node.registry.create_box("farm", "field").await.unwrap();
    a.node
        .registry
        .create_frame(
            "farm",
            "field",
            "sensors",
            serde_json::json!({"region": "string", "temp": "float64"}),
            vec![],
        )
        .await
        .unwrap();
    for (region, offset) in [("north", 0), ("south", 10)] {
        a.node
            .write_to_frame("farm", "field", "sensors", &rows(region, 5_000, offset))
            .await
            .unwrap();
    }
    eventually("the Nodes to see each other", || a.node.colony_peers() >= 1).await;

    let sql = "SELECT region, count(*), sum(temp) FROM farm.field.sensors GROUP BY region";
    let whole = lines(&a.node.query_ctx.sql(sql).await.unwrap());
    let t = std::time::Instant::now();
    for _ in 0..10 {
        assert_eq!(lines(&a.node.sql(sql).await.unwrap()), whole);
    }
    eprintln!("a small query in a colony: {:?} each", t.elapsed() / 10);

    let (fa, fb) = (
        a.node.colony_forage().unwrap().counts(),
        b.node.colony_forage().unwrap().counts(),
    );
    assert_eq!((fa.planned, fa.whole), (10, 10), "{fa:?}");
    assert_eq!((fa.won, fb.won, fb.planned), (0, 0, 0), "{fa:?} {fb:?}");
    a.node.shutdown().await;
    b.node.shutdown().await;
}
