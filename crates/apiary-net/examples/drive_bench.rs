//! What reaching the comb through its host costs.
//!
//! Two Nodes on loopback, connected over real QUIC: a host with the drive and a
//! client that reaches it only through the colony. For each thing a Node does to
//! the comb, the same work is timed against the local directory and through the
//! drive:
//!
//! 1. **Single operations**: head, get and put of a 1 KiB object, and a
//!    create-if-absent (what a Delta commit rests on).
//! 2. **Bulk transfer**: one 64 MiB put and get.
//! 3. **Delta commits**: appending a 1,000-row batch to a Frame.
//! 4. **A table scan**: an aggregate over about two million rows.
//!
//! ```text
//! cargo run --release -p apiary-net --example drive_bench -- --out drive.json
//! ```
//!
//! Loopback has no network latency, so these are the costs of the protocol and
//! the copying, and a floor: a real LAN adds its round trips to every operation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde_json::{Value, json};

use apiary_comb::custom_store::register_store;
use apiary_comb::{CellState, Comb, query_session};
use apiary_core::{FieldDef, FrameSchema, SystemClock};
use apiary_net::{
    ApiaryKey, Caps, ControlRouter, DRIVE_SERVICE, DriveService, DriveStore, IrohConfig,
    IrohTransport, Mesh, MeshConfig, NodeKey, PeerAddr, Protocol, RevocationStore, Token,
    TokenSpec, Transport, Trust,
};

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

async fn time_each<F, Fut>(n: usize, mut op: F) -> f64
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut times = Vec::new();
    for i in 0..n {
        let start = Instant::now();
        op(i).await;
        times.push(ms(start.elapsed()));
    }
    median(times)
}

async fn mesh_node(apiary: &ApiaryKey) -> (NodeKey, Mesh) {
    let key = NodeKey::generate();
    let transport: Arc<dyn Transport> = Arc::new(
        IrohTransport::bind(IrohConfig::new(key.clone()))
            .await
            .unwrap(),
    );
    let spec = TokenSpec {
        apiary: "bench".into(),
        colony: "c".into(),
        caps: Caps::ALL,
        lifetime_secs: 3600,
        node: None,
        bootstrap: vec![],
        relay: None,
    };
    let now = chrono::Utc::now().timestamp();
    let mesh = Mesh::new(
        transport,
        MeshConfig {
            trust: Trust {
                apiary: "bench".into(),
                key: apiary.public(),
            },
            token: Token::parse(&apiary.issue(&spec, now)).unwrap(),
            site: None,
            clock: SystemClock::shared(),
        },
        Arc::new(RevocationStore::open(None, apiary.public())),
    );
    mesh.start();
    (key, mesh)
}

fn schema() -> FrameSchema {
    let f = |name: &str, ty: &str| FieldDef {
        name: name.into(),
        data_type: ty.into(),
        nullable: false,
    };
    FrameSchema {
        fields: vec![f("id", "int64"), f("group", "int64"), f("value", "float64")],
    }
}

fn batch(first: i64, rows: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(first..first + rows)) as ArrayRef,
        ),
        (
            "group",
            Arc::new(Int64Array::from_iter_values(
                (first..first + rows).map(|i| i % 100),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from_iter_values(
                (first..first + rows).map(|i| (i % 1000) as f64 * 0.5),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = args
        .iter()
        .position(|a| a == "--out")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let tmp = tempfile::TempDir::new_in(std::env::temp_dir()).unwrap();
    let comb_dir = tmp.path().join("comb");
    std::fs::create_dir_all(&comb_dir).unwrap();

    // The host and a client, over real QUIC.
    let apiary = ApiaryKey::generate();
    let (host_key, host) = mesh_node(&apiary).await;
    let (_client_key, client) = mesh_node(&apiary).await;
    let local: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&comb_dir).unwrap());
    let router = ControlRouter::new();
    router.add(
        DRIVE_SERVICE,
        Arc::new(DriveService::new(Arc::clone(&local))),
    );
    host.register(Protocol::Control, router);
    let mut host_addr = host.addr();
    host_addr
        .direct
        .retain(|a| a.ip().is_loopback() || !a.ip().is_unspecified());
    client.remember(&host_addr);
    let drive = Arc::new(DriveStore::new(
        client.clone(),
        PeerAddr::id_only(host_key.id()),
    ));
    // The first call connects and admits; keep that out of the timings.
    drive.head(&Path::from("warmup")).await.ok();

    println!("Drive benchmark: a client reaching the host's drive over QUIC on loopback\n");
    let mut report = serde_json::Map::new();

    // 1. Single operations.
    let small = vec![7u8; 1024];
    let n = 300;
    let mut ops = serde_json::Map::new();
    for (label, store) in [
        ("local", Arc::clone(&local)),
        ("drive", drive.clone() as Arc<dyn ObjectStore>),
    ] {
        store
            .put(&Path::from("ops/one"), PutPayload::from(small.clone()))
            .await
            .unwrap();
        let head = time_each(n, |_| {
            let s = Arc::clone(&store);
            async move {
                s.head(&Path::from("ops/one")).await.unwrap();
            }
        })
        .await;
        let get = time_each(n, |_| {
            let s = Arc::clone(&store);
            async move {
                s.get(&Path::from("ops/one"))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
            }
        })
        .await;
        let put = time_each(n, |i| {
            let s = Arc::clone(&store);
            let data = small.clone();
            async move {
                s.put(&Path::from(format!("ops/over-{i}")), PutPayload::from(data))
                    .await
                    .unwrap();
            }
        })
        .await;
        let create = time_each(n, |i| {
            let s = Arc::clone(&store);
            let data = small.clone();
            async move {
                s.put_opts(
                    &Path::from(format!("ops/{label}-create-{i}")),
                    PutPayload::from(data),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            }
        })
        .await;
        println!(
            "{label:>5}: head {head:6.2} ms   get {get:6.2} ms   put {put:6.2} ms   create-if-absent {create:6.2} ms   (1 KiB, median of {n})"
        );
        ops.insert(
            label.into(),
            json!({"head_ms": head, "get_ms": get, "put_ms": put, "create_ms": create}),
        );
    }
    report.insert("single_operations".into(), Value::Object(ops));

    // 2. Bulk transfer.
    println!();
    let big: Vec<u8> = (0..64 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let mut bulk = serde_json::Map::new();
    for (label, store) in [
        ("local", Arc::clone(&local)),
        ("drive", drive.clone() as Arc<dyn ObjectStore>),
    ] {
        let start = Instant::now();
        store
            .put(
                &Path::from(format!("bulk/{label}")),
                PutPayload::from(big.clone()),
            )
            .await
            .unwrap();
        let put = start.elapsed();
        let start = Instant::now();
        let got = store
            .get(&Path::from(format!("bulk/{label}")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let get = start.elapsed();
        assert_eq!(got.len(), big.len());
        let mb = big.len() as f64 / 1_000_000.0;
        println!(
            "{label:>5}: 64 MiB put {:6.0} ms ({:5.0} MB/s)   get {:6.0} ms ({:5.0} MB/s)",
            ms(put),
            mb / put.as_secs_f64(),
            ms(get),
            mb / get.as_secs_f64()
        );
        bulk.insert(
            label.into(),
            json!({"put_ms": ms(put), "put_mb_s": mb / put.as_secs_f64(),
                   "get_ms": ms(get), "get_mb_s": mb / get.as_secs_f64()}),
        );
    }
    report.insert("bulk_64mib".into(), Value::Object(bulk));

    // 3 and 4. Delta commits and a scan, local against through the drive.
    register_store("bench-drive", drive.clone() as Arc<dyn ObjectStore>);
    let combs = [
        (
            "local",
            Comb::from_local_path(&comb_dir.join("tables-local")).unwrap(),
        ),
        (
            "drive",
            Comb::from_storage_uri("apiary-drive://bench-drive/tables-drive/").unwrap(),
        ),
    ];
    println!();
    let mut commits = serde_json::Map::new();
    let mut scans = serde_json::Map::new();
    for (label, comb) in &combs {
        let table = comb
            .create_frame_table("h", "b", "f", &schema(), &[])
            .await
            .unwrap();
        // Commits: a 1,000-row batch each.
        let t = Arc::new(table);
        let commit = time_each(40, |i| {
            let comb = comb.clone();
            let t = Arc::clone(&t);
            async move {
                let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
                let _ = &t;
                comb.append(
                    &table,
                    &batch(i as i64 * 1000, 1000),
                    64 << 20,
                    CellState::Nectar,
                )
                .await
                .unwrap();
            }
        })
        .await;

        // Fill to about two million rows in a few large Cells, then scan.
        let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
        for part in 0..4 {
            comb.append(
                &table,
                &batch(1_000_000 + part * 500_000, 500_000),
                64 << 20,
                CellState::Nectar,
            )
            .await
            .unwrap();
        }
        let table = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
        let rows = comb.frame_stats(&table).unwrap();
        let mut times = Vec::new();
        for _ in 0..5 {
            let ctx = query_session();
            comb.register_table(&ctx, "t", &table).await.unwrap();
            let start = Instant::now();
            let result = ctx
                .sql("SELECT \"group\", avg(value), count(*) FROM t GROUP BY \"group\"")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            times.push(ms(start.elapsed()));
            assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 100);
        }
        let scan = median(times);
        println!(
            "{label:>5}: Delta commit of 1,000 rows {commit:6.1} ms   scan+aggregate over {} rows ({:.0} MB) {scan:7.1} ms",
            rows.rows,
            rows.bytes as f64 / 1e6
        );
        commits.insert((*label).into(), json!(commit));
        scans.insert(
            (*label).into(),
            json!({"ms": scan, "rows": rows.rows, "bytes": rows.bytes}),
        );
    }
    report.insert("delta_commit_1000_rows_ms".into(), Value::Object(commits));
    report.insert("scan_aggregate".into(), Value::Object(scans));

    if let Some(path) = out {
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&Value::Object(report)).unwrap(),
        )
        .unwrap();
        println!("\nwrote {path}");
    }
}
