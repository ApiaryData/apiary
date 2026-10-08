//! A colony of real `apiary` processes: a comb host and two client Nodes that
//! reach its drive only through the colony's QUIC connections, set up by a
//! Beekeeper with the CLI.
#![cfg(unix)]

use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use arrow_flight::Action;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::CommandStatementIngest;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::Channel;

const BIN: &str = env!("CARGO_BIN_EXE_apiary");

fn free_tcp() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn free_udp() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn run(args: &[&str]) -> String {
    let out = Command::new(BIN).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "apiary {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

struct Beekeeper {
    dir: tempfile::TempDir,
    public: String,
}

impl Beekeeper {
    fn new() -> Self {
        let dir = tempfile::TempDir::new().unwrap();
        let out = run(&[
            "key",
            "generate",
            "--out",
            dir.path().join("apiary.key").to_str().unwrap(),
        ]);
        let public = out.lines().last().unwrap().trim().to_string();
        Self { dir, public }
    }

    fn key(&self) -> PathBuf {
        self.dir.path().join("apiary.key")
    }

    fn token(&self, caps: &str) -> String {
        run(&[
            "token",
            "issue",
            "--key",
            self.key().to_str().unwrap(),
            "--apiary",
            "factory",
            "--colony",
            "line1",
            "--caps",
            caps,
            "--days",
            "30",
        ])
        .trim()
        .to_string()
    }
}

struct Node {
    child: Option<Child>,
    config: PathBuf,
    flight: u16,
    udp: u16,
    id: String,
    dir: tempfile::TempDir,
}

impl Node {
    /// Write a config and learn the Node's id, but do not start it yet.
    fn prepare(
        bee: &Beekeeper,
        role: &str,
        storage: &str,
        serve_comb: bool,
        bootstrap: Option<(&str, u16)>,
        token: &str,
    ) -> Self {
        let dir = tempfile::TempDir::new().unwrap();
        let flight = free_tcp();
        let udp = free_udp();
        std::fs::write(dir.path().join("token"), token).unwrap();
        let boot = bootstrap
            .map(|(id, port)| {
                format!("bootstrap = [{{ id = \"{id}\", addrs = [\"127.0.0.1:{port}\"] }}]\n")
            })
            .unwrap_or_default();
        let storage = storage.replace("{DIR}", &dir.path().join("comb").display().to_string());
        let config = format!(
            r#"
[node]
storage = "{storage}"
cache_dir = "{cache}"
deposit_interval_secs = 3600

[flight]
listen = "127.0.0.1:{flight}"

[net]
apiary = "factory"
apiary_public_key = "{public}"
token_file = "{token}"
site = "{role}"
udp_port = {udp}
mdns = false
serve_comb = {serve_comb}
discovery_interval_secs = 1
measure_interval_secs = 2
{boot}
"#,
            cache = dir.path().join("cache").display(),
            public = bee.public,
            token = dir.path().join("token").display(),
        );
        let path = dir.path().join("apiary.toml");
        std::fs::write(&path, config).unwrap();
        let id = run(&["node", "id", "--config", path.to_str().unwrap()])
            .trim()
            .to_string();
        Self {
            child: None,
            config: path,
            flight,
            udp,
            id,
            dir,
        }
    }

    fn start(&mut self) {
        self.child = Some(
            Command::new(BIN)
                .args(["node", "run", "--config"])
                .arg(&self.config)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("the binary starts"),
        );
    }

    async fn wait_ready(&self) {
        for _ in 0..600 {
            if std::net::TcpStream::connect(("127.0.0.1", self.flight)).is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("node {} never listened", &self.id[..8]);
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = Command::new("kill")
                .args(["-KILL", &child.id().to_string()])
                .status();
            let _ = child.wait();
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.flight)
    }

    fn comb(&self) -> PathBuf {
        self.dir.path().join("comb")
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn channel(port: u16) -> Channel {
    Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

async fn act(port: u16, kind: &str, body: serde_json::Value) -> Result<serde_json::Value, String> {
    let mut client = FlightServiceClient::new(channel(port).await);
    let mut stream = client
        .do_action(Action {
            r#type: kind.to_string(),
            body: serde_json::to_vec(&body).unwrap().into(),
        })
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    let first = stream
        .message()
        .await
        .map_err(|e| e.message().to_string())?
        .unwrap();
    Ok(serde_json::from_slice(&first.body).unwrap())
}

fn batch(first: i64, rows: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(first..first + rows)) as ArrayRef,
        ),
        (
            "temp",
            Arc::new(Float64Array::from_iter_values((0..rows).map(|i| i as f64))) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn ingest(port: u16, first: i64, rows: i64) -> Result<i64, String> {
    let mut client = FlightSqlServiceClient::new(channel(port).await);
    client
        .execute_ingest(
            CommandStatementIngest {
                table: "readings".into(),
                schema: Some("field".into()),
                catalog: Some("farm".into()),
                ..Default::default()
            },
            futures::stream::iter(vec![Ok(batch(first, rows))]),
        )
        .await
        .map_err(|e| e.to_string())
}

async fn ids(port: u16) -> Result<Vec<i64>, String> {
    let mut client = FlightSqlServiceClient::new(channel(port).await);
    let info = client
        .execute("SELECT id FROM farm.field.readings".to_string(), None)
        .await
        .map_err(|e| e.to_string())?;
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .map_err(|e| e.to_string())?
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        out.extend(col.values().iter().copied());
    }
    out.sort();
    Ok(out)
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..300 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for: {what}");
}

fn status_json(url: &str) -> serde_json::Value {
    serde_json::from_str(&run(&["net", "status", "--url", url, "--json"])).unwrap()
}

fn peer_ids(status: &serde_json::Value) -> Vec<String> {
    status["peers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect()
}

fn files_under(dir: &Path, ext: &str) -> usize {
    let mut n = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|x| x.to_str()) == Some(ext) {
                n += 1;
            }
        }
    }
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_commit_to_one_comb_through_the_host_and_a_revoked_node_is_cut_off() {
    let bee = Beekeeper::new();

    // The host has the drive; the clients reach it by its id.
    let mut host = Node::prepare(
        &bee,
        "pi-site",
        "local://{DIR}",
        true,
        None,
        &bee.token("run,ingest"),
    );
    let drive = format!("apiary-drive://{}/", host.id);
    let boot = Some((host.id.as_str(), host.udp));
    let mut a = Node::prepare(
        &bee,
        "pi-site",
        &drive,
        false,
        boot,
        &bee.token("run,ingest"),
    );
    let mut b = Node::prepare(
        &bee,
        "pi-site",
        &drive,
        false,
        boot,
        &bee.token("run,ingest"),
    );

    host.start();
    host.wait_ready().await;
    act(
        host.flight,
        "apiary.create_hive",
        serde_json::json!({"name": "farm"}),
    )
    .await
    .unwrap();
    act(
        host.flight,
        "apiary.create_box",
        serde_json::json!({"hive": "farm", "name": "field"}),
    )
    .await
    .unwrap();
    act(
        host.flight,
        "apiary.create_frame",
        serde_json::json!({"hive": "farm", "box": "field", "name": "readings",
            "schema": {"id": "int64", "temp": "float64"}}),
    )
    .await
    .unwrap();

    // The clients start with no data of their own: their comb is the host's drive.
    a.start();
    b.start();
    a.wait_ready().await;
    b.wait_ready().await;
    assert!(
        !a.comb().exists() && !b.comb().exists(),
        "clients keep no comb of their own"
    );

    // Each client ingests into its own crop and deposits through the host.
    assert_eq!(ingest(a.flight, 0, 100).await.unwrap(), 100);
    assert_eq!(ingest(b.flight, 1000, 100).await.unwrap(), 100);
    act(a.flight, "apiary.flush_crop", serde_json::json!({}))
        .await
        .unwrap();
    act(b.flight, "apiary.flush_crop", serde_json::json!({}))
        .await
        .unwrap();

    // The table is on the host's disk, and every node reads all of it.
    assert!(
        files_under(&host.comb(), "parquet") >= 2,
        "both deposits are on the host's drive"
    );
    let expected: Vec<i64> = (0..100).chain(1000..1100).collect();
    for node in [&host, &a, &b] {
        assert_eq!(
            ids(node.flight).await.unwrap(),
            expected,
            "node {} sees every row",
            &node.id[..8]
        );
    }
    assert_eq!(
        files_under(&a.dir.path().join("cache"), "parquet"),
        0,
        "clients hold no table data"
    );

    // Everyone sees everyone, and the host says it serves the comb.
    eventually("the host to see both clients", || async {
        let s = status_json(&host.url());
        let peers = peer_ids(&s);
        peers.contains(&a.id) && peers.contains(&b.id)
    })
    .await;
    let s = status_json(&host.url());
    assert_eq!(s["serves_comb"], true);
    assert_eq!(s["colony"], "line1");
    let sa = status_json(&a.url());
    assert!(peer_ids(&sa).contains(&host.id));
    let host_peer = sa["peers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == host.id.as_str())
        .unwrap();
    assert_eq!(
        host_peer["path"], "direct",
        "peers in one site connect directly"
    );
    assert_eq!(host_peer["same_site"], true);

    // The Beekeeper revokes b and hands the list to the host only.
    let list = bee.dir.path().join("revocations.json");
    run(&[
        "revoke",
        "--key",
        bee.key().to_str().unwrap(),
        "--list",
        list.to_str().unwrap(),
        "--node",
        &b.id,
        "--push",
        &host.url(),
    ]);
    eventually("b to be cut off at the host", || async {
        !peer_ids(&status_json(&host.url())).contains(&b.id)
    })
    .await;
    // The host passed the list to a; b is refused everywhere.
    eventually("a to learn of the revocation", || async {
        status_json(&a.url())["revocations_seq"] == 1
    })
    .await;
    assert!(!peer_ids(&status_json(&a.url())).contains(&b.id));
    assert_eq!(status_json(&host.url())["revocations_seq"], 1);

    // b can still take deposits into its crop, but can no longer reach the drive,
    // so nothing it deposits reaches the comb.
    assert_eq!(ingest(b.flight, 2000, 10).await.unwrap(), 10);
    assert!(
        act(b.flight, "apiary.flush_crop", serde_json::json!({}))
            .await
            .is_err()
    );
    assert!(!ids(host.flight).await.unwrap().contains(&2000));

    // a is unaffected.
    assert_eq!(ingest(a.flight, 3000, 10).await.unwrap(), 10);
    act(a.flight, "apiary.flush_crop", serde_json::json!({}))
        .await
        .unwrap();
    assert!(ids(host.flight).await.unwrap().contains(&3000));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_survives_the_host_going_away_and_deposits_when_it_returns() {
    let bee = Beekeeper::new();
    let mut host = Node::prepare(
        &bee,
        "pi-site",
        "local://{DIR}",
        true,
        None,
        &bee.token("run,ingest"),
    );
    let drive = format!("apiary-drive://{}/", host.id);
    let boot = Some((host.id.as_str(), host.udp));
    let mut a = Node::prepare(
        &bee,
        "pi-site",
        &drive,
        false,
        boot,
        &bee.token("run,ingest"),
    );

    host.start();
    host.wait_ready().await;
    act(
        host.flight,
        "apiary.create_hive",
        serde_json::json!({"name": "farm"}),
    )
    .await
    .unwrap();
    act(
        host.flight,
        "apiary.create_box",
        serde_json::json!({"hive": "farm", "name": "field"}),
    )
    .await
    .unwrap();
    act(
        host.flight,
        "apiary.create_frame",
        serde_json::json!({"hive": "farm", "box": "field", "name": "readings",
            "schema": {"id": "int64", "temp": "float64"}}),
    )
    .await
    .unwrap();
    a.start();
    a.wait_ready().await;

    assert_eq!(ingest(a.flight, 0, 50).await.unwrap(), 50);
    act(a.flight, "apiary.flush_crop", serde_json::json!({}))
        .await
        .unwrap();

    // The host (the Pi the drive hangs off) dies. The client keeps ingesting.
    host.kill();
    assert_eq!(ingest(a.flight, 100, 50).await.unwrap(), 50);
    assert_eq!(ingest(a.flight, 200, 50).await.unwrap(), 50);
    assert!(
        act(a.flight, "apiary.flush_crop", serde_json::json!({}))
            .await
            .is_err(),
        "a deposit cannot reach a drive that is down"
    );

    // The drive comes back (same disk, same key: it is the same Node).
    host.start();
    host.wait_ready().await;

    // The client finds it again and deposits what it held, losing nothing.
    eventually("the deposit to go through", || async {
        act(a.flight, "apiary.flush_crop", serde_json::json!({}))
            .await
            .is_ok()
    })
    .await;
    let expected: Vec<i64> = (0..50).chain(100..150).chain(200..250).collect();
    eventually("the host to hold every row", || async {
        ids(host.flight)
            .await
            .map(|v| v == expected)
            .unwrap_or(false)
    })
    .await;
}
