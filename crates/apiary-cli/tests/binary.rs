//! The real `apiary` binary: start a Node from a config file, use it over Flight,
//! query it with `apiary sql`, and stop it with a termination signal.
#![cfg(unix)]

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use arrow_flight::Action;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::CommandStatementIngest;
use arrow_flight::sql::client::FlightSqlServiceClient;
use tonic::transport::Channel;

use apiary_comb::Comb;

const BIN: &str = env!("CARGO_BIN_EXE_apiary");

struct Running {
    child: Child,
    port: u16,
    dir: tempfile::TempDir,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn start_node(extra_flight: &str) -> Running {
    let dir = tempfile::TempDir::new().unwrap();
    let port = free_port();
    let config = format!(
        r#"
[node]
storage = "local://{site}"
cache_dir = "{cache}"
deposit_interval_secs = 3600

[flight]
listen = "127.0.0.1:{port}"
{extra_flight}
"#,
        site = dir.path().join("site").display(),
        cache = dir.path().join("cache").display(),
    );
    let path = dir.path().join("apiary.toml");
    std::fs::write(&path, config).unwrap();

    let child = Command::new(BIN)
        .args(["node", "run", "--config"])
        .arg(&path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the binary starts");
    let running = Running { child, port, dir };

    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return running;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the node never listened");
}

async fn channel(port: u16) -> Channel {
    Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

async fn act(port: u16, kind: &str, body: serde_json::Value) {
    let mut client = FlightServiceClient::new(channel(port).await);
    client
        .do_action(Action {
            r#type: kind.to_string(),
            body: serde_json::to_vec(&body).unwrap().into(),
        })
        .await
        .unwrap();
}

fn readings() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "temp",
            Arc::new(Float64Array::from(vec![20.5, 21.0])) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn create_and_fill(port: u16) {
    act(
        port,
        "apiary.create_hive",
        serde_json::json!({"name": "farm"}),
    )
    .await;
    act(
        port,
        "apiary.create_box",
        serde_json::json!({"hive": "farm", "name": "field"}),
    )
    .await;
    act(
        port,
        "apiary.create_frame",
        serde_json::json!({
            "hive": "farm", "box": "field", "name": "readings",
            "schema": {"id": "int64", "temp": "float64"}
        }),
    )
    .await;
    let mut client = FlightSqlServiceClient::new(channel(port).await);
    let landed = client
        .execute_ingest(
            CommandStatementIngest {
                table: "readings".into(),
                schema: Some("field".into()),
                catalog: Some("farm".into()),
                ..Default::default()
            },
            futures::stream::iter(vec![Ok(readings())]),
        )
        .await
        .unwrap();
    assert_eq!(landed, 2);
}

#[tokio::test]
async fn the_cli_queries_a_running_node() {
    let node = start_node("").await;
    create_and_fill(node.port).await;

    let url = format!("http://127.0.0.1:{}", node.port);
    let out = Command::new(BIN)
        .args([
            "sql",
            "--url",
            &url,
            "SELECT id, temp FROM farm.field.readings ORDER BY id",
        ])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains("20.5") && text.contains("21"), "{text}");
    assert!(text.contains("2 from the crop"), "{text}");

    let bad = Command::new(BIN)
        .args(["sql", "--url", &url, "SELECT * FROM farm.field.nope"])
        .output()
        .unwrap();
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("apiary:"));
}

#[tokio::test]
async fn a_termination_signal_deposits_the_crop_before_the_node_exits() {
    let mut node = start_node("").await;
    create_and_fill(node.port).await;

    // The node never deposited (the interval is an hour): the rows are in the crop.
    let site = node.dir.path().join("site");
    let comb = Comb::from_local_path(&site).unwrap();
    let table = comb
        .open_frame_table("farm", "field", "readings")
        .await
        .unwrap()
        .unwrap();
    assert!(comb.read(&table, None).await.unwrap().is_none());

    Command::new("kill")
        .args(["-TERM", &node.child.id().to_string()])
        .status()
        .unwrap();
    let mut exited = None;
    for _ in 0..200 {
        if let Some(status) = node.child.try_wait().unwrap() {
            exited = Some(status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(exited.expect("the node stops on SIGTERM").success());

    let table = comb
        .open_frame_table("farm", "field", "readings")
        .await
        .unwrap()
        .unwrap();
    let rows = comb
        .read(&table, None)
        .await
        .unwrap()
        .expect("rows were deposited");
    assert_eq!(rows.num_rows(), 2, "shutdown deposited the crop");
}

#[tokio::test]
async fn a_token_protects_the_entrance() {
    let node = start_node("token = \"s3cret\"").await;
    let url = format!("http://127.0.0.1:{}", node.port);

    let without = Command::new(BIN)
        .args(["sql", "--url", &url, "SHOW HIVES"])
        .env_remove("APIARY_TOKEN")
        .output()
        .unwrap();
    assert!(!without.status.success());

    let with = Command::new(BIN)
        .args(["sql", "--url", &url, "--token", "s3cret", "SHOW HIVES"])
        .output()
        .unwrap();
    assert!(
        with.status.success(),
        "{}",
        String::from_utf8_lossy(&with.stderr)
    );
}

#[test]
fn check_validates_a_config_file() {
    let dir = tempfile::TempDir::new().unwrap();
    let good = dir.path().join("good.toml");
    std::fs::write(&good, "[node]\nstorage = \"local:///tmp/x\"\n").unwrap();
    let ok = Command::new(BIN)
        .args(["node", "check", "--config"])
        .arg(&good)
        .output()
        .unwrap();
    assert!(ok.status.success());

    let bad = dir.path().join("bad.toml");
    std::fs::write(&bad, "[node]\nstorgae = \"x\"\n").unwrap();
    let out = Command::new(BIN)
        .args(["node", "check", "--config"])
        .arg(&bad)
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("storgae"));
}
