//! Entrance benchmark: what it costs to deposit and query through the entrances.
//!
//! Run in release mode, ideally on the hardware (or the container limits) you
//! care about:
//!
//! ```text
//! cargo run --release -p apiary-entrance --example entrance_bench -- --out entrance.json
//! ```
//!
//! On sensor-shaped rows (timestamp, device, two readings, status) it measures:
//!
//! 1. **Flight ingest**, per batch size, against the same ingest called directly
//!    on the Node: the difference is what the gRPC call and the Guard cost.
//! 2. **Flight queries** over a populated crop, and after it is deposited.
//! 3. **MQTT ingest** end to end (publish to an embedded broker until the rows are
//!    queryable), with one row per message and with twenty.
//!
//! Every Node here syncs its crop to disk before an ingest returns.

use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::CommandStatementIngest;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::{Value, json};
use tonic::transport::Channel;

use apiary_core::config::NodeConfig;
use apiary_entrance::mqtt::{self, MqttConfig, Subscription};
use apiary_entrance::{Guard, SetAside, Source, flight};
use apiary_runtime::ApiaryNode;

const HOUR: Duration = Duration::from_secs(3600);

fn sensor_batch(start: i64, rows: usize) -> RecordBatch {
    let ids = || (0..rows as i64).map(move |i| start + i);
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                ids().map(|id| 1_700_000_000_000_000 + id * 1_000_000),
            )),
        ),
        (
            "device",
            Arc::new(StringArray::from_iter_values(
                ids().map(|id| format!("device-{:03}", id % 50)),
            )),
        ),
        (
            "temp",
            Arc::new(Float64Array::from_iter_values(
                ids().map(|id| 20.0 + (id % 100) as f64 * 0.1),
            )),
        ),
        (
            "humidity",
            Arc::new(Float64Array::from_iter_values(
                ids().map(|id| 40.0 + (id % 60) as f64 * 0.5),
            )),
        ),
        (
            "status",
            Arc::new(Int64Array::from_iter_values(ids().map(|id| id % 4))),
        ),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn json_rows(start: i64, rows: usize) -> String {
    let items: Vec<Value> = (0..rows as i64)
        .map(|i| {
            let id = start + i;
            json!({
                "ts": 1_700_000_000_000_000i64 + id * 1_000_000,
                "device": format!("device-{:03}", id % 50),
                "temp": 20.0 + (id % 100) as f64 * 0.1,
                "humidity": 40.0 + (id % 60) as f64 * 0.5,
                "status": id % 4,
            })
        })
        .collect();
    serde_json::to_string(&items).unwrap()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

struct Bench {
    _tmp: tempfile::TempDir,
    node: Arc<ApiaryNode>,
    guard: Guard,
}

async fn node(dir: &Path) -> Bench {
    let tmp = tempfile::TempDir::new_in(dir).unwrap();
    let mut config = NodeConfig::detect("local://bench");
    config.storage_uri = format!("local://{}", tmp.path().join("site").display());
    config.cache_dir = tmp.path().join("cache");
    config.deposit_interval = HOUR;
    config.crop_max_bytes = u64::MAX;
    config.crop_sync = true;
    config.cap_interval = HOUR;
    config.harvest_interval = HOUR;
    config.clear_interval = HOUR;
    let aside = SetAside::open(config.set_aside_dir()).unwrap();
    let node = Arc::new(ApiaryNode::start(config).await.unwrap());
    node.registry.create_hive("bench").await.unwrap();
    node.registry.create_box("bench", "plant").await.unwrap();
    node.registry
        .create_frame(
            "bench",
            "plant",
            "readings",
            serde_json::from_str(r#"{"ts": "datetime", "device": "string", "temp": "float64", "humidity": "float64", "status": "int64"}"#).unwrap(),
            vec![],
        )
        .await
        .unwrap();
    let guard = Guard::new(Arc::clone(&node), aside);
    Bench {
        _tmp: tmp,
        node,
        guard,
    }
}

async fn connect(addr: SocketAddr) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .initial_stream_window_size(Some(8 * 1024 * 1024))
        .initial_connection_window_size(Some(16 * 1024 * 1024))
        .connect()
        .await
        .unwrap();
    FlightSqlServiceClient::new(channel)
}

fn ingest_command() -> CommandStatementIngest {
    CommandStatementIngest {
        table: "readings".into(),
        schema: Some("plant".into()),
        catalog: Some("bench".into()),
        ..Default::default()
    }
}

async fn direct_query_ms(node: &ApiaryNode, sql: &str, runs: usize) -> f64 {
    let mut times = Vec::new();
    for _ in 0..runs {
        let start = Instant::now();
        node.sql_with_stages(sql).await.unwrap();
        times.push(ms(start.elapsed()));
    }
    median(times)
}

async fn query_ms(client: &mut FlightSqlServiceClient<Channel>, sql: &str, runs: usize) -> f64 {
    let mut times = Vec::new();
    for _ in 0..runs {
        let start = Instant::now();
        let info = client.execute(sql.to_string(), None).await.unwrap();
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        let _: Vec<RecordBatch> = client
            .do_get(ticket)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        times.push(ms(start.elapsed()));
    }
    median(times)
}

async fn flight_section(dir: &Path) -> Value {
    let mut rows = Vec::new();
    for (batch_rows, calls) in [(10usize, 300usize), (100, 300), (1000, 200), (10_000, 50)] {
        // Direct on the Node.
        let direct = node(dir).await;
        let mut direct_times = Vec::new();
        for i in 0..calls {
            let batch = sensor_batch((i * batch_rows) as i64, batch_rows);
            let start = Instant::now();
            direct
                .guard
                .admit(
                    "bench",
                    "plant",
                    "readings",
                    &batch,
                    &Source::Caller("bench".into()),
                )
                .await
                .unwrap();
            direct_times.push(ms(start.elapsed()));
        }

        // Over Flight.
        let served = node(dir).await;
        let server = flight::start(served.guard.clone(), "127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let mut client = connect(server.addr()).await;
        let mut flight_times = Vec::new();
        let wall = Instant::now();
        for i in 0..calls {
            let batch = sensor_batch((i * batch_rows) as i64, batch_rows);
            let start = Instant::now();
            client
                .execute_ingest(ingest_command(), futures::stream::iter(vec![Ok(batch)]))
                .await
                .unwrap();
            flight_times.push(ms(start.elapsed()));
        }
        let wall = wall.elapsed();
        server.stop().await;

        let direct_median = median(direct_times);
        let flight_median = median(flight_times);
        println!(
            "flight ingest  {batch_rows:>6} rows/batch: direct {direct_median:>7.2} ms, flight {flight_median:>7.2} ms, {:>9.0} rows/s",
            (calls * batch_rows) as f64 / wall.as_secs_f64()
        );
        rows.push(json!({
            "batch_rows": batch_rows,
            "calls": calls,
            "direct_median_ms": direct_median,
            "flight_median_ms": flight_median,
            "overhead_ms": flight_median - direct_median,
            "flight_rows_per_sec": (calls * batch_rows) as f64 / wall.as_secs_f64(),
        }));
        direct.node.shutdown().await;
        served.node.shutdown().await;
    }
    Value::Array(rows)
}

async fn query_section(dir: &Path) -> Value {
    let b = node(dir).await;
    let server = flight::start(b.guard.clone(), "127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();
    let mut client = connect(server.addr()).await;
    for i in 0..100 {
        client
            .execute_ingest(
                ingest_command(),
                futures::stream::iter(vec![Ok(sensor_batch(i * 1000, 1000))]),
            )
            .await
            .unwrap();
    }
    let aggregate =
        "SELECT device, avg(temp), max(humidity) FROM bench.plant.readings GROUP BY device";
    let point = "SELECT count(*) FROM bench.plant.readings WHERE status = 3";
    let in_crop = (
        query_ms(&mut client, aggregate, 5).await,
        query_ms(&mut client, point, 5).await,
    );
    let direct_crop = direct_query_ms(&b.node, aggregate, 5).await;
    b.node.flush_crop().await.unwrap();
    let in_comb = (
        query_ms(&mut client, aggregate, 5).await,
        query_ms(&mut client, point, 5).await,
    );
    let direct_comb = direct_query_ms(&b.node, aggregate, 5).await;
    println!(
        "flight query   100,000 rows: aggregate {:.1} ms in the crop, {:.1} ms in the comb; filter {:.1} / {:.1} ms",
        in_crop.0, in_comb.0, in_crop.1, in_comb.1
    );
    println!(
        "direct query   100,000 rows: aggregate {direct_crop:.1} ms in the crop, {direct_comb:.1} ms in the comb"
    );
    server.stop().await;
    b.node.shutdown().await;
    json!({
        "rows": 100_000,
        "aggregate_crop_ms": in_crop.0,
        "aggregate_comb_ms": in_comb.0,
        "filter_crop_ms": in_crop.1,
        "filter_comb_ms": in_comb.1,
        "direct_aggregate_crop_ms": direct_crop,
        "direct_aggregate_comb_ms": direct_comb,
    })
}

fn broker() -> u16 {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let toml = format!(
        r#"
id = 0
[router]
max_connections = 100
max_outgoing_packet_count = 200
max_segment_size = 104857600
max_segment_count = 10
[v4.1]
name = "v4-1"
listen = "127.0.0.1:{port}"
next_connection_delay_ms = 1
[v4.1.connections]
connection_timeout_ms = 60000
max_payload_size = 262144
max_inflight_count = 100
dynamic_filters = true
"#
    );
    let config: rumqttd::Config = config::Config::builder()
        .add_source(config::File::from_str(&toml, config::FileFormat::Toml))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();
    let mut broker = rumqttd::Broker::new(config);
    std::thread::spawn(move || {
        let _ = broker.start();
    });
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return port;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the broker did not start");
}

async fn rows_in(node: &ApiaryNode) -> i64 {
    let out = node
        .sql("SELECT count(*) FROM bench.plant.readings")
        .await
        .unwrap();
    out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn mqtt_section(dir: &Path) -> Value {
    let total_rows = 20_000usize;
    let mut results = Vec::new();
    for rows_per_message in [1usize, 20] {
        let b = node(dir).await;
        let port = broker();
        let running = mqtt::start(
            b.guard.clone(),
            MqttConfig {
                host: "127.0.0.1".into(),
                port,
                client_id: format!("bench-{port}"),
                username: None,
                password: None,
                subscriptions: vec![Subscription {
                    topic: "plant/+/readings".into(),
                    frame: "bench.plant.readings".into(),
                }],
                batch_rows: 1000,
                batch_interval: Duration::from_millis(100),
                idle_flush: Duration::from_millis(10),
            },
        )
        .unwrap();
        let (client, mut events) = AsyncClient::new(
            MqttOptions::new(format!("pub-{port}"), "127.0.0.1", port),
            256,
        );
        tokio::spawn(async move {
            loop {
                if events.poll().await.is_err() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let messages = total_rows / rows_per_message;
        let payloads: Vec<String> = (0..messages)
            .map(|m| json_rows((m * rows_per_message) as i64, rows_per_message))
            .collect();
        let start = Instant::now();
        for payload in &payloads {
            client
                .publish(
                    "plant/a/readings",
                    QoS::AtLeastOnce,
                    false,
                    payload.clone().into_bytes(),
                )
                .await
                .unwrap();
        }
        let published = start.elapsed();
        let mut landed = start.elapsed();
        loop {
            if rows_in(&b.node).await as usize >= total_rows {
                landed = start.elapsed();
                break;
            }
            if start.elapsed() > Duration::from_secs(120) {
                println!("mqtt: gave up with {} rows", rows_in(&b.node).await);
                break;
            }
            // Poll finely: a 20-rows-per-message run lands in about 100 ms, so a
            // coarse poll would round the result to its own step.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let got = rows_in(&b.node).await;
        println!(
            "mqtt ingest    {rows_per_message:>2} rows/message: {got} rows in {:.2} s ({:.0} rows/s, {:.0} messages/s); publishing took {:.2} s",
            landed.as_secs_f64(),
            got as f64 / landed.as_secs_f64(),
            messages as f64 / landed.as_secs_f64(),
            published.as_secs_f64()
        );
        results.push(json!({
            "rows_per_message": rows_per_message,
            "messages": messages,
            "rows_landed": got,
            "seconds": landed.as_secs_f64(),
            "rows_per_sec": got as f64 / landed.as_secs_f64(),
            "messages_per_sec": messages as f64 / landed.as_secs_f64(),
        }));
        running.stop().await;
        b.node.shutdown().await;
    }
    Value::Array(results)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = args
        .iter()
        .position(|a| a == "--out")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let dir = std::env::temp_dir();
    let cores = NodeConfig::detect("local://x").cores;
    println!("Entrance benchmark ({cores} cores, crop sync on)\n");
    let flight_ingest = flight_section(&dir).await;
    println!();
    let flight_query = query_section(&dir).await;
    println!();
    let mqtt_ingest = mqtt_section(&dir).await;

    let report = json!({
        "cores": cores,
        "flight_ingest": flight_ingest,
        "flight_query": flight_query,
        "mqtt_ingest": mqtt_ingest,
    });
    if let Some(path) = out {
        std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap()).unwrap();
        println!("\nwrote {path}");
    }
}
