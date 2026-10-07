//! Ingest benchmark: what the crop costs, and what it buys.
//!
//! Run in release mode, ideally on the hardware (or the container limits) you
//! care about:
//!
//! ```text
//! cargo run --release -p apiary-runtime --example ingest_bench -- --out ingest.json
//! ```
//!
//! It measures, on sensor-shaped rows (timestamp, device, two readings, status):
//!
//! 1. **Ingest** into the crop, per batch size, with the disk sync on and off.
//! 2. **Deposit**: moving a full crop into the comb.
//! 3. **Direct writes** with `write_to_frame`, which commit before returning,
//!    for comparison with ingest.
//! 4. **Query latency** as the crop grows, and after it is deposited. Each query
//!    reads the crop's pending segments into memory, so this is the cost of
//!    leaving data in the crop.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use serde_json::json;

use apiary_core::config::NodeConfig;
use apiary_runtime::ApiaryNode;

const HOUR: Duration = Duration::from_secs(3600);
const SENSOR_SCHEMA: &str = r#"{"ts": "datetime", "device": "string", "temp": "float64", "humidity": "float64", "status": "int64"}"#;

/// `rows` sensor readings numbered from `start`.
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
            Arc::new(Int64Array::from_iter_values(ids().map(|id| id % 3))),
        ),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

/// A node on its own directory, depositing only when told to.
async fn start_node(dir: &Path, sync: bool) -> ApiaryNode {
    let mut config = NodeConfig::detect("local://bench");
    config.storage_uri = format!("local://{}", dir.join("store").display());
    config.cache_dir = dir.join("cache");
    config.deposit_interval = HOUR;
    config.crop_max_bytes = u64::MAX;
    config.crop_sync = sync;
    let node = ApiaryNode::start(config).await.expect("node starts");

    node.registry.create_hive("plant").await.unwrap();
    node.registry.create_box("plant", "line1").await.unwrap();
    node.registry
        .create_frame(
            "plant",
            "line1",
            "readings",
            serde_json::from_str(SENSOR_SCHEMA).unwrap(),
            vec![],
        )
        .await
        .unwrap();
    node
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Latency and throughput of a run of calls.
struct Run {
    calls: usize,
    rows: usize,
    elapsed: Duration,
    p50: Duration,
    p99: Duration,
}

impl Run {
    fn from(mut latencies: Vec<Duration>, rows: usize, elapsed: Duration) -> Self {
        let calls = latencies.len();
        latencies.sort();
        Self {
            calls,
            rows,
            elapsed,
            p50: percentile(&latencies, 0.50),
            p99: percentile(&latencies, 0.99),
        }
    }

    fn rows_per_s(&self) -> f64 {
        self.rows as f64 / self.elapsed.as_secs_f64()
    }

    fn json(&self) -> serde_json::Value {
        json!({
            "calls": self.calls,
            "rows": self.rows,
            "seconds": self.elapsed.as_secs_f64(),
            "rows_per_s": self.rows_per_s(),
            "p50_ms": ms(self.p50),
            "p99_ms": ms(self.p99),
        })
    }
}

async fn ingest_run(node: &ApiaryNode, batch_rows: usize, total_rows: usize) -> Run {
    let batches = total_rows / batch_rows;
    let mut latencies = Vec::with_capacity(batches);
    let started = Instant::now();
    for i in 0..batches {
        let batch = sensor_batch((i * batch_rows) as i64, batch_rows);
        let call = Instant::now();
        node.ingest("plant", "line1", "readings", &batch)
            .await
            .unwrap();
        latencies.push(call.elapsed());
    }
    Run::from(latencies, batches * batch_rows, started.elapsed())
}

async fn write_run(node: &ApiaryNode, batch_rows: usize, total_rows: usize) -> Run {
    let batches = total_rows / batch_rows;
    let mut latencies = Vec::with_capacity(batches);
    let started = Instant::now();
    for i in 0..batches {
        let batch = sensor_batch((i * batch_rows) as i64, batch_rows);
        let call = Instant::now();
        node.write_to_frame("plant", "line1", "readings", &batch)
            .await
            .unwrap();
        latencies.push(call.elapsed());
    }
    Run::from(latencies, batches * batch_rows, started.elapsed())
}

/// Median latency of a query over the frame, in milliseconds.
async fn query_ms(node: &ApiaryNode) -> f64 {
    let sql = "SELECT avg(temp), max(humidity) FROM plant.line1.readings";
    node.sql(sql).await.unwrap(); // warm up
    let mut times = Vec::new();
    for _ in 0..5 {
        let started = Instant::now();
        node.sql(sql).await.unwrap();
        times.push(started.elapsed());
    }
    times.sort();
    ms(times[times.len() / 2])
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = args
        .iter()
        .position(|a| a == "--out")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| "ingest_bench.json".to_string());
    let quick = args.iter().any(|a| a == "--quick");
    let scale = if quick { 10 } else { 1 };

    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    println!("# Ingest benchmark ({cores} cores visible)\n");
    let mut report = json!({ "cores": cores, "quick": quick });

    // ---- 1. ingest into the crop
    println!("## 1. Ingest into the crop\n");
    println!("| batch rows | sync | calls | rows/s | p50 ms | p99 ms |");
    println!("|---:|:---|---:|---:|---:|---:|");
    let mut ingest_results = Vec::new();
    for (batch_rows, total) in [
        (10, 50_000),
        (100, 200_000),
        (1_000, 500_000),
        (10_000, 1_000_000),
    ] {
        for sync in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let node = start_node(dir.path(), sync).await;
            let run = ingest_run(&node, batch_rows, total / scale).await;
            println!(
                "| {batch_rows} | {} | {} | {:.0} | {:.2} | {:.2} |",
                if sync { "on" } else { "off" },
                run.calls,
                run.rows_per_s(),
                ms(run.p50),
                ms(run.p99)
            );
            let mut entry = run.json();
            entry["batch_rows"] = json!(batch_rows);
            entry["sync"] = json!(sync);
            ingest_results.push(entry);
        }
    }
    report["ingest"] = json!(ingest_results);

    // ---- 2. deposit a full crop
    println!("\n## 2. Deposit a full crop into the comb\n");
    {
        let dir = tempfile::tempdir().unwrap();
        let node = start_node(dir.path(), true).await;
        let total = 500_000 / scale;
        ingest_run(&node, 1_000, total).await;
        let started = Instant::now();
        let deposit = node.flush_crop().await.unwrap();
        let took = started.elapsed();
        let rows_per_s = deposit.rows as f64 / took.as_secs_f64();
        println!(
            "{} rows in {} segments deposited in {:.2} s ({:.0} rows/s)",
            deposit.rows,
            deposit.segments,
            took.as_secs_f64(),
            rows_per_s
        );
        report["deposit"] = json!({
            "rows": deposit.rows,
            "segments": deposit.segments,
            "seconds": took.as_secs_f64(),
            "rows_per_s": rows_per_s,
        });
    }

    // ---- 3. direct writes, for comparison
    println!("\n## 3. Direct writes with write_to_frame (commit before returning)\n");
    println!("| batch rows | calls | rows/s | p50 ms | p99 ms |");
    println!("|---:|---:|---:|---:|---:|");
    let mut direct_results = Vec::new();
    for (batch_rows, total) in [
        (10, 5_000),
        (100, 20_000),
        (1_000, 100_000),
        (10_000, 200_000),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let node = start_node(dir.path(), true).await;
        let run = write_run(&node, batch_rows, total / scale).await;
        println!(
            "| {batch_rows} | {} | {:.0} | {:.2} | {:.2} |",
            run.calls,
            run.rows_per_s(),
            ms(run.p50),
            ms(run.p99)
        );
        let mut entry = run.json();
        entry["batch_rows"] = json!(batch_rows);
        direct_results.push(entry);
    }
    report["direct_write"] = json!(direct_results);

    // ---- 4. query latency against the crop
    println!("\n## 4. Query latency: rows left in the crop vs deposited\n");
    println!("| rows | in the crop (ms) | in the comb (ms) |");
    println!("|---:|---:|---:|");
    let mut query_results = Vec::new();
    for rows in [0, 10_000, 100_000, 500_000, 1_000_000] {
        let rows = rows / scale;
        let dir = tempfile::tempdir().unwrap();
        let node = start_node(dir.path(), false).await;
        if rows > 0 {
            ingest_run(&node, 1_000, rows).await;
        }
        let in_crop = query_ms(&node).await;
        node.flush_crop().await.unwrap();
        let in_comb = query_ms(&node).await;
        println!("| {rows} | {in_crop:.1} | {in_comb:.1} |");
        query_results.push(json!({"rows": rows, "crop_ms": in_crop, "comb_ms": in_comb}));
    }
    report["query"] = json!(query_results);

    std::fs::write(&out, serde_json::to_string_pretty(&report).unwrap()).unwrap();
    println!("\nWrote {out}");
}
