//! The MQTT entrance, against an embedded broker.

mod common;

use std::net::TcpListener;
use std::time::Duration;

use rumqttc::{AsyncClient, MqttOptions, QoS};

use apiary_entrance::mqtt::{self, MqttConfig, Subscription};
use common::{Fixture, fixture, rows};

/// Start an in-process broker on a free port.
fn broker() -> u16 {
    let port = {
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap().port()
    };
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
max_payload_size = 20480
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
    // Wait until it accepts connections.
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return port;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the broker did not start");
}

fn config(port: u16, batch_rows: usize, batch_interval: Duration) -> MqttConfig {
    MqttConfig {
        host: "127.0.0.1".into(),
        port,
        client_id: format!("apiary-test-{port}"),
        username: None,
        password: None,
        subscriptions: vec![Subscription {
            topic: "plant/+/readings".into(),
            frame: "farm.field.readings".into(),
        }],
        batch_rows,
        batch_interval,
    }
}

/// A publisher connected to the broker, with its event loop running.
async fn publisher(port: u16) -> AsyncClient {
    let (client, mut events) = AsyncClient::new(
        MqttOptions::new(format!("publisher-{port}"), "127.0.0.1", port),
        64,
    );
    tokio::spawn(async move {
        loop {
            if events.poll().await.is_err() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });
    client
}

/// Publish once the subscriber is connected and subscribed.
async fn publish(client: &AsyncClient, topic: &str, payload: &str) {
    client
        .publish(topic, QoS::AtLeastOnce, false, payload.as_bytes().to_vec())
        .await
        .unwrap();
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..200 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// Give the subscriber time to connect and subscribe before publishing.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

async fn setup(
    batch_rows: usize,
    batch_interval: Duration,
) -> (Fixture, mqtt::RunningMqtt, AsyncClient) {
    let f = fixture().await;
    let port = broker();
    let running = mqtt::start(f.guard.clone(), config(port, batch_rows, batch_interval)).unwrap();
    let client = publisher(port).await;
    settle().await;
    (f, running, client)
}

#[tokio::test]
async fn messages_become_rows_in_the_crop() {
    let (f, running, client) = setup(1000, Duration::from_millis(100)).await;

    publish(&client, "plant/a/readings", r#"{"id": 1, "temp": 20.5}"#).await;
    publish(
        &client,
        "plant/b/readings",
        r#"[{"id": 2}, {"id": 3, "temp": 18.0}]"#,
    )
    .await;

    eventually("three rows to land", || async { rows(&f.node).await == 3 }).await;
    let stage = f
        .node
        .sql("SELECT count(id) FROM farm.field.readings WHERE _stage = 'crop'")
        .await
        .unwrap();
    assert_eq!(stage[0].num_rows(), 1);
    assert!(f.guard.set_aside().list().unwrap().is_empty());

    running.stop().await;
}

#[tokio::test]
async fn what_cannot_be_admitted_is_set_aside_with_a_reason() {
    let (f, running, client) = setup(1000, Duration::from_millis(100)).await;

    publish(&client, "plant/a/readings", "{oops").await;
    publish(&client, "plant/a/readings", r#"{"id": 1, "humidity": 0.4}"#).await;
    publish(&client, "plant/a/readings", r#"{"id": 7, "temp": 1.0}"#).await;

    eventually("the good row to land", || async {
        rows(&f.node).await == 1
    })
    .await;
    eventually("both bad messages to be set aside", || async {
        f.guard.set_aside().list().unwrap().len() == 2
    })
    .await;

    let reasons: Vec<String> = f
        .guard
        .set_aside()
        .list()
        .unwrap()
        .into_iter()
        .map(|r| r.reason)
        .collect();
    assert!(
        reasons.iter().any(|r| r.contains("not JSON")),
        "{reasons:?}"
    );
    assert!(
        reasons.iter().any(|r| r.contains("does not fit")),
        "{reasons:?}"
    );

    let first = f.guard.set_aside().list().unwrap().remove(0);
    assert_eq!(first.frame, "farm.field.readings");
    assert!(first.source.starts_with("mqtt plant/a/readings"));
    assert_eq!(f.guard.set_aside().payload(&first).unwrap(), b"{oops");

    running.stop().await;
}

#[tokio::test]
async fn a_batch_is_deposited_when_it_is_full_without_waiting() {
    // An hour-long interval: only the row count can trigger the deposit.
    let (f, running, client) = setup(3, Duration::from_secs(3600)).await;

    publish(&client, "plant/a/readings", r#"{"id": 1}"#).await;
    publish(&client, "plant/a/readings", r#"{"id": 2}"#).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(rows(&f.node).await, 0, "two rows are not a full batch");

    publish(&client, "plant/a/readings", r#"{"id": 3}"#).await;
    eventually("the full batch to land", || async {
        rows(&f.node).await == 3
    })
    .await;

    running.stop().await;
}

#[tokio::test]
async fn stopping_deposits_what_is_buffered() {
    let (f, running, client) = setup(1000, Duration::from_secs(3600)).await;

    publish(&client, "plant/a/readings", r#"{"id": 1}"#).await;
    publish(&client, "plant/a/readings", r#"{"id": 2}"#).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(rows(&f.node).await, 0);

    running.stop().await;
    assert_eq!(rows(&f.node).await, 2, "nothing buffered is lost on stop");
}

#[tokio::test]
async fn topics_outside_the_subscription_are_ignored() {
    let (f, running, client) = setup(1000, Duration::from_millis(100)).await;

    publish(&client, "plant/a/other", r#"{"id": 1}"#).await;
    publish(&client, "plant/a/readings", r#"{"id": 2}"#).await;

    eventually("the subscribed row to land", || async {
        rows(&f.node).await == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(rows(&f.node).await, 1);

    running.stop().await;
}

#[test]
fn a_bad_subscription_is_refused_at_start() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let f = fixture().await;
        let mut bad = config(1, 10, Duration::from_millis(100));
        bad.subscriptions[0].frame = "not-a-frame".into();
        assert!(mqtt::start(f.guard.clone(), bad).is_err());

        let mut none = config(1, 10, Duration::from_millis(100));
        none.subscriptions.clear();
        assert!(mqtt::start(f.guard.clone(), none).is_err());
    });
}
