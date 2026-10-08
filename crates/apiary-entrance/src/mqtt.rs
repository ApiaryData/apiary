//! The MQTT entrance: a Node subscribes to topics and deposits what arrives.
//!
//! Each subscription maps a topic filter to one Frame. A message is JSON: an
//! object, or an array of objects, whose fields are the Frame's columns. Messages
//! are decoded one at a time, so one bad message cannot spoil the others, then
//! gathered per Frame into batches cut by size or time, and admitted through the
//! [`Guard`].
//!
//! A message is acknowledged to the broker only after its batch has landed in the
//! crop (or has been set aside): until then the broker still holds it, so a Node
//! that dies mid-batch is redelivered what it had not secured. Redelivery means
//! at-least-once, and the crop does not deduplicate; a Frame with a dedup key
//! removes repeats when it ripens.
//!
//! What cannot be admitted is set aside with its reason, never dropped: a
//! message that is not JSON or does not fit the Frame, and a batch the Guard
//! refuses.

use std::collections::HashMap;
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::json::ReaderBuilder;
use arrow::record_batch::RecordBatch;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use apiary_core::{ApiaryError, Result};

use crate::guard::{Admission, Guard, Source};

/// One topic filter and the Frame it feeds.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Subscription {
    /// An MQTT topic filter (`plant/+/temperature`, `plant/#`).
    pub topic: String,
    /// The Frame as `hive.box.frame`.
    pub frame: String,
}

/// How a Node connects to a broker and what it deposits.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MqttConfig {
    /// Broker host.
    pub host: String,
    /// Broker port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// The client id (must be unique at the broker; a fixed one lets the broker
    /// hold this Node's session across restarts).
    pub client_id: String,
    /// Optional broker credentials.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional broker credentials.
    #[serde(default)]
    pub password: Option<String>,
    /// What to subscribe to.
    pub subscriptions: Vec<Subscription>,
    /// A batch is deposited once it holds this many rows...
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    /// ...or once its oldest message has waited this long.
    #[serde(default = "default_batch_interval", with = "duration_millis")]
    pub batch_interval: Duration,
    /// ...or once no message has arrived for this long (0 turns this off).
    ///
    /// A message is acknowledged only after it is deposited, and a broker keeps
    /// only so many messages in flight to a subscriber (Mosquitto 20 by default).
    /// A batch counted in rows may never fill from one window of small messages,
    /// so without this the stream would stall until `batch_interval` every time.
    #[serde(default = "default_idle_flush", with = "duration_millis")]
    pub idle_flush: Duration,
}

fn default_port() -> u16 {
    1883
}

fn default_batch_rows() -> usize {
    1000
}

fn default_batch_interval() -> Duration {
    Duration::from_millis(500)
}

fn default_idle_flush() -> Duration {
    Duration::from_millis(10)
}

mod duration_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        (d.as_millis() as u64).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_millis(u64::deserialize(d)?))
    }
}

/// A Frame's three-part name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FrameName {
    hive: String,
    box_name: String,
    frame: String,
}

impl FrameName {
    fn parse(text: &str) -> Result<Self> {
        let parts: Vec<&str> = text.split('.').collect();
        match parts.as_slice() {
            [hive, box_name, frame] if parts.iter().all(|p| !p.is_empty()) => Ok(Self {
                hive: hive.to_string(),
                box_name: box_name.to_string(),
                frame: frame.to_string(),
            }),
            _ => Err(ApiaryError::Config {
                message: format!("'{text}' is not a frame name; use hive.box.frame"),
            }),
        }
    }
}

impl std::fmt::Display for FrameName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.hive, self.box_name, self.frame)
    }
}

/// Messages decoded and waiting to be deposited, for one Frame.
struct Buffer {
    batches: Vec<RecordBatch>,
    rows: usize,
    /// The messages behind the batches, to acknowledge once they have landed.
    publishes: Vec<Publish>,
    /// The topic of the first message, named as the deposit's source.
    topic: String,
    since: tokio::time::Instant,
}

/// A running MQTT subscriber.
pub struct RunningMqtt {
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl RunningMqtt {
    /// Deposit what is buffered, disconnect and wait for the subscriber to end.
    pub async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

/// Start subscribing. Fails if a subscription's frame name is malformed; a
/// broker that is down is retried, not an error.
pub fn start(guard: Guard, config: MqttConfig) -> Result<RunningMqtt> {
    let mut routes = Vec::new();
    for sub in &config.subscriptions {
        routes.push((sub.topic.clone(), FrameName::parse(&sub.frame)?));
    }
    if routes.is_empty() {
        return Err(ApiaryError::Config {
            message: "The MQTT entrance has no subscriptions".into(),
        });
    }

    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(run(guard, config, routes, stopped));
    Ok(RunningMqtt { stop, task })
}

async fn run(
    guard: Guard,
    config: MqttConfig,
    routes: Vec<(String, FrameName)>,
    mut stopped: oneshot::Receiver<()>,
) {
    let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
    options.set_keep_alive(Duration::from_secs(30));
    options.set_manual_acks(true);
    options.set_clean_session(false);
    if let (Some(user), Some(pass)) = (&config.username, &config.password) {
        options.set_credentials(user, pass);
    }
    let (client, mut eventloop) = AsyncClient::new(options, 256);

    let mut buffers: HashMap<FrameName, Buffer> = HashMap::new();
    let mut schemas: HashMap<FrameName, SchemaRef> = HashMap::new();
    let mut tick = tokio::time::interval(config.batch_interval.max(Duration::from_millis(10)) / 2);
    // When the last message arrived, for the idle flush.
    let mut last_message = tokio::time::Instant::now();
    let far_future = Duration::from_secs(86_400 * 365);

    loop {
        tokio::select! {
            _ = &mut stopped => break,
            _ = tokio::time::sleep_until(
                if config.idle_flush.is_zero() || buffers.is_empty() {
                    tokio::time::Instant::now() + far_future
                } else {
                    last_message + config.idle_flush
                }
            ) => {
                // The stream paused: deposit what has arrived.
                let keys: Vec<FrameName> = buffers.keys().cloned().collect();
                for key in keys {
                    deposit(&guard, &client, &key, &mut buffers).await;
                }
                last_message = tokio::time::Instant::now();
            }
            _ = tick.tick() => {
                let due: Vec<FrameName> = buffers
                    .iter()
                    .filter(|(_, b)| b.since.elapsed() >= config.batch_interval)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in due {
                    deposit(&guard, &client, &key, &mut buffers).await;
                }
            }
            event = eventloop.poll() => match event {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!(host = %config.host, port = config.port, "Connected to the MQTT broker");
                    for (topic, _) in &routes {
                        if let Err(e) = client.subscribe(topic, QoS::AtLeastOnce).await {
                            warn!(%topic, error = %e, "Failed to subscribe");
                        }
                    }
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    last_message = tokio::time::Instant::now();
                    receive(&guard, &client, &routes, &config, &mut schemas, &mut buffers, publish).await;
                }
                Ok(_) => {}
                Err(e) => {
                    // The event loop reconnects when polled again.
                    warn!(error = %e, "MQTT connection error; retrying");
                    tokio::select! {
                        _ = &mut stopped => break,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
    }

    // Secure what is buffered, then leave.
    let keys: Vec<FrameName> = buffers.keys().cloned().collect();
    for key in keys {
        deposit(&guard, &client, &key, &mut buffers).await;
    }
    let _ = client.disconnect().await;
}

/// Handle one incoming message.
async fn receive(
    guard: &Guard,
    client: &AsyncClient,
    routes: &[(String, FrameName)],
    config: &MqttConfig,
    schemas: &mut HashMap<FrameName, SchemaRef>,
    buffers: &mut HashMap<FrameName, Buffer>,
    publish: Publish,
) {
    let Some((_, frame)) = routes
        .iter()
        .find(|(filter, _)| rumqttc::matches(&publish.topic, filter))
    else {
        // Not ours (a stray retained message); nothing to keep.
        let _ = client.ack(&publish).await;
        return;
    };
    let frame = frame.clone();

    let schema = match schemas.get(&frame) {
        Some(schema) => schema.clone(),
        None => match guard
            .node()
            .frame_schema(&frame.hive, &frame.box_name, &frame.frame)
            .await
        {
            Ok(schema) => {
                schemas.insert(frame.clone(), schema.clone());
                schema
            }
            Err(e) => {
                // The Frame does not exist (yet): keep the message aside
                // rather than lose it.
                warn!(frame = %frame, error = %e, "Cannot deposit: no such frame");
                aside_raw(guard, &frame, &publish, &e.to_string());
                let _ = client.ack(&publish).await;
                return;
            }
        },
    };

    match decode(&schema, &publish.payload) {
        Ok(batch) => {
            let buffer = buffers.entry(frame.clone()).or_insert_with(|| Buffer {
                batches: Vec::new(),
                rows: 0,
                publishes: Vec::new(),
                topic: publish.topic.clone(),
                since: tokio::time::Instant::now(),
            });
            buffer.rows += batch.num_rows();
            buffer.batches.push(batch);
            buffer.publishes.push(publish);
            if buffer.rows >= config.batch_rows {
                deposit(guard, client, &frame, buffers).await;
            }
        }
        Err(reason) => {
            debug!(frame = %frame, %reason, "Message set aside");
            aside_raw(guard, &frame, &publish, &reason);
            let _ = client.ack(&publish).await;
        }
    }
}

fn aside_raw(guard: &Guard, frame: &FrameName, publish: &Publish, reason: &str) {
    if let Err(e) = guard.set_aside_raw(
        &frame.hive,
        &frame.box_name,
        &frame.frame,
        &format!("mqtt {}", publish.topic),
        reason,
        &publish.payload,
    ) {
        warn!(error = %e, "Failed to set a message aside");
    }
}

/// Deposit a Frame's buffered batches through the Guard, then acknowledge the
/// messages. A failure other than a refusal leaves them buffered, unacknowledged,
/// to be tried again.
async fn deposit(
    guard: &Guard,
    client: &AsyncClient,
    frame: &FrameName,
    buffers: &mut HashMap<FrameName, Buffer>,
) {
    let Some(buffer) = buffers.get(frame) else {
        return;
    };
    let Some(first) = buffer.batches.first() else {
        buffers.remove(frame);
        return;
    };
    let merged = match concat_batches(&first.schema(), &buffer.batches) {
        Ok(batch) => batch,
        Err(e) => {
            warn!(frame = %frame, error = %e, "Failed to merge a batch");
            return;
        }
    };

    let source = Source::Stream(format!("mqtt {}", buffer.topic));
    match guard
        .admit(&frame.hive, &frame.box_name, &frame.frame, &merged, &source)
        .await
    {
        Ok(admission) => {
            if let Admission::Landed(result) = &admission {
                debug!(frame = %frame, rows = result.rows, "Deposited a batch from MQTT");
            }
            if let Some(buffer) = buffers.remove(frame) {
                for publish in &buffer.publishes {
                    let _ = client.ack(publish).await;
                }
            }
        }
        Err(e) => {
            warn!(frame = %frame, error = %e, "Deposit failed; will retry");
            // Retry on the next tick rather than spinning.
            if let Some(buffer) = buffers.get_mut(frame) {
                buffer.since = tokio::time::Instant::now();
            }
        }
    }
}

/// Decode one message into a batch of the Frame's schema.
fn decode(schema: &SchemaRef, payload: &[u8]) -> std::result::Result<RecordBatch, String> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| format!("The message is not JSON: {e}"))?;
    let rows = match value {
        serde_json::Value::Object(_) => vec![value],
        serde_json::Value::Array(items) if items.iter().all(serde_json::Value::is_object) => items,
        _ => {
            return Err("The message must be a JSON object or an array of objects".to_string());
        }
    };
    if rows.is_empty() {
        return Err("The message holds no rows".to_string());
    }

    let mut decoder = ReaderBuilder::new(schema.clone())
        .with_strict_mode(true)
        .build_decoder()
        .map_err(|e| format!("Cannot decode against the frame: {e}"))?;
    decoder
        .serialize(&rows)
        .map_err(|e| format!("The message does not fit the frame: {e}"))?;
    decoder
        .flush()
        .map_err(|e| format!("The message does not fit the frame: {e}"))?
        .ok_or_else(|| "The message holds no rows".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("temp", DataType::Float64, true),
        ]))
    }

    #[test]
    fn an_object_and_an_array_both_decode() {
        let one = decode(&schema(), br#"{"id": 1, "temp": 20.5}"#).unwrap();
        assert_eq!(one.num_rows(), 1);

        let many = decode(&schema(), br#"[{"id": 1}, {"id": 2, "temp": 3.0}]"#).unwrap();
        assert_eq!(many.num_rows(), 2);
        let temp = many
            .column_by_name("temp")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(temp.is_null(0), "a field a message omits is null");
        let id = many
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.value(1), 2);
    }

    #[test]
    fn bad_messages_say_why() {
        for (payload, wanted) in [
            (&b"{oops"[..], "not JSON"),
            (&b"42"[..], "object or an array"),
            (&b"[1, 2]"[..], "object or an array"),
            (&b"[]"[..], "no rows"),
            (&br#"{"id": 1, "humidity": 0.4}"#[..], "does not fit"),
            (&br#"{"id": "abc"}"#[..], "does not fit"),
        ] {
            let why = decode(&schema(), payload).unwrap_err();
            assert!(why.contains(wanted), "{payload:?}: {why}");
        }
    }

    #[test]
    fn frame_names_need_three_parts() {
        assert!(FrameName::parse("h.b.f").is_ok());
        assert!(FrameName::parse("h.b").is_err());
        assert!(FrameName::parse("h..f").is_err());
        assert!(FrameName::parse("a.b.c.d").is_err());
    }
}
