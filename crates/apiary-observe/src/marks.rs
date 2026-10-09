//! Marked bees: the Node's own account of what it did, read back as a trace.
//!
//! A Node records its deeds (an ingest, a deposit, a cap, a query) as `tracing`
//! events on the `apiary::mark` target. In production that is a log line. Under
//! a [`Sim`](crate::Sim) this layer is installed, and each such event becomes a
//! trace [`Event`](crate::Event) stamped with the virtual time, the Node and, when
//! a Bee did it, the Bee.

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

use crate::trace::Trace;

/// The target Nodes mark their deeds on.
pub const MARK_TARGET: &str = "apiary::mark";

/// A `tracing` layer that copies `apiary::mark` events into a [`Trace`].
pub struct MarkLayer {
    trace: Trace,
}

impl MarkLayer {
    /// A layer that records into `trace`.
    pub fn new(trace: Trace) -> Self {
        Self { trace }
    }
}

#[derive(Default)]
struct Fields {
    node: Option<String>,
    bee: Option<u64>,
    kind: Option<String>,
    detail: String,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "node" => self.node = Some(value.to_string()),
            "kind" => self.kind = Some(value.to_string()),
            "message" => self.detail = value.to_string(),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "bee" {
            self.bee = Some(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `node = %id` and the message arrive here, already formatted.
        let text = format!("{value:?}");
        match field.name() {
            "node" => self.node = Some(text),
            "kind" => self.kind = Some(text),
            "message" => self.detail = text,
            _ => {}
        }
    }
}

impl<S: Subscriber> Layer<S> for MarkLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != MARK_TARGET {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.trace.mark(
            fields.node.as_deref().unwrap_or("?"),
            fields.bee,
            fields.kind.as_deref().unwrap_or("mark"),
            fields.detail,
        );
    }
}
