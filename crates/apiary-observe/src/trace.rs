//! The marked bees: a record of what happened, with who did it and when.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apiary_core::Clock;

/// One thing that happened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// Virtual time since the run began.
    pub at: Duration,
    /// The Node (or `store:<name>`, `net`) it happened in.
    pub node: String,
    /// The Bee, when a Bee did it.
    pub bee: Option<u64>,
    /// What kind of thing: `store.put`, `commit`, `net.drop`, ...
    pub kind: String,
    /// The particulars.
    pub detail: String,
}

/// An ordered record of a run. Cheap to clone; clones share one record.
#[derive(Clone)]
pub struct Trace {
    events: Arc<Mutex<Vec<Event>>>,
    clock: Arc<dyn Clock>,
}

impl Trace {
    /// An empty trace stamped by `clock`.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            events: Arc::default(),
            clock,
        }
    }

    /// Record an event now.
    pub fn mark(&self, node: &str, bee: Option<u64>, kind: &str, detail: impl Into<String>) {
        let event = Event {
            at: self.clock.monotonic(),
            node: node.to_string(),
            bee,
            kind: kind.to_string(),
            detail: detail.into(),
        };
        self.events.lock().expect("trace lock").push(event);
    }

    /// Everything recorded so far.
    pub fn events(&self) -> Vec<Event> {
        self.events.lock().expect("trace lock").clone()
    }

    /// Events of one kind (a prefix match, so `store` finds `store.put`).
    pub fn of_kind(&self, prefix: &str) -> Vec<Event> {
        self.events()
            .into_iter()
            .filter(|e| e.kind.starts_with(prefix))
            .collect()
    }

    /// How many events there are.
    pub fn len(&self) -> usize {
        self.events.lock().expect("trace lock").len()
    }

    /// Whether nothing has been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A fingerprint of the whole run. Equal for runs that did the same things in
    /// the same order at the same virtual times.
    pub fn digest(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut eat = |bytes: &[u8]| {
            for b in bytes {
                h ^= u64::from(*b);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
            // A separator, so ("ab", "c") and ("a", "bc") differ.
            h ^= 0xff;
            h = h.wrapping_mul(0x0100_0000_01b3);
        };
        for e in self.events.lock().expect("trace lock").iter() {
            eat(&e.at.as_nanos().to_le_bytes());
            eat(e.node.as_bytes());
            eat(&e.bee.unwrap_or(u64::MAX).to_le_bytes());
            eat(e.kind.as_bytes());
            eat(e.detail.as_bytes());
        }
        h
    }

    /// The trace as text, one event per line, for a failing test to print.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for e in self.events.lock().expect("trace lock").iter() {
            let bee = e.bee.map_or(String::new(), |b| format!("/bee{b}"));
            let _ = writeln!(
                out,
                "{:>10.3}s {}{bee} {} {}",
                e.at.as_secs_f64(),
                e.node,
                e.kind,
                e.detail
            );
        }
        out
    }
}

impl std::fmt::Debug for Trace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Trace({} events)", self.len())
    }
}
