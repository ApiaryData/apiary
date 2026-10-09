//! What sits on the dance floor.
//!
//! An [`Entry`] is addressed by its [`EntryKey`] and has one author: the Node
//! (and Bee) named in the key. Only the author publishes under a key, and each
//! publication carries a higher version, so merging two copies is a matter of
//! keeping the higher version. A [`WireEntry`] is the same entry as it travels:
//! its lifetime is what remains of it, not a deadline, because Nodes do not share
//! a clock the floor can trust (a Pi booted without network time has none).

use serde::{Deserialize, Serialize};

use apiary_core::Millis;
use apiary_net::NodeId;

/// What an entry is about. Sampling and listing are by kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EntryKind {
    /// A Node's sign of life and load.
    Presence,
}

/// The address of an entry: one author, one subject.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntryKey {
    /// What kind of entry this is.
    pub kind: EntryKind,
    /// What it is about: empty for a Node's own presence, a query and stage for
    /// work, and so on.
    pub target: String,
    /// The author's Node.
    pub node: NodeId,
    /// The author's Bee, or 0 for entries a Node posts as a whole.
    pub bee: u32,
}

impl EntryKey {
    /// The key of `node`'s presence.
    pub fn presence(node: NodeId) -> Self {
        Self {
            kind: EntryKind::Presence,
            target: String::new(),
            node,
            bee: 0,
        }
    }
}

/// A Node's presence: what the V1 heartbeat file said, on the floor instead.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Presence {
    /// The Node's site label.
    pub site: Option<String>,
    /// Bees on the Node.
    pub bees: u32,
    /// Bees without work.
    pub idle_bees: u32,
    /// The Node's temperature, 0 to 1.
    pub temperature: f64,
    /// The share of the Node's memory pool in use, 0 to 1.
    pub memory_pressure: f64,
    /// The Node's memory pool, in bytes.
    pub memory_bytes: u64,
}

/// What an entry says.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EntryBody {
    /// A Node's presence.
    Presence(Presence),
    /// The author took the entry down. It lingers for a while so an older copy
    /// still travelling the mesh cannot bring it back.
    Withdrawn,
}

/// An entry as a Node holds it. `expires_at` is on this Node's own monotonic
/// clock, in milliseconds, and means nothing to any other Node.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    /// Its address.
    pub key: EntryKey,
    /// Higher wins.
    pub version: u64,
    /// When it lapses, on this Node's monotonic clock.
    pub expires_at: Millis,
    /// What it says.
    pub body: EntryBody,
}

/// An entry on the wire: the version and what remains of its lifetime.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WireEntry {
    /// Its address.
    pub key: EntryKey,
    /// Higher wins.
    pub version: u64,
    /// Milliseconds it has left, as the sender held it.
    pub ttl_ms: u64,
    /// What it says.
    pub body: EntryBody,
}
