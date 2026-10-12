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
    /// A Bee's advertisement of work worth doing.
    Dance,
    /// A Bee's claim on one Patch.
    SoftClaim,
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

    /// The key of the dance `bee` of `node` performs for a stage's Patches.
    pub fn dance(query: &str, stage: usize, node: NodeId, bee: u32) -> Self {
        Self {
            kind: EntryKind::Dance,
            target: format!("{query}/{stage}"),
            node,
            bee,
        }
    }

    /// The key of the claim `bee` of `node` makes on one Patch.
    pub fn claim(query: &str, stage: usize, patch: usize, node: NodeId, bee: u32) -> Self {
        Self {
            kind: EntryKind::SoftClaim,
            target: format!("{query}/{stage}/{patch}"),
            node,
            bee,
        }
    }
}

/// An advertisement of a stage's Patches, as a waggle dance advertises a patch of
/// flowers. How strongly it is advertised is how long it has left to live: the
/// entry's lifetime is set so that it lapses when the dance has run its circuits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dance {
    /// The query.
    pub query: String,
    /// The stage whose Patches are advertised.
    pub stage: usize,
    /// How many Patches the stage has.
    pub patches: usize,
    /// Circuits at the last posting, before any decay.
    pub circuits: f64,
    /// Patches the dancer has finished from this group, which each take a circuit off.
    pub renewals: u32,
    /// If the stage's input is held by one Node, that Node: no other can run it.
    pub pinned: Option<NodeId>,
    /// Patches a Scout has seen run well past the group's median, which any Bee
    /// following this dance may run again even though they are claimed. Whichever
    /// attempt completes first is committed.
    pub redo: Vec<usize>,
}

/// Where a claim stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimState {
    /// A Bee is running the Patch.
    Running,
    /// The Patch is complete (the comb store has the final word).
    Done,
}

/// A Bee's claim on one Patch. Another Bee on another Node may claim it before
/// gossip tells her this one has; the Patch's completion record settles that.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SoftClaim {
    /// Whether the Patch is being run or is done.
    pub state: ClaimState,
    /// How long the claimant has been running the Patch, or took to finish it, in
    /// milliseconds. A Scout compares it with the group's median to find stragglers.
    pub elapsed_ms: u64,
    /// How many Patches the group has, so a Scout can advertise the group again
    /// without having read the plan.
    pub patches: usize,
    /// The Node that alone can run the group, if there is one.
    pub pinned: Option<NodeId>,
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
    /// An advertisement of work.
    Dance(Dance),
    /// A claim on a Patch.
    SoftClaim(SoftClaim),
    /// The author took the entry down. It lingers for a while so an older copy
    /// still travelling the mesh cannot bring it back.
    Withdrawn,
}

impl Entry {
    /// How long the entry has left to live, as of `now` on this Node's monotonic
    /// clock, in milliseconds.
    pub fn remaining(&self, now: Millis) -> Millis {
        self.expires_at.saturating_sub(now)
    }
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
