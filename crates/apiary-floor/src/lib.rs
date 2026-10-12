//! The dance floor: entries with versions and lifetimes, shared between a
//! colony's Nodes and merged as a state-based CRDT.
//!
//! - [`entry`]: what an entry is, and how it travels
//! - [`state`]: the merge rules, and one Node's copy ([`InMemoryFloor`], the whole
//!   floor of a solitary Node)
//! - [`gossip`]: [`GossipFloor`], the same copy kept in step with the colony's
//!   other Nodes over the gossip protocol
//!
//! Nodes read only their own copy. Nothing on the floor is asked of anyone, so a
//! Node that cannot reach its peers still has a floor, and it is a little out of
//! date rather than unavailable.

use std::time::Duration;

use apiary_core::SeededRng;

pub mod entry;
pub mod gossip;
pub mod state;

pub use entry::{
    ClaimState, Dance, Entry, EntryBody, EntryKey, EntryKind, Presence, SoftClaim, WireEntry,
};
pub use gossip::{GossipConfig, GossipCounts, GossipFloor};
pub use state::{InMemoryFloor, MAX_TTL};

/// A Node's floor, however it is kept in step with the others.
pub trait Floor: Send + Sync + 'static {
    /// Post `body` under `key` for `ttl` (at most [`MAX_TTL`]); returns its version.
    fn publish(&self, key: EntryKey, body: EntryBody, ttl: Duration) -> u64;

    /// Take `key` down.
    fn withdraw(&self, key: &EntryKey);

    /// The live entry under `key`.
    fn get(&self, key: &EntryKey) -> Option<Entry>;

    /// Every live entry of `kind`.
    fn entries(&self, kind: EntryKind) -> Vec<Entry>;

    /// Every live entry of `kind` whose target starts with `prefix`.
    fn entries_for(&self, kind: EntryKind, prefix: &str) -> Vec<Entry>;

    /// Now, on the clock entries' lifetimes are measured against, in milliseconds.
    fn now(&self) -> apiary_core::Millis;

    /// Signalled whenever this Node's copy changes, by a post or by news from a
    /// peer, so a Node can wake its Bees to look.
    fn changed(&self) -> &tokio::sync::Notify;

    /// Up to `k` live entries of `kind` at random, from this Node's copy only.
    fn sample(&self, kind: EntryKind, k: usize, rng: &mut dyn SeededRng) -> Vec<Entry>;
}

impl Floor for InMemoryFloor {
    fn publish(&self, key: EntryKey, body: EntryBody, ttl: Duration) -> u64 {
        InMemoryFloor::publish(self, key, body, ttl)
    }

    fn withdraw(&self, key: &EntryKey) {
        InMemoryFloor::withdraw(self, key);
    }

    fn get(&self, key: &EntryKey) -> Option<Entry> {
        InMemoryFloor::get(self, key)
    }

    fn entries(&self, kind: EntryKind) -> Vec<Entry> {
        InMemoryFloor::entries(self, kind)
    }

    fn entries_for(&self, kind: EntryKind, prefix: &str) -> Vec<Entry> {
        InMemoryFloor::entries_for(self, kind, prefix)
    }

    fn now(&self) -> apiary_core::Millis {
        InMemoryFloor::now(self)
    }

    fn changed(&self) -> &tokio::sync::Notify {
        InMemoryFloor::changed(self)
    }

    fn sample(&self, kind: EntryKind, k: usize, rng: &mut dyn SeededRng) -> Vec<Entry> {
        InMemoryFloor::sample(self, kind, k, rng)
    }
}
