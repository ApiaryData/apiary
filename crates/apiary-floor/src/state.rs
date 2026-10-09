//! The merge rules, and the store that holds one Node's copy of the floor.
//!
//! The floor is a state-based CRDT: merging keeps, per key, the entry with the
//! highest version, and anything past its lifetime counts as absent. Merging is
//! commutative, associative and idempotent, so entries can arrive in any order,
//! twice, or by way of several Nodes, and every copy converges.
//!
//! Two rules keep that honest without a shared clock:
//!
//! - A lifetime travels as what remains of it and becomes a deadline on the
//!   receiver's monotonic clock, so a Node with the wrong wall clock reads the
//!   floor the same as any other.
//! - An author that restarts publishes above whatever version of its key it can
//!   still see, because its old entries come back to it from its peers.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use apiary_core::{Clock, Millis, SeededRng};

use crate::entry::{Entry, EntryBody, EntryKey, EntryKind, WireEntry};

/// The longest an entry may live. A withdrawn entry's tombstone lives this long
/// too, which is what stops an older copy of it coming back.
pub const MAX_TTL: Duration = Duration::from_secs(60);

struct Slot {
    entry: Entry,
    /// When this Node last changed the slot, for gossip to send only what is new.
    seq: u64,
}

struct State {
    slots: BTreeMap<EntryKey, Slot>,
    seq: u64,
}

/// One Node's copy of the floor.
pub struct InMemoryFloor {
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
}

impl InMemoryFloor {
    /// An empty floor that tells time by `clock`.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            state: Mutex::new(State {
                slots: BTreeMap::new(),
                seq: 0,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("floor poisoned")
    }

    fn now(&self) -> Millis {
        self.clock.monotonic().as_millis() as Millis
    }

    /// Post `body` under `key`, replacing what this Node holds for it, for `ttl`
    /// (at most [`MAX_TTL`]). Returns the version it took.
    pub fn publish(&self, key: EntryKey, body: EntryBody, ttl: Duration) -> u64 {
        let now = self.now();
        let ttl = ttl.min(MAX_TTL).as_millis() as Millis;
        let mut state = self.lock();
        let version = state.slots.get(&key).map_or(1, |s| s.entry.version + 1);
        state.seq += 1;
        let seq = state.seq;
        state.slots.insert(
            key.clone(),
            Slot {
                entry: Entry {
                    key,
                    version,
                    expires_at: now + ttl,
                    body,
                },
                seq,
            },
        );
        version
    }

    /// Take `key` down: its entry becomes a tombstone for [`MAX_TTL`].
    pub fn withdraw(&self, key: &EntryKey) {
        if self.get(key).is_some() {
            self.publish(key.clone(), EntryBody::Withdrawn, MAX_TTL);
        }
    }

    /// The live entry under `key`, if there is one.
    pub fn get(&self, key: &EntryKey) -> Option<Entry> {
        let now = self.now();
        let state = self.lock();
        state
            .slots
            .get(key)
            .map(|s| &s.entry)
            .filter(|e| is_live(e, now))
            .cloned()
    }

    /// Every live entry of `kind`, in key order.
    pub fn entries(&self, kind: EntryKind) -> Vec<Entry> {
        let now = self.now();
        let state = self.lock();
        state
            .slots
            .values()
            .map(|s| &s.entry)
            .filter(|e| e.key.kind == kind && is_live(e, now))
            .cloned()
            .collect()
    }

    /// Up to `k` live entries of `kind`, chosen at random without replacement.
    /// This reads only this Node's copy: no one is asked.
    pub fn sample(&self, kind: EntryKind, k: usize, rng: &mut dyn SeededRng) -> Vec<Entry> {
        let mut all = self.entries(kind);
        let mut picked = Vec::with_capacity(k.min(all.len()));
        while picked.len() < k && !all.is_empty() {
            let i = rng.next_below(all.len() as u64) as usize;
            picked.push(all.swap_remove(i));
        }
        picked
    }

    /// Merge entries from a peer. Returns how many changed this copy.
    pub fn merge(&self, incoming: Vec<WireEntry>) -> usize {
        let now = self.now();
        let mut state = self.lock();
        let mut changed = 0;
        for wire in incoming {
            if wire.ttl_ms == 0 {
                continue;
            }
            let expires_at = now + wire.ttl_ms.min(MAX_TTL.as_millis() as Millis);
            let entry = Entry {
                key: wire.key,
                version: wire.version,
                expires_at,
                body: wire.body,
            };
            let wins = match state.slots.get_mut(&entry.key) {
                Some(slot) if slot.entry.expires_at > now => {
                    match entry.version.cmp(&slot.entry.version) {
                        std::cmp::Ordering::Greater => true,
                        std::cmp::Ordering::Less => false,
                        std::cmp::Ordering::Equal => {
                            // The same version with different bodies means a key
                            // had two authors. Pick the same one everywhere.
                            if entry.body != slot.entry.body && tie_break(&entry, &slot.entry) {
                                true
                            } else {
                                // The same entry again: remember the later lapse
                                // but do not tell anyone, or two Nodes would
                                // trade it for ever.
                                slot.entry.expires_at = slot.entry.expires_at.max(expires_at);
                                false
                            }
                        }
                    }
                }
                _ => true,
            };
            if wins {
                state.seq += 1;
                let seq = state.seq;
                state.slots.insert(entry.key.clone(), Slot { entry, seq });
                changed += 1;
            }
        }
        changed
    }

    /// Drop everything that has lapsed.
    pub fn sweep(&self) {
        let now = self.now();
        self.lock().slots.retain(|_, s| s.entry.expires_at > now);
    }

    /// The sequence number of the latest change to this copy.
    pub fn seq(&self) -> u64 {
        self.lock().seq
    }

    /// Entries changed after `seq`, for sending, and the sequence number they are
    /// current to.
    pub fn changed_since(&self, seq: u64) -> (Vec<WireEntry>, u64) {
        let now = self.now();
        let state = self.lock();
        let entries = state
            .slots
            .values()
            .filter(|s| s.seq > seq)
            .filter_map(|s| to_wire(&s.entry, now))
            .collect();
        (entries, state.seq)
    }

    /// Every key held and its version, for a peer to compare against.
    pub fn digest(&self) -> Vec<(EntryKey, u64)> {
        let now = self.now();
        let state = self.lock();
        state
            .slots
            .values()
            .filter(|s| s.entry.expires_at > now)
            .map(|s| (s.entry.key.clone(), s.entry.version))
            .collect()
    }

    /// The entries held that are newer than a peer's `digest` says it has.
    pub fn newer_than(&self, digest: &[(EntryKey, u64)]) -> Vec<WireEntry> {
        let now = self.now();
        let theirs: BTreeMap<&EntryKey, u64> = digest.iter().map(|(k, v)| (k, *v)).collect();
        let state = self.lock();
        state
            .slots
            .values()
            .filter(|s| {
                theirs
                    .get(&s.entry.key)
                    .is_none_or(|v| s.entry.version > *v)
            })
            .filter_map(|s| to_wire(&s.entry, now))
            .collect()
    }
}

fn is_live(entry: &Entry, now: Millis) -> bool {
    entry.expires_at > now && !matches!(entry.body, EntryBody::Withdrawn)
}

fn to_wire(entry: &Entry, now: Millis) -> Option<WireEntry> {
    // A tombstone is sent like any entry: that is its job.
    (entry.expires_at > now).then(|| WireEntry {
        key: entry.key.clone(),
        version: entry.version,
        ttl_ms: entry.expires_at - now,
        body: entry.body.clone(),
    })
}

/// Whether `a` beats `b` when their versions are equal and bodies differ: the
/// larger serialisation wins, the same on every Node.
fn tie_break(a: &Entry, b: &Entry) -> bool {
    let a = serde_json::to_vec(&a.body).unwrap_or_default();
    let b = serde_json::to_vec(&b.body).unwrap_or_default();
    a > b
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::Presence;
    use apiary_core::ManualClock;
    use apiary_net::NodeKey;

    fn node(n: u8) -> apiary_net::NodeId {
        NodeKey::from_bytes(&[n; 32]).id()
    }

    fn presence(bees: u32) -> EntryBody {
        EntryBody::Presence(Presence {
            site: None,
            bees,
            idle_bees: 0,
            temperature: 0.0,
            memory_pressure: 0.0,
            memory_bytes: 0,
        })
    }

    fn clock() -> Arc<ManualClock> {
        Arc::new(ManualClock::new(chrono::Utc::now()))
    }

    fn all(floor: &InMemoryFloor) -> Vec<WireEntry> {
        floor.changed_since(0).0
    }

    #[test]
    fn higher_versions_win_in_any_order() {
        let key = EntryKey::presence(node(1));
        let wire = |version, bees| WireEntry {
            key: key.clone(),
            version,
            ttl_ms: 10_000,
            body: presence(bees),
        };
        let forward = InMemoryFloor::new(clock());
        forward.merge(vec![wire(1, 1), wire(2, 2), wire(3, 3)]);
        let backward = InMemoryFloor::new(clock());
        backward.merge(vec![wire(3, 3), wire(2, 2), wire(1, 1)]);
        for floor in [&forward, &backward] {
            let e = floor.get(&key).expect("held");
            assert_eq!((e.version, e.body), (3, presence(3)));
        }
    }

    #[test]
    fn merging_twice_changes_nothing_the_second_time() {
        let a = InMemoryFloor::new(clock());
        a.publish(
            EntryKey::presence(node(1)),
            presence(4),
            Duration::from_secs(5),
        );
        let b = InMemoryFloor::new(clock());
        assert_eq!(b.merge(all(&a)), 1);
        let seq = b.seq();
        assert_eq!(b.merge(all(&a)), 0);
        assert_eq!(b.seq(), seq, "a repeat is not news, so it is not re-sent");
    }

    #[test]
    fn equal_versions_with_different_bodies_settle_the_same_way_everywhere() {
        let key = EntryKey::presence(node(1));
        let wire = |bees| WireEntry {
            key: key.clone(),
            version: 7,
            ttl_ms: 10_000,
            body: presence(bees),
        };
        let one = InMemoryFloor::new(clock());
        one.merge(vec![wire(1), wire(2)]);
        let two = InMemoryFloor::new(clock());
        two.merge(vec![wire(2), wire(1)]);
        assert_eq!(one.get(&key).unwrap().body, two.get(&key).unwrap().body);
    }

    #[test]
    fn entries_lapse_by_their_remaining_lifetime() {
        let clock = clock();
        let a = InMemoryFloor::new(clock.clone());
        let key = EntryKey::presence(node(1));
        a.publish(key.clone(), presence(1), Duration::from_secs(10));
        clock.advance(Duration::from_secs(6));

        // Sent with four seconds left, it lapses four seconds after arrival,
        // whatever the receiver's own clock reads.
        let late = ManualClock::new(chrono::Utc::now());
        late.advance(Duration::from_secs(1_000));
        let late = Arc::new(late);
        let b = InMemoryFloor::new(late.clone());
        b.merge(all(&a));
        assert!(b.get(&key).is_some());
        late.advance(Duration::from_secs(3));
        assert!(b.get(&key).is_some());
        late.advance(Duration::from_secs(2));
        assert!(b.get(&key).is_none());
        assert!(b.entries(EntryKind::Presence).is_empty());
    }

    #[test]
    fn a_lapsed_entry_is_not_sent_on() {
        let clock = clock();
        let a = InMemoryFloor::new(clock.clone());
        a.publish(
            EntryKey::presence(node(1)),
            presence(1),
            Duration::from_secs(1),
        );
        clock.advance(Duration::from_secs(2));
        assert!(all(&a).is_empty());
        assert!(a.digest().is_empty());
    }

    #[test]
    fn a_withdrawn_entry_stays_withdrawn_when_an_older_copy_arrives() {
        let key = EntryKey::presence(node(1));
        let author = InMemoryFloor::new(clock());
        author.publish(key.clone(), presence(1), Duration::from_secs(30));
        let old_copy = all(&author);
        let peer = InMemoryFloor::new(clock());
        peer.merge(old_copy.clone());

        author.withdraw(&key);
        peer.merge(all(&author));
        assert!(peer.get(&key).is_none());

        // The old copy, still travelling the mesh, arrives afterwards.
        peer.merge(old_copy);
        assert!(peer.get(&key).is_none());
    }

    #[test]
    fn a_restarted_author_publishes_above_what_its_peers_remember() {
        let key = EntryKey::presence(node(1));
        let before = InMemoryFloor::new(clock());
        for _ in 0..5 {
            before.publish(key.clone(), presence(1), Duration::from_secs(30));
        }
        // The author restarts with an empty floor and hears its own old entry.
        let after = InMemoryFloor::new(clock());
        after.merge(all(&before));
        let version = after.publish(key.clone(), presence(2), Duration::from_secs(30));
        assert_eq!(version, 6);
        before.merge(all(&after));
        assert_eq!(before.get(&key).unwrap().body, presence(2));
    }

    #[test]
    fn changed_since_sends_only_what_is_new() {
        let a = InMemoryFloor::new(clock());
        a.publish(
            EntryKey::presence(node(1)),
            presence(1),
            Duration::from_secs(5),
        );
        let (first, seq) = a.changed_since(0);
        assert_eq!(first.len(), 1);
        assert!(a.changed_since(seq).0.is_empty());
        a.publish(
            EntryKey::presence(node(2)),
            presence(1),
            Duration::from_secs(5),
        );
        assert_eq!(a.changed_since(seq).0.len(), 1);
    }

    #[test]
    fn a_digest_finds_what_a_peer_is_missing() {
        let a = InMemoryFloor::new(clock());
        let b = InMemoryFloor::new(clock());
        a.publish(
            EntryKey::presence(node(1)),
            presence(1),
            Duration::from_secs(5),
        );
        a.publish(
            EntryKey::presence(node(2)),
            presence(1),
            Duration::from_secs(5),
        );
        b.merge(all(&a));
        a.publish(
            EntryKey::presence(node(2)),
            presence(9),
            Duration::from_secs(5),
        );
        a.publish(
            EntryKey::presence(node(3)),
            presence(1),
            Duration::from_secs(5),
        );

        let missing = a.newer_than(&b.digest());
        let mut nodes: Vec<_> = missing.iter().map(|w| w.key.node).collect();
        nodes.sort();
        let mut expect = vec![node(2), node(3)];
        expect.sort();
        assert_eq!(nodes, expect);
    }

    #[test]
    fn sampling_is_without_replacement_and_seeded() {
        use apiary_core::StdSeededRng;
        let a = InMemoryFloor::new(clock());
        for n in 1..=10 {
            a.publish(
                EntryKey::presence(node(n)),
                presence(n as u32),
                Duration::from_secs(5),
            );
        }
        let pick = |seed| {
            let mut rng = StdSeededRng::from_seed(seed);
            a.sample(EntryKind::Presence, 4, &mut rng)
                .into_iter()
                .map(|e| e.key.node)
                .collect::<Vec<_>>()
        };
        let first = pick(1);
        assert_eq!(first.len(), 4);
        let mut unique = first.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 4);
        assert_eq!(first, pick(1));
        assert_ne!(first, pick(2));
        assert_eq!(
            a.sample(EntryKind::Presence, 99, &mut StdSeededRng::from_seed(3))
                .len(),
            10
        );
    }
}
