//! The floor over the gossip protocol.
//!
//! Every round, a Node pushes to each peer in its colony what has changed on its
//! own copy since it last pushed to that peer, and the peer answers with what it
//! holds that the sender lacks. Because an entry that arrives and changes a copy
//! counts as a change there too, entries travel on by way of whichever Nodes can
//! reach each other, so a full mesh is not required for the floor to converge,
//! only for it to converge in one round. Every few rounds a push also carries a
//! digest of everything the sender holds, which repairs whatever a lost message
//! or a reconnection left behind.
//!
//! One round per peer is in flight at a time, so a peer that has gone quiet never
//! holds up the rest.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

use apiary_core::{Clock, SeededRng, StdSeededRng};
use apiary_net::wire::{read_frame, write_frame};
use apiary_net::{Admitted, Handler, Mesh, NetError, NodeId, PeerAddr, Protocol};

use crate::Floor;
use crate::entry::{Entry, EntryBody, EntryKey, EntryKind, WireEntry};
use crate::state::InMemoryFloor;

/// How long one exchange with a peer may take.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(2);

/// What a Node pushes.
#[derive(Serialize, Deserialize)]
struct Push {
    entries: Vec<WireEntry>,
    /// Every key the sender holds and its version, now and then.
    digest: Option<Vec<(EntryKey, u64)>>,
}

/// What a peer answers.
#[derive(Serialize, Deserialize)]
struct Pull {
    entries: Vec<WireEntry>,
}

/// How a Node gossips.
#[derive(Clone, Debug)]
pub struct GossipConfig {
    /// The colony whose floor this is. Peers in other colonies are not gossiped
    /// with: a colony's floor is its own.
    pub colony: String,
    /// Time between rounds.
    pub interval: Duration,
    /// Every this many rounds, a push carries a full digest.
    pub anti_entropy_every: u32,
    /// Seeds the jitter that keeps Nodes from gossiping in step.
    pub seed: u64,
}

impl GossipConfig {
    /// Defaults for a colony: a round every 250 ms, a digest every fifth round.
    pub fn new(colony: impl Into<String>) -> Self {
        Self {
            colony: colony.into(),
            interval: Duration::from_millis(250),
            anti_entropy_every: 5,
            seed: 0,
        }
    }
}

/// What gossip has done, for status and tests.
#[derive(Default)]
pub struct GossipStats {
    rounds: AtomicU64,
    pushes: AtomicU64,
    failed: AtomicU64,
    entries_sent: AtomicU64,
    entries_received: AtomicU64,
}

/// A snapshot of [`GossipStats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GossipCounts {
    /// Rounds begun.
    pub rounds: u64,
    /// Pushes that completed.
    pub pushes: u64,
    /// Pushes that failed (the peer was unreachable or slow).
    pub failed: u64,
    /// Entries sent, in pushes and in answers.
    pub entries_sent: u64,
    /// Entries received, in pushes and in answers.
    pub entries_received: u64,
}

struct PeerSync {
    /// This copy's sequence number as of the last push the peer acknowledged.
    sent_seq: u64,
    /// Pushes made, to space out the digests.
    pushes: u32,
}

struct Shared {
    floor: Arc<InMemoryFloor>,
    mesh: Mesh,
    cfg: GossipConfig,
    peers: Mutex<HashMap<NodeId, PeerSync>>,
    in_flight: Mutex<HashSet<NodeId>>,
    stats: GossipStats,
}

/// A Node's floor, gossiped to its colony's other Nodes.
pub struct GossipFloor {
    shared: Arc<Shared>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl GossipFloor {
    /// Start gossiping over `mesh`. The floor serves the gossip protocol on the
    /// mesh and begins its rounds at once; both stop when it is dropped or
    /// [`stop`](Self::stop)ped.
    pub fn start(mesh: Mesh, clock: Arc<dyn Clock>, cfg: GossipConfig) -> Self {
        let floor = Arc::new(InMemoryFloor::new(clock.clone()));
        let shared = Arc::new(Shared {
            floor,
            mesh,
            cfg,
            peers: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashSet::new()),
            stats: GossipStats::default(),
        });
        shared
            .mesh
            .register(Protocol::Gossip, Arc::new(Serve(shared.clone())));
        let runner = tokio::spawn(run(shared.clone(), clock));
        Self {
            shared,
            tasks: Mutex::new(vec![runner]),
        }
    }

    /// The local copy, for merging into directly.
    pub fn local(&self) -> &Arc<InMemoryFloor> {
        &self.shared.floor
    }

    /// What gossip has done so far.
    pub fn counts(&self) -> GossipCounts {
        let s = &self.shared.stats;
        GossipCounts {
            rounds: s.rounds.load(Ordering::Relaxed),
            pushes: s.pushes.load(Ordering::Relaxed),
            failed: s.failed.load(Ordering::Relaxed),
            entries_sent: s.entries_sent.load(Ordering::Relaxed),
            entries_received: s.entries_received.load(Ordering::Relaxed),
        }
    }

    /// Stop gossiping. The local copy stays readable.
    pub fn stop(&self) {
        for task in self.tasks.lock().expect("tasks poisoned").drain(..) {
            task.abort();
        }
    }
}

impl Drop for GossipFloor {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Floor for GossipFloor {
    fn publish(&self, key: EntryKey, body: EntryBody, ttl: Duration) -> u64 {
        self.shared.floor.publish(key, body, ttl)
    }

    fn withdraw(&self, key: &EntryKey) {
        self.shared.floor.withdraw(key);
    }

    fn get(&self, key: &EntryKey) -> Option<Entry> {
        self.shared.floor.get(key)
    }

    fn entries(&self, kind: EntryKind) -> Vec<Entry> {
        self.shared.floor.entries(kind)
    }

    fn sample(&self, kind: EntryKind, k: usize, rng: &mut dyn SeededRng) -> Vec<Entry> {
        self.shared.floor.sample(kind, k, rng)
    }
}

/// Answers pushes on admitted gossip connections.
struct Serve(Arc<Shared>);

#[async_trait]
impl Handler for Serve {
    async fn handle(&self, conn: Arc<Admitted>) {
        // Only the colony's own Nodes may post to its floor.
        if conn.membership.colony != self.0.cfg.colony {
            conn.close("not in this colony");
            return;
        }
        while let Ok(mut bi) = conn.accept_bi().await {
            let shared = self.0.clone();
            tokio::spawn(async move {
                let push: Push = match read_frame(&mut bi.recv).await {
                    Ok(push) => push,
                    Err(e) => {
                        debug!(error = %e, "Unreadable gossip push");
                        return;
                    }
                };
                shared
                    .stats
                    .entries_received
                    .fetch_add(push.entries.len() as u64, Ordering::Relaxed);
                shared.floor.merge(push.entries);
                let entries = push
                    .digest
                    .map(|d| shared.floor.newer_than(&d))
                    .unwrap_or_default();
                shared
                    .stats
                    .entries_sent
                    .fetch_add(entries.len() as u64, Ordering::Relaxed);
                if write_frame(&mut bi.send, &Pull { entries }).await.is_ok() {
                    let _ = bi.send.shutdown().await;
                }
            });
        }
    }
}

async fn run(shared: Arc<Shared>, clock: Arc<dyn Clock>) {
    let mut rng = StdSeededRng::from_seed(shared.cfg.seed ^ hash_id(shared.mesh.id()));
    loop {
        // Up to a fifth either way, so Nodes started together do not stay in step.
        let jitter = 0.8 + 0.4 * rng.next_f64();
        clock.sleep(shared.cfg.interval.mul_f64(jitter)).await;
        shared.floor.sweep();
        shared.stats.rounds.fetch_add(1, Ordering::Relaxed);

        let peers: Vec<NodeId> = shared
            .mesh
            .peers()
            .into_iter()
            .filter(|p| p.membership.colony == shared.cfg.colony)
            .map(|p| p.id)
            .collect();
        shared
            .peers
            .lock()
            .expect("peers poisoned")
            .retain(|id, _| peers.contains(id));
        for peer in peers {
            if !shared
                .in_flight
                .lock()
                .expect("in-flight poisoned")
                .insert(peer)
            {
                continue;
            }
            let shared = shared.clone();
            tokio::spawn(async move {
                let result = tokio::time::timeout(EXCHANGE_TIMEOUT, push(&shared, peer)).await;
                shared
                    .in_flight
                    .lock()
                    .expect("in-flight poisoned")
                    .remove(&peer);
                match result {
                    Ok(Ok(())) => {
                        shared.stats.pushes.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(e)) => {
                        shared.stats.failed.fetch_add(1, Ordering::Relaxed);
                        debug!(%peer, error = %e, "A gossip push failed");
                    }
                    Err(_) => {
                        shared.stats.failed.fetch_add(1, Ordering::Relaxed);
                        warn!(%peer, "A gossip push timed out");
                    }
                }
            });
        }
    }
}

/// One push to one peer, and the answer.
async fn push(shared: &Shared, peer: NodeId) -> Result<(), NetError> {
    let (since, round) = {
        let mut peers = shared.peers.lock().expect("peers poisoned");
        let sync = peers.entry(peer).or_insert(PeerSync {
            sent_seq: 0,
            pushes: 0,
        });
        (sync.sent_seq, sync.pushes)
    };
    let (entries, seq) = shared.floor.changed_since(since);
    // A first push, and every few after it, carries the digest.
    let digest = (round % shared.cfg.anti_entropy_every.max(1) == 0).then(|| shared.floor.digest());
    // Nothing new and nothing to compare: skip, but still count as a round.
    if entries.is_empty() && digest.is_none() {
        let mut peers = shared.peers.lock().expect("peers poisoned");
        if let Some(sync) = peers.get_mut(&peer) {
            sync.pushes += 1;
        }
        return Ok(());
    }
    let sent = entries.len() as u64;

    let conn = shared
        .mesh
        .connect(&PeerAddr::id_only(peer), Protocol::Gossip)
        .await?;
    let mut bi = conn.open_bi().await?;
    write_frame(&mut bi.send, &Push { entries, digest }).await?;
    let pull: Pull = read_frame(&mut bi.recv).await?;
    let _ = bi.send.shutdown().await;

    shared.stats.entries_sent.fetch_add(sent, Ordering::Relaxed);
    shared
        .stats
        .entries_received
        .fetch_add(pull.entries.len() as u64, Ordering::Relaxed);
    shared.floor.merge(pull.entries);
    let mut peers = shared.peers.lock().expect("peers poisoned");
    if let Some(sync) = peers.get_mut(&peer) {
        sync.sent_seq = seq;
        sync.pushes += 1;
    }
    Ok(())
}

fn hash_id(id: NodeId) -> u64 {
    apiary_core::rng::hash_str(&id.to_string())
}
