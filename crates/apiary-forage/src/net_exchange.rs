//! Handing batches between Nodes over the exchange protocol.
//!
//! A [`NetExchange`] is the comb-store exchange with a faster road beside it. A
//! Patch's output is written to the store as before, which keeps it durable and
//! readable by anyone, and is also kept in memory on the Node that produced it.
//! A consumer on another Node of the colony asks the producer for it over QUIC and
//! gets it from memory, one hop, instead of reading it back from the store (a drive
//! served by a host Pi is two hops, a bucket a request with its own latency). If
//! the producer is gone, slow, or has evicted the output, the consumer reads it
//! from the store, which has it.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::debug;

use apiary_core::Result;
use apiary_net::wire::{read_frame, write_frame};
use apiary_net::{Admitted, Handler, Mesh, NodeId, PeerAddr, Protocol, parse_public};
use apiary_plan::exchange::{Completed, Completion, Exchange, PatchKey};

/// How long a consumer waits for a producer before reading from the store.
const FETCH_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OutputKey {
    patch: PatchKey,
    attempt: String,
    output: usize,
    chunk: usize,
}

/// Outputs this Node produced, kept for others to fetch, up to a budget of bytes.
struct Held {
    outputs: BTreeMap<OutputKey, Bytes>,
    order: VecDeque<OutputKey>,
    bytes: usize,
    budget: usize,
}

impl Held {
    fn insert(&mut self, key: OutputKey, data: Bytes) {
        if data.len() > self.budget {
            return;
        }
        self.bytes += data.len();
        if let Some(old) = self.outputs.insert(key.clone(), data) {
            self.bytes -= old.len();
        } else {
            self.order.push_back(key);
        }
        // The oldest go first: a consumer that has not asked yet reads the store.
        while self.bytes > self.budget {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(gone) = self.outputs.remove(&oldest) {
                self.bytes -= gone.len();
            }
        }
    }

    fn get(&self, key: &OutputKey) -> Option<Bytes> {
        self.outputs.get(key).cloned()
    }

    fn forget(&mut self, query: &str) {
        self.outputs.retain(|k, _| k.patch.query != query);
        self.order.retain(|k| k.patch.query != query);
        self.bytes = self.outputs.values().map(Bytes::len).sum();
    }
}

#[derive(Serialize, Deserialize)]
struct Request {
    patch: PatchKey,
    attempt: String,
    output: usize,
    chunk: usize,
}

#[derive(Serialize, Deserialize)]
struct Header {
    /// The length of what follows, or none if this Node does not hold it.
    len: Option<u64>,
}

struct Shared {
    held: Mutex<Held>,
}

/// The comb-store exchange, with Patch outputs also served from memory to the
/// colony's other Nodes.
pub struct NetExchange {
    me: NodeId,
    inner: Arc<dyn Exchange>,
    mesh: Mesh,
    shared: Arc<Shared>,
    stats: Mutex<NetExchangeCounts>,
}

/// How outputs travelled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetExchangeCounts {
    /// Outputs read from this Node's own memory.
    pub local: u64,
    /// Outputs fetched from another Node over QUIC.
    pub peer: u64,
    /// Outputs read from the comb store.
    pub store: u64,
    /// Fetches from a peer that failed or came back empty, so the store was read.
    pub fell_back: u64,
}

impl NetExchange {
    /// An exchange over `inner` (the comb-store exchange) for the Node `me` on
    /// `mesh`, holding up to `budget` bytes of outputs for others to fetch. It
    /// serves the exchange protocol on the mesh.
    pub fn new(me: NodeId, inner: Arc<dyn Exchange>, mesh: Mesh, budget: usize) -> Arc<Self> {
        let shared = Arc::new(Shared {
            held: Mutex::new(Held {
                outputs: BTreeMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                budget,
            }),
        });
        mesh.register(Protocol::Exchange, Arc::new(Serve(Arc::clone(&shared))));
        Arc::new(Self {
            me,
            inner,
            mesh,
            shared,
            stats: Mutex::new(NetExchangeCounts::default()),
        })
    }

    /// How outputs have travelled so far.
    pub fn counts(&self) -> NetExchangeCounts {
        *self.stats.lock().expect("stats")
    }

    fn count(&self, change: impl FnOnce(&mut NetExchangeCounts)) {
        change(&mut self.stats.lock().expect("stats"));
    }

    async fn fetch(&self, peer: NodeId, request: &Request) -> Option<Bytes> {
        let attempt = async {
            let conn = self
                .mesh
                .connect(&PeerAddr::id_only(peer), Protocol::Exchange)
                .await
                .ok()?;
            let mut bi = conn.open_bi().await.ok()?;
            write_frame(&mut bi.send, request).await.ok()?;
            let header: Header = read_frame(&mut bi.recv).await.ok()?;
            let len = header.len? as usize;
            let mut data = vec![0u8; len];
            bi.recv.read_exact(&mut data).await.ok()?;
            Some(Bytes::from(data))
        };
        tokio::time::timeout(FETCH_TIMEOUT, attempt)
            .await
            .ok()
            .flatten()
    }
}

#[async_trait]
impl Exchange for NetExchange {
    async fn put(
        &self,
        patch: &PatchKey,
        attempt: &str,
        output: usize,
        chunk: usize,
        data: Bytes,
    ) -> Result<()> {
        // Durable first, then ready to be fetched.
        self.inner
            .put(patch, attempt, output, chunk, data.clone())
            .await?;
        self.shared.held.lock().expect("held").insert(
            OutputKey {
                patch: patch.clone(),
                attempt: attempt.to_string(),
                output,
                chunk,
            },
            data,
        );
        Ok(())
    }

    async fn complete(&self, patch: &PatchKey, record: &Completion) -> Result<Completed> {
        // The record names this Node by its network id, so a consumer can find it.
        let mut record = record.clone();
        record.node = self.me.to_string();
        self.inner.complete(patch, &record).await
    }

    async fn completion(&self, patch: &PatchKey) -> Result<Option<Completion>> {
        self.inner.completion(patch).await
    }

    async fn get(
        &self,
        patch: &PatchKey,
        done: &Completion,
        output: usize,
        chunk: usize,
    ) -> Result<Bytes> {
        let key = OutputKey {
            patch: patch.clone(),
            attempt: done.attempt.clone(),
            output,
            chunk,
        };
        if let Some(data) = self.shared.held.lock().expect("held").get(&key) {
            self.count(|c| c.local += 1);
            return Ok(data);
        }
        if let Ok(peer) = parse_public(&done.node)
            && peer != self.me
        {
            let request = Request {
                patch: patch.clone(),
                attempt: done.attempt.clone(),
                output,
                chunk,
            };
            match self.fetch(peer, &request).await {
                Some(data) => {
                    self.count(|c| c.peer += 1);
                    return Ok(data);
                }
                None => {
                    debug!(%peer, "A peer did not give an output; reading the store");
                    self.count(|c| c.fell_back += 1);
                }
            }
        }
        self.count(|c| c.store += 1);
        self.inner.get(patch, done, output, chunk).await
    }

    async fn completed(&self, query: &str, stage: usize) -> Result<Vec<usize>> {
        self.inner.completed(query, stage).await
    }

    async fn clear(&self, query: &str) -> Result<()> {
        self.shared.held.lock().expect("held").forget(query);
        self.inner.clear(query).await
    }
}

/// Serves outputs this Node holds to the colony's other Nodes.
struct Serve(Arc<Shared>);

#[async_trait]
impl Handler for Serve {
    async fn handle(&self, conn: Arc<Admitted>) {
        while let Ok(mut bi) = conn.accept_bi().await {
            let shared = Arc::clone(&self.0);
            tokio::spawn(async move {
                let Ok(request) = read_frame::<_, Request>(&mut bi.recv).await else {
                    return;
                };
                let data = shared.held.lock().expect("held").get(&OutputKey {
                    patch: request.patch,
                    attempt: request.attempt,
                    output: request.output,
                    chunk: request.chunk,
                });
                let header = Header {
                    len: data.as_ref().map(|d| d.len() as u64),
                };
                if write_frame(&mut bi.send, &header).await.is_err() {
                    return;
                }
                if let Some(data) = data {
                    let _ = bi.send.write_all(&data).await;
                }
                let _ = bi.send.shutdown().await;
            });
        }
    }
}
