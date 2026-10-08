//! An in-process transport, for tests and for the simulator.
//!
//! Nodes on a [`MemNetwork`] reach each other by id through channels, with no
//! sockets and no real time. The network can be partitioned and each link given
//! a path (direct or relayed, with a round-trip time), so the layers above can be
//! tested against the failures they must survive.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::io::{ReadHalf, WriteHalf, split};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};

use crate::error::NetError;
use crate::identity::NodeId;
use crate::transport::{Bi, Conn, PathInfo, PathKind, PeerAddr, Protocol, Transport};

/// The size of each stream's in-memory buffer.
const STREAM_BUFFER: usize = 256 * 1024;

type Pair = (NodeId, NodeId);

fn pair(a: NodeId, b: NodeId) -> Pair {
    if a.as_bytes() <= b.as_bytes() {
        (a, b)
    } else {
        (b, a)
    }
}

struct Incoming {
    conn: Arc<MemConn>,
}

#[derive(Default)]
struct State {
    nodes: HashMap<NodeId, mpsc::UnboundedSender<Incoming>>,
    blocked: HashSet<Pair>,
    paths: HashMap<Pair, PathInfo>,
    conns: Vec<Arc<MemConn>>,
}

/// A simulated network that [`MemTransport`]s join.
#[derive(Clone, Default)]
pub struct MemNetwork {
    state: Arc<Mutex<State>>,
}

impl MemNetwork {
    /// An empty network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Join the network as `id`.
    pub fn join(&self, id: NodeId) -> MemTransport {
        let (tx, rx) = mpsc::unbounded_channel();
        self.state
            .lock()
            .expect("mem network poisoned")
            .nodes
            .insert(id, tx);
        MemTransport {
            id,
            net: self.clone(),
            incoming: AsyncMutex::new(rx),
            closed: AtomicBool::new(false),
            shutdown: watch::channel(false).0,
        }
    }

    /// Cut the link between two nodes: new connections fail and existing ones close.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        let mut state = self.state.lock().expect("mem network poisoned");
        state.blocked.insert(pair(a, b));
        for conn in &state.conns {
            if pair(conn.local, conn.remote) == pair(a, b) {
                conn.close("partitioned");
            }
        }
    }

    /// Restore a cut link.
    pub fn heal(&self, a: NodeId, b: NodeId) {
        self.state
            .lock()
            .expect("mem network poisoned")
            .blocked
            .remove(&pair(a, b));
    }

    /// Set what the path between two nodes looks like.
    pub fn set_path(&self, a: NodeId, b: NodeId, path: PathInfo) {
        self.state
            .lock()
            .expect("mem network poisoned")
            .paths
            .insert(pair(a, b), path);
    }

    fn path(&self, a: NodeId, b: NodeId) -> PathInfo {
        self.state
            .lock()
            .expect("mem network poisoned")
            .paths
            .get(&pair(a, b))
            .copied()
            .unwrap_or(PathInfo {
                kind: PathKind::Direct,
                rtt: Some(std::time::Duration::from_micros(200)),
            })
    }
}

/// One node's end of the in-memory network.
pub struct MemTransport {
    id: NodeId,
    net: MemNetwork,
    incoming: AsyncMutex<mpsc::UnboundedReceiver<Incoming>>,
    closed: AtomicBool,
    shutdown: watch::Sender<bool>,
}

struct MemConn {
    local: NodeId,
    remote: NodeId,
    protocol: Protocol,
    net: MemNetwork,
    to_peer: mpsc::UnboundedSender<Bi>,
    from_peer: AsyncMutex<mpsc::UnboundedReceiver<Bi>>,
    /// Shared with the peer's end: closing either closes both.
    closed: watch::Sender<bool>,
    closed_rx: watch::Receiver<bool>,
}

impl MemConn {
    fn close(&self, _reason: &str) {
        let _ = self.closed.send(true);
    }
}

#[async_trait]
impl Conn for Arc<MemConn> {
    fn remote(&self) -> NodeId {
        self.remote
    }

    fn protocol(&self) -> Protocol {
        self.protocol
    }

    async fn open_bi(&self) -> Result<Bi, NetError> {
        if *self.closed_rx.borrow() {
            return Err(NetError::Closed);
        }
        let (mine, theirs) = tokio::io::duplex(STREAM_BUFFER);
        self.to_peer
            .send(halves(theirs))
            .map_err(|_| NetError::Closed)?;
        Ok(halves(mine))
    }

    async fn accept_bi(&self) -> Result<Bi, NetError> {
        let mut closed = self.closed_rx.clone();
        let mut rx = self.from_peer.lock().await;
        loop {
            tokio::select! {
                stream = rx.recv() => return stream.ok_or(NetError::Closed),
                changed = closed.changed() => {
                    if changed.is_err() || *closed.borrow() {
                        return Err(NetError::Closed);
                    }
                }
            }
        }
    }

    fn path(&self) -> PathInfo {
        self.net.path(self.local, self.remote)
    }

    fn is_closed(&self) -> bool {
        *self.closed_rx.borrow()
    }

    fn close(&self, reason: &str) {
        MemConn::close(self, reason);
    }
}

fn halves(stream: tokio::io::DuplexStream) -> Bi {
    let (r, w): (ReadHalf<_>, WriteHalf<_>) = split(stream);
    Bi {
        send: Box::new(w),
        recv: Box::new(r),
    }
}

#[async_trait]
impl Transport for MemTransport {
    fn id(&self) -> NodeId {
        self.id
    }

    async fn dial(&self, peer: &PeerAddr, protocol: Protocol) -> Result<Arc<dyn Conn>, NetError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(NetError::Closed);
        }
        let target = peer.id;
        let (peer_tx, blocked) = {
            let state = self.net.state.lock().expect("mem network poisoned");
            (
                state.nodes.get(&target).cloned(),
                state.blocked.contains(&pair(self.id, target)),
            )
        };
        let peer_tx = peer_tx
            .filter(|_| !blocked)
            .ok_or_else(|| NetError::Unreachable(format!("no route to {}", target.fmt_short())))?;

        let (a_to_b, b_from_a) = mpsc::unbounded_channel();
        let (b_to_a, a_from_b) = mpsc::unbounded_channel();
        let (closed, closed_rx) = watch::channel(false);
        let make = |local, remote, to_peer, from_peer| {
            Arc::new(MemConn {
                local,
                remote,
                protocol,
                net: self.net.clone(),
                to_peer,
                from_peer: AsyncMutex::new(from_peer),
                closed: closed.clone(),
                closed_rx: closed_rx.clone(),
            })
        };
        let mine = make(self.id, target, a_to_b, a_from_b);
        let theirs = make(target, self.id, b_to_a, b_from_a);
        {
            let mut state = self.net.state.lock().expect("mem network poisoned");
            state.conns.push(Arc::clone(&mine));
        }
        peer_tx.send(Incoming { conn: theirs }).map_err(|_| {
            NetError::Unreachable(format!("{} is not listening", target.fmt_short()))
        })?;
        Ok(Arc::new(mine))
    }

    async fn accept(&self) -> Result<Arc<dyn Conn>, NetError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(NetError::Closed);
        }
        let mut shutdown = self.shutdown.subscribe();
        let mut incoming = self.incoming.lock().await;
        tokio::select! {
            next = incoming.recv() => match next {
                Some(Incoming { conn }) => Ok(Arc::new(conn)),
                None => Err(NetError::Closed),
            },
            _ = shutdown.wait_for(|stopped| *stopped) => Err(NetError::Closed),
        }
    }

    fn addr(&self) -> PeerAddr {
        PeerAddr::id_only(self.id)
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        let _ = self.shutdown.send(true);
        let mut state = self.net.state.lock().expect("mem network poisoned");
        state.nodes.remove(&self.id);
        for conn in &state.conns {
            if conn.local == self.id || conn.remote == self.id {
                conn.close("transport closed");
            }
        }
    }
}
