//! The transport: how Nodes talk, behind Apiary's own trait.
//!
//! Everything above this layer (membership, the drive, later the dance floor and
//! the exchange of batches) sees only these traits, so the QUIC library can be
//! replaced without touching the colony, and the observation hive can run the
//! whole stack over a simulated network.
//!
//! One connection per peer and protocol carries many streams. The design has
//! three protocols, separated by ALPN: gossip (small frequent messages), exchange
//! (Arrow batches between stages) and control (requests and responses).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::NetError;
use crate::identity::NodeId;

/// The three protocols a connection can carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// Dance-floor entries and SWIM probes.
    Gossip,
    /// Arrow IPC streams between stages.
    Exchange,
    /// Requests and responses: the drive, plan fetches, summaries.
    Control,
}

impl Protocol {
    /// Every protocol.
    pub const ALL: [Protocol; 3] = [Protocol::Gossip, Protocol::Exchange, Protocol::Control];

    /// The ALPN that names this protocol on the wire.
    pub fn alpn(self) -> &'static [u8] {
        match self {
            Protocol::Gossip => b"apiary/gossip/1",
            Protocol::Exchange => b"apiary/exchange/1",
            Protocol::Control => b"apiary/control/1",
        }
    }

    /// The protocol an ALPN names.
    pub fn from_alpn(alpn: &[u8]) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.alpn() == alpn)
    }
}

/// Where a peer can be found: its id, and any addresses we know.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerAddr {
    /// The peer's Node id.
    pub id: NodeId,
    /// Direct socket addresses to try.
    pub direct: Vec<SocketAddr>,
    /// The relay it is reachable through, as a URL.
    pub relay: Option<String>,
}

impl PeerAddr {
    /// A peer known only by id.
    pub fn id_only(id: NodeId) -> Self {
        Self {
            id,
            direct: Vec::new(),
            relay: None,
        }
    }
}

/// One bidirectional stream.
pub struct Bi {
    /// The sending half.
    pub send: Box<dyn AsyncWrite + Send + Unpin>,
    /// The receiving half.
    pub recv: Box<dyn AsyncRead + Send + Unpin>,
}

/// How a connection is routed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathKind {
    /// Straight to the peer.
    Direct,
    /// Through a relay server.
    Relayed,
    /// Not known yet.
    Unknown,
}

/// What a connection's path looks like right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PathInfo {
    /// Direct or relayed.
    pub kind: PathKind,
    /// The round-trip time, if measured.
    pub rtt: Option<Duration>,
}

/// An established, authenticated connection to one peer for one protocol.
///
/// The peer's identity is proved by the transport: `remote` is the key the peer
/// connected as, and it holds that key.
#[async_trait]
pub trait Conn: Send + Sync {
    /// The peer's Node id.
    fn remote(&self) -> NodeId;
    /// The protocol this connection carries.
    fn protocol(&self) -> Protocol;
    /// Open a stream to the peer.
    async fn open_bi(&self) -> Result<Bi, NetError>;
    /// Accept a stream the peer opened.
    async fn accept_bi(&self) -> Result<Bi, NetError>;
    /// The current path.
    fn path(&self) -> PathInfo;
    /// Whether the connection has closed.
    fn is_closed(&self) -> bool;
    /// Close it.
    fn close(&self, reason: &str);
}

/// A way of reaching peers by Node id.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// This Node's id.
    fn id(&self) -> NodeId;
    /// Connect to a peer for a protocol.
    async fn dial(&self, peer: &PeerAddr, protocol: Protocol) -> Result<Arc<dyn Conn>, NetError>;
    /// The next incoming connection (of any protocol), or `Closed`.
    async fn accept(&self) -> Result<Arc<dyn Conn>, NetError>;
    /// Where this Node can be reached, to hand to peers and to discovery.
    fn addr(&self) -> PeerAddr;
    /// Stop: refuse new connections and close the existing ones.
    async fn close(&self);
}
