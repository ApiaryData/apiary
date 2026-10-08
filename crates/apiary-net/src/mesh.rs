//! The mesh: connections admitted by membership.
//!
//! The [`Mesh`] sits on a [`Transport`] and lets a connection carry traffic only
//! once both ends have shown a valid join token. The first stream of every
//! connection is the admission handshake:
//!
//! 1. the dialer sends its token and its revocation list;
//! 2. the acceptor adopts the list if newer, checks the token against the key the
//!    transport authenticated, and replies with its own token and list, or refuses
//!    and says why;
//! 3. the dialer adopts the acceptor's list if newer and checks its token.
//!
//! So revocations spread on every connection in both directions, a site that was
//! offline learns what it missed the moment it reconnects, and a Node adopts a
//! newer list it hears from one peer by passing it to the rest at once.
//!
//! After admission the connection is multiplexed: each stream starts with a kind
//! byte, system streams (revocation pushes) are handled here, and the others
//! belong to the protocol's [`Handler`] or to whoever dialed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::{debug, info, warn};

use apiary_core::Clock;

use crate::error::NetError;
use crate::identity::NodeId;
use crate::revocation::{RevocationStore, Revocations};
use crate::token::{Membership, Token, Trust};
use crate::transport::{Bi, Conn, PathInfo, PathKind, PeerAddr, Protocol, Transport};
use crate::wire::{read_frame, write_frame};

/// How long a peer has to complete the handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

const KIND_SYSTEM: u8 = 0;
const KIND_PROTOCOL: u8 = 1;
const KIND_HANDSHAKE: u8 = 2;

#[derive(Serialize, Deserialize)]
struct Hello {
    token: String,
    revocations: Revocations,
    site: Option<String>,
}

#[derive(Serialize, Deserialize)]
enum Reply {
    Welcome(Hello),
    Refused { reason: String },
}

#[derive(Serialize, Deserialize)]
enum System {
    Revocations(Revocations),
}

/// What a Node needs to take part.
#[derive(Clone)]
pub struct MeshConfig {
    /// The Apiary and the key that vouches for its members.
    pub trust: Trust,
    /// This Node's own token.
    pub token: Token,
    /// This Node's site label, if one is declared.
    pub site: Option<String>,
    /// The clock membership is checked against.
    pub clock: Arc<dyn Clock>,
}

/// A connection that has passed admission.
pub struct Admitted {
    /// The peer.
    pub peer: NodeId,
    /// What the peer's token grants.
    pub membership: Membership,
    /// The peer's declared site label.
    pub site: Option<String>,
    conn: Arc<dyn Conn>,
    incoming: AsyncMutex<mpsc::Receiver<Bi>>,
}

impl Admitted {
    /// The protocol this connection carries.
    pub fn protocol(&self) -> Protocol {
        self.conn.protocol()
    }

    /// Open a stream to the peer.
    pub async fn open_bi(&self) -> Result<Bi, NetError> {
        let mut bi = self.conn.open_bi().await?;
        bi.send.write_all(&[KIND_PROTOCOL]).await?;
        Ok(bi)
    }

    /// Accept a stream the peer opened.
    pub async fn accept_bi(&self) -> Result<Bi, NetError> {
        self.incoming
            .lock()
            .await
            .recv()
            .await
            .ok_or(NetError::Closed)
    }

    /// The current path to the peer.
    pub fn path(&self) -> PathInfo {
        self.conn.path()
    }

    /// Whether the connection has closed.
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    /// Close the connection.
    pub fn close(&self, reason: &str) {
        self.conn.close(reason);
    }
}

/// Serves one protocol's admitted connections.
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Handle a connection until it closes.
    async fn handle(&self, conn: Arc<Admitted>);
}

/// What the mesh knows about an admitted peer.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    /// The peer.
    pub id: NodeId,
    /// What its token grants.
    pub membership: Membership,
    /// Its declared site label.
    pub site: Option<String>,
    /// The best path to it across its connections: direct beats relayed.
    pub path: PathInfo,
    /// The protocols it has connections for.
    pub protocols: Vec<Protocol>,
}

/// Live connections by peer and protocol.
type ConnTable = HashMap<(NodeId, Protocol), Vec<Arc<Admitted>>>;

struct Inner {
    transport: Arc<dyn Transport>,
    cfg: MeshConfig,
    revocations: Arc<RevocationStore>,
    handlers: Mutex<HashMap<Protocol, Arc<dyn Handler>>>,
    /// Live connections by peer and protocol. A pair can briefly have two, when
    /// both ends dial at once; all of them are kept so revocation reaches each.
    conns: Mutex<ConnTable>,
}

/// A Node's connections to its peers.
#[derive(Clone)]
pub struct Mesh {
    inner: Arc<Inner>,
}

impl Mesh {
    /// A mesh over `transport`. Call [`start`](Self::start) to accept connections.
    pub fn new(
        transport: Arc<dyn Transport>,
        cfg: MeshConfig,
        revocations: Arc<RevocationStore>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                transport,
                cfg,
                revocations,
                handlers: Mutex::new(HashMap::new()),
                conns: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// This Node's id.
    pub fn id(&self) -> NodeId {
        self.inner.transport.id()
    }

    /// Where this Node can be reached.
    pub fn addr(&self) -> PeerAddr {
        self.inner.transport.addr()
    }

    /// The transport underneath.
    pub fn transport(&self) -> &Arc<dyn Transport> {
        &self.inner.transport
    }

    /// The revocation list this Node holds.
    pub fn revocations(&self) -> Revocations {
        self.inner.revocations.current()
    }

    /// Serve a protocol: admitted incoming connections for it go to `handler`.
    pub fn register(&self, protocol: Protocol, handler: Arc<dyn Handler>) {
        self.inner
            .handlers
            .lock()
            .expect("handlers poisoned")
            .insert(protocol, handler);
    }

    /// Start accepting connections. Runs until the transport closes.
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let mesh = self.clone();
        tokio::spawn(async move {
            loop {
                match mesh.inner.transport.accept().await {
                    Ok(conn) => {
                        let mesh = mesh.clone();
                        tokio::spawn(async move { mesh.admit_incoming(conn).await });
                    }
                    Err(NetError::Closed) => return,
                    Err(e) => {
                        warn!(error = %e, "Accepting a connection failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        })
    }

    /// Connect to a peer for a protocol, reusing a live connection if there is one.
    pub async fn connect(
        &self,
        peer: &PeerAddr,
        protocol: Protocol,
    ) -> Result<Arc<Admitted>, NetError> {
        if let Some(existing) = self.live(peer.id, protocol) {
            return Ok(existing);
        }
        let conn = self.inner.transport.dial(peer, protocol).await?;
        let admitted = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.admit_outgoing(conn.clone()))
            .await
            .unwrap_or_else(|_| {
                conn.close("handshake timed out");
                Err(NetError::Unreachable("the handshake timed out".into()))
            })?;
        Ok(admitted)
    }

    /// Everyone this Node is admitted to, one entry per peer.
    pub fn peers(&self) -> Vec<PeerInfo> {
        let conns = self.inner.conns.lock().expect("conns poisoned");
        let mut by_peer: HashMap<NodeId, PeerInfo> = HashMap::new();
        for ((peer, protocol), admitted) in conns
            .iter()
            .flat_map(|(key, list)| list.iter().map(move |c| (key, c)))
        {
            if admitted.is_closed() {
                continue;
            }
            let path = admitted.path();
            by_peer
                .entry(*peer)
                .and_modify(|info| {
                    info.protocols.push(*protocol);
                    if rank(path) < rank(info.path) {
                        info.path = path;
                    }
                })
                .or_insert_with(|| PeerInfo {
                    id: *peer,
                    membership: admitted.membership.clone(),
                    site: admitted.site.clone(),
                    path,
                    protocols: vec![*protocol],
                });
        }
        let mut peers: Vec<PeerInfo> = by_peer.into_values().collect();
        peers.sort_by_key(|p| p.id);
        peers
    }

    /// Adopt a revocation list (from the Beekeeper or a peer). Returns whether it
    /// was new. Connections to anyone it revokes are closed and the list is
    /// passed on to the rest of the mesh.
    pub fn apply_revocations(&self, list: Revocations) -> Result<bool, NetError> {
        self.adopt(list, None)
    }

    /// A live admitted connection to a peer for a protocol, if there is one.
    pub fn peer_conn(&self, peer: NodeId, protocol: Protocol) -> Option<Arc<Admitted>> {
        self.live(peer, protocol)
    }

    fn live(&self, peer: NodeId, protocol: Protocol) -> Option<Arc<Admitted>> {
        let mut conns = self.inner.conns.lock().expect("conns poisoned");
        let list = conns.get_mut(&(peer, protocol))?;
        list.retain(|c| !c.is_closed());
        list.first().cloned()
    }

    fn now(&self) -> i64 {
        self.inner.cfg.clock.now_utc().timestamp()
    }

    fn hello(&self) -> Hello {
        Hello {
            token: self.inner.cfg.token.text().to_string(),
            revocations: self.inner.revocations.current(),
            site: self.inner.cfg.site.clone(),
        }
    }

    /// Check a peer's token for the key it connected as.
    fn check(&self, token: &str, peer: &NodeId) -> Result<Membership, String> {
        let token = Token::parse(token).map_err(|e| e.to_string())?;
        let membership = token
            .verify(
                &self.inner.cfg.trust,
                peer,
                self.now(),
                &self.inner.revocations.current(),
            )
            .map_err(|e| e.to_string())?;
        if membership.clock_suspect {
            warn!(
                peer = %peer.fmt_short(),
                "This node's clock is behind the peer's token; membership accepted without checking expiry"
            );
        }
        Ok(membership)
    }

    /// Adopt a list from `from` (or from outside), enforce it, pass it on.
    fn adopt(&self, list: Revocations, from: Option<NodeId>) -> Result<bool, NetError> {
        let adopted = self
            .inner
            .revocations
            .offer(list)
            .map_err(|e| NetError::Refused(e.to_string()))?;
        if adopted {
            self.enforce();
            self.propagate(from);
        }
        Ok(adopted)
    }

    /// Close connections to peers the current list revokes.
    fn enforce(&self) {
        let current = self.inner.revocations.current();
        let mut conns = self.inner.conns.lock().expect("conns poisoned");
        for ((peer, _), list) in conns.iter_mut() {
            list.retain(|admitted| {
                let revoked = current.revokes_node(peer)
                    || current.revokes_token(&admitted.membership.token_id);
                if revoked {
                    info!(peer = %peer.fmt_short(), "Closing the connection: the peer was revoked");
                    admitted.close("revoked");
                }
                !revoked
            });
        }
        conns.retain(|_, list| !list.is_empty());
    }

    /// Send the current list to every peer but `except`.
    fn propagate(&self, except: Option<NodeId>) {
        let list = self.inner.revocations.current();
        let targets: Vec<Arc<Admitted>> = {
            let conns = self.inner.conns.lock().expect("conns poisoned");
            let mut seen = std::collections::HashSet::new();
            conns
                .iter()
                .flat_map(|(key, list)| list.iter().map(move |c| (key, c)))
                .filter(|((peer, _), c)| {
                    Some(*peer) != except && !c.is_closed() && seen.insert(*peer)
                })
                .map(|(_, c)| Arc::clone(c))
                .collect()
        };
        for target in targets {
            let list = list.clone();
            tokio::spawn(async move {
                let sent = async {
                    let mut bi = target.conn.open_bi().await?;
                    bi.send.write_all(&[KIND_SYSTEM]).await?;
                    write_frame(&mut bi.send, &System::Revocations(list)).await
                }
                .await;
                if let Err(e) = sent {
                    debug!(peer = %target.peer.fmt_short(), error = %e, "Could not pass on the revocation list");
                }
            });
        }
    }

    // -- the handshake ------------------------------------------------------

    async fn admit_outgoing(&self, conn: Arc<dyn Conn>) -> Result<Arc<Admitted>, NetError> {
        let peer = conn.remote();
        let mut bi = conn.open_bi().await?;
        bi.send.write_all(&[KIND_HANDSHAKE]).await?;
        write_frame(&mut bi.send, &self.hello()).await?;

        let reply: Reply = read_frame(&mut bi.recv).await?;
        let welcome = match reply {
            Reply::Refused { reason } => {
                conn.close("refused");
                return Err(NetError::Refused(format!("the peer refused us: {reason}")));
            }
            Reply::Welcome(welcome) => welcome,
        };
        // Their list may be newer than ours; take it before judging their token.
        let _ = self.adopt(welcome.revocations, Some(peer));
        let membership = match self.check(&welcome.token, &peer) {
            Ok(m) => m,
            Err(reason) => {
                conn.close("peer not admitted");
                return Err(NetError::Refused(format!("the peer's token: {reason}")));
            }
        };
        Ok(self.register_conn(conn, membership, welcome.site))
    }

    async fn admit_incoming(&self, conn: Arc<dyn Conn>) {
        let peer = conn.remote();
        let outcome = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
            let mut bi = conn.accept_bi().await?;
            let mut kind = [0u8; 1];
            bi.recv.read_exact(&mut kind).await?;
            if kind[0] != KIND_HANDSHAKE {
                return Err(NetError::Protocol(
                    "the first stream must be the handshake".into(),
                ));
            }
            let hello: Hello = read_frame(&mut bi.recv).await?;

            let _ = self.adopt(hello.revocations, Some(peer));
            match self.check(&hello.token, &peer) {
                Ok(membership) => {
                    write_frame(&mut bi.send, &Reply::Welcome(self.hello())).await?;
                    Ok(Some((membership, hello.site)))
                }
                Err(reason) => {
                    warn!(peer = %peer.fmt_short(), %reason, "Refusing a peer");
                    write_frame(&mut bi.send, &Reply::Refused { reason }).await?;
                    Ok(None)
                }
            }
        })
        .await;

        match outcome {
            Ok(Ok(Some((membership, site)))) => {
                let admitted = self.register_conn(conn.clone(), membership, site);
                let handler = self
                    .inner
                    .handlers
                    .lock()
                    .expect("handlers poisoned")
                    .get(&conn.protocol())
                    .cloned();
                // With no handler for this protocol the connection simply stays
                // open for streams its dialer accepts.
                if let Some(handler) = handler {
                    handler.handle(admitted).await;
                }
            }
            Ok(Ok(None)) => {
                // Give the peer a moment to read the refusal, then hang up.
                tokio::time::sleep(Duration::from_millis(50)).await;
                conn.close("refused");
            }
            Ok(Err(e)) => {
                debug!(peer = %peer.fmt_short(), error = %e, "Handshake failed");
                conn.close("handshake failed");
            }
            Err(_) => conn.close("handshake timed out"),
        }
    }

    /// Record an admitted connection and start demultiplexing its streams.
    fn register_conn(
        &self,
        conn: Arc<dyn Conn>,
        membership: Membership,
        site: Option<String>,
    ) -> Arc<Admitted> {
        let (tx, rx) = mpsc::channel(64);
        let admitted = Arc::new(Admitted {
            peer: conn.remote(),
            membership,
            site,
            conn: Arc::clone(&conn),
            incoming: AsyncMutex::new(rx),
        });
        {
            let mut conns = self.inner.conns.lock().expect("conns poisoned");
            let list = conns.entry((admitted.peer, conn.protocol())).or_default();
            list.retain(|c| !c.is_closed());
            list.push(Arc::clone(&admitted));
        }

        let mesh = self.clone();
        let peer = admitted.peer;
        tokio::spawn(async move {
            while let Ok(mut bi) = conn.accept_bi().await {
                let mut kind = [0u8; 1];
                if bi.recv.read_exact(&mut kind).await.is_err() {
                    continue;
                }
                match kind[0] {
                    KIND_PROTOCOL => {
                        if tx.send(bi).await.is_err() {
                            break;
                        }
                    }
                    KIND_SYSTEM => {
                        let mesh = mesh.clone();
                        tokio::spawn(async move {
                            if let Ok(System::Revocations(list)) =
                                read_frame::<_, System>(&mut bi.recv).await
                            {
                                let _ = mesh.adopt(list, Some(peer));
                            }
                        });
                    }
                    _ => {}
                }
            }
        });
        admitted
    }
}

fn rank(path: PathInfo) -> u8 {
    match path.kind {
        PathKind::Direct => 0,
        PathKind::Unknown => 1,
        PathKind::Relayed => 2,
    }
}
