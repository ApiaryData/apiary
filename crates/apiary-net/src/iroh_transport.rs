//! The iroh QUIC transport.
//!
//! [iroh](https://iroh.computer) dials by public key, encrypts and authenticates
//! every connection with the Node keys, connects directly when it can, punches
//! through NAT, and falls back to a relay when it must. This module is the one
//! place Apiary touches it: everything above sees the [`Transport`] trait.
//!
//! Apiary does not use iroh's public infrastructure. There is no address lookup
//! service and no default relay: peers are found by the discovery sources in
//! [`crate::discovery`] and by the addresses in join tokens, and the relay, if
//! any, is one the operator runs (see [`crate::relay`]).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use iroh::endpoint::{Connection, presets};
use iroh::{Endpoint, EndpointAddr, RelayMode, RelayUrl};

use crate::error::NetError;
use crate::identity::{NodeId, NodeKey};
use crate::transport::{Bi, Conn, PathInfo, PathKind, PeerAddr, Protocol, Transport};

/// How to run the iroh endpoint.
#[derive(Clone, Debug)]
pub struct IrohConfig {
    /// This Node's key.
    pub key: NodeKey,
    /// The UDP port to listen on (0 picks one). Pin it where a firewall or a
    /// container port mapping needs to know it.
    pub udp_port: u16,
    /// Relay servers to use, as URLs. Empty means no relay: peers must be
    /// directly reachable.
    pub relays: Vec<String>,
    /// Addresses to advertise besides the ones iroh finds, for a Node behind a
    /// port mapping it cannot see (Docker with published ports, a Kubernetes
    /// UDP Service).
    pub external_addrs: Vec<SocketAddr>,
    /// Use only the relay: bind no IP sockets. For tests of relayed paths.
    pub relay_only: bool,
}

impl IrohConfig {
    /// A configuration with these keys and everything else at its default.
    pub fn new(key: NodeKey) -> Self {
        Self {
            key,
            udp_port: 0,
            relays: Vec::new(),
            external_addrs: Vec::new(),
            relay_only: false,
        }
    }
}

/// A Node's iroh endpoint behind the [`Transport`] trait.
pub struct IrohTransport {
    endpoint: Endpoint,
}

impl IrohTransport {
    /// Bind an endpoint.
    pub async fn bind(cfg: IrohConfig) -> Result<Self, NetError> {
        let relay_mode = if cfg.relays.is_empty() {
            RelayMode::Disabled
        } else {
            let urls = cfg
                .relays
                .iter()
                .map(|u| {
                    u.parse::<RelayUrl>()
                        .map_err(|e| NetError::Unreachable(format!("bad relay URL '{u}': {e}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            RelayMode::custom(urls)
        };

        let mut builder = Endpoint::builder(presets::Minimal)
            .secret_key(cfg.key.secret().clone())
            .alpns(Protocol::ALL.iter().map(|p| p.alpn().to_vec()).collect())
            .relay_mode(relay_mode);

        if cfg.relay_only {
            builder = builder.clear_ip_transports();
        } else {
            let any = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), cfg.udp_port);
            builder = builder
                .bind_addr(any)
                .map_err(|e| NetError::Unreachable(format!("cannot bind {any}: {e}")))?;
        }
        for addr in &cfg.external_addrs {
            builder = builder.external_addr(*addr);
        }

        let endpoint = builder
            .bind()
            .await
            .map_err(|e| NetError::Unreachable(format!("cannot start the QUIC endpoint: {e}")))?;
        Ok(Self { endpoint })
    }

    /// The iroh endpoint, for status and tests.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

fn to_endpoint_addr(peer: &PeerAddr) -> EndpointAddr {
    let mut addr = EndpointAddr::new(peer.id);
    for direct in &peer.direct {
        addr = addr.with_ip_addr(*direct);
    }
    if let Some(relay) = peer
        .relay
        .as_deref()
        .and_then(|u| u.parse::<RelayUrl>().ok())
    {
        addr = addr.with_relay_url(relay);
    }
    addr
}

struct IrohConn {
    conn: Connection,
    protocol: Protocol,
}

#[async_trait]
impl Conn for IrohConn {
    fn remote(&self) -> NodeId {
        self.conn.remote_id()
    }

    fn protocol(&self) -> Protocol {
        self.protocol
    }

    async fn open_bi(&self) -> Result<Bi, NetError> {
        let (send, recv) = self.conn.open_bi().await.map_err(|_| NetError::Closed)?;
        Ok(Bi {
            send: Box::new(send),
            recv: Box::new(recv),
        })
    }

    async fn accept_bi(&self) -> Result<Bi, NetError> {
        let (send, recv) = self.conn.accept_bi().await.map_err(|_| NetError::Closed)?;
        Ok(Bi {
            send: Box::new(send),
            recv: Box::new(recv),
        })
    }

    fn path(&self) -> PathInfo {
        let paths = self.conn.paths();
        match paths.iter().find(|p| p.is_selected()) {
            Some(path) => PathInfo {
                kind: if path.is_ip() {
                    PathKind::Direct
                } else if path.is_relay() {
                    PathKind::Relayed
                } else {
                    PathKind::Unknown
                },
                rtt: Some(path.rtt()),
            },
            None => PathInfo {
                kind: PathKind::Unknown,
                rtt: None,
            },
        }
    }

    fn is_closed(&self) -> bool {
        self.conn.close_reason().is_some()
    }

    fn close(&self, reason: &str) {
        self.conn.close(0u32.into(), reason.as_bytes());
    }
}

#[async_trait]
impl Transport for IrohTransport {
    fn id(&self) -> NodeId {
        self.endpoint.id()
    }

    async fn dial(&self, peer: &PeerAddr, protocol: Protocol) -> Result<Arc<dyn Conn>, NetError> {
        let conn = self
            .endpoint
            .connect(to_endpoint_addr(peer), protocol.alpn())
            .await
            .map_err(|e| {
                NetError::Unreachable(format!("cannot connect to {}: {e}", peer.id.fmt_short()))
            })?;
        Ok(Arc::new(IrohConn { conn, protocol }))
    }

    async fn accept(&self) -> Result<Arc<dyn Conn>, NetError> {
        loop {
            let incoming = self.endpoint.accept().await.ok_or(NetError::Closed)?;
            let conn = match incoming.await {
                Ok(conn) => conn,
                // A failed handshake is one peer's problem, not the endpoint's.
                Err(_) => continue,
            };
            match Protocol::from_alpn(conn.alpn()) {
                Some(protocol) => return Ok(Arc::new(IrohConn { conn, protocol })),
                None => conn.close(1u32.into(), b"unknown protocol"),
            }
        }
    }

    fn addr(&self) -> PeerAddr {
        let addr = self.endpoint.addr();
        PeerAddr {
            id: self.endpoint.id(),
            direct: addr.ip_addrs().copied().collect(),
            relay: addr.relay_urls().next().map(ToString::to_string),
        }
    }

    async fn close(&self) {
        self.endpoint.close().await;
    }
}
