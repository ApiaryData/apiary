//! A relay server, run inside an Apiary Node.
//!
//! A relay carries a connection between two Nodes that cannot reach each other
//! directly (both behind NAT, or a firewall that drops UDP) and helps them find a
//! direct path. It sees only ciphertext: the QUIC connection between the two Nodes
//! is encrypted and authenticated end to end with their own keys. Any Node with a
//! public address can run one, and a small cloud VM is the natural place.
//!
//! This relay speaks plain HTTP on its port. That is fine for the traffic it
//! carries, for the reason above, but a relay on the open internet should sit
//! behind a TLS-terminating proxy, and Nodes should then use its `https://` URL.

use std::net::SocketAddr;

use iroh_relay::server::{RelayConfig, Server, ServerConfig};

use crate::error::NetError;

/// A running relay.
pub struct RelayServer {
    server: Server,
}

impl RelayServer {
    /// Start a relay on `bind` (plain HTTP).
    pub async fn spawn(bind: SocketAddr) -> Result<Self, NetError> {
        let mut config = ServerConfig::default();
        config.relay = Some(RelayConfig::new(bind));
        let server = Server::spawn(config)
            .await
            .map_err(|e| NetError::Unreachable(format!("cannot start the relay on {bind}: {e}")))?;
        Ok(Self { server })
    }

    /// The address the relay is serving on.
    pub fn addr(&self) -> Option<SocketAddr> {
        self.server.http_addr()
    }

    /// Stop the relay.
    pub async fn shutdown(self) {
        let _ = self.server.shutdown().await;
    }
}
