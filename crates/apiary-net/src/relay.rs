//! A relay server, run inside an Apiary Node.
//!
//! A relay carries a connection between two Nodes that cannot reach each other
//! directly (both behind NAT, or a firewall that drops UDP) and helps them find a
//! direct path. It sees only ciphertext: the QUIC connection between the two Nodes
//! is encrypted and authenticated end to end with their own keys. Any Node with a
//! public address can run one, and a small cloud VM is the natural place.
//!
//! Two modes:
//!
//! - **Plain HTTP** ([`RelayServer::spawn`]): forwards traffic. Two Nodes behind
//!   NATs reach each other through it, but cannot get to a direct path, because
//!   neither learns what address its NAT gave it.
//! - **TLS with address discovery** ([`RelayServer::spawn_tls`]): also answers
//!   QUIC address discovery. A Node asks the relay what address and port it sees a
//!   packet come from, and so learns its NAT's mapping; with both mappings known,
//!   the two Nodes send to each other at once and the NATs let the packets in.
//!   QUIC needs TLS, so this mode needs a certificate, and Nodes must trust it
//!   (a public certificate, or your own CA's: see [`generate_cert`]).
//!
//! Whether a direct path forms still depends on the NATs: an ordinary (cone) NAT
//! can be punched through, a symmetric NAT cannot, and then the relay carries the
//! traffic, as it always could.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iroh_relay::server::{CertConfig, QuicConfig, RelayConfig, Server, ServerConfig, TlsConfig};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::NetError;

/// A running relay.
pub struct RelayServer {
    server: Server,
}

/// Where a TLS relay listens and what it presents.
#[derive(Clone, Debug)]
pub struct RelayTlsFiles {
    /// The HTTPS port Nodes connect to.
    pub https: SocketAddr,
    /// The UDP port that answers QUIC address discovery (7842 by default for Nodes).
    pub quic: SocketAddr,
    /// The certificate chain, as PEM.
    pub cert: PathBuf,
    /// The private key, as PEM.
    pub key: PathBuf,
}

impl RelayServer {
    /// Start a relay on `bind` (plain HTTP, no address discovery).
    pub async fn spawn(bind: SocketAddr) -> Result<Self, NetError> {
        let mut config = ServerConfig::default();
        config.relay = Some(RelayConfig::new(bind));
        Self::start(config, bind).await
    }

    /// Start a relay with TLS and QUIC address discovery. `http` serves a small
    /// plain-HTTP probe endpoint beside the TLS one.
    pub async fn spawn_tls(http: SocketAddr, tls: &RelayTlsFiles) -> Result<Self, NetError> {
        let server_config = server_config(&tls.cert, &tls.key)?;
        let mut relay = RelayConfig::new(http);
        relay.tls = Some(TlsConfig::new(
            tls.https,
            CertConfig::Manual { server_config },
        ));
        let mut config = ServerConfig::default();
        config.relay = Some(relay);
        config.quic = Some(QuicConfig::new(tls.quic));
        Self::start(config, http).await
    }

    async fn start(config: ServerConfig, bind: SocketAddr) -> Result<Self, NetError> {
        let server = Server::spawn(config)
            .await
            .map_err(|e| NetError::Unreachable(format!("cannot start the relay on {bind}: {e}")))?;
        Ok(Self { server })
    }

    /// The address the plain-HTTP relay (or probe endpoint) is serving on.
    pub fn addr(&self) -> Option<SocketAddr> {
        self.server.http_addr()
    }

    /// The address the TLS relay is serving on, if it has TLS.
    pub fn https_addr(&self) -> Option<SocketAddr> {
        self.server.https_addr()
    }

    /// The UDP address that answers QUIC address discovery, if enabled.
    pub fn quic_addr(&self) -> Option<SocketAddr> {
        self.server.quic_addr()
    }

    /// Stop the relay.
    pub async fn shutdown(self) {
        let _ = self.server.shutdown().await;
    }
}

fn tls_err(what: &str, path: &Path, e: impl std::fmt::Display) -> NetError {
    NetError::Unreachable(format!("{what} {}: {e}", path.display()))
}

/// Read the certificates in a PEM file.
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, NetError> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| tls_err("cannot read the certificate", path, e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| tls_err("cannot parse the certificate", path, e))?;
    if certs.is_empty() {
        return Err(tls_err("no certificate found in", path, "empty"));
    }
    Ok(certs)
}

fn server_config(cert: &Path, key: &Path) -> Result<rustls::ServerConfig, NetError> {
    let certs = load_certs(cert)?;
    let key = PrivateKeyDer::from_pem_file(key)
        .map_err(|e| tls_err("cannot read the private key", key, e))?;
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| NetError::Unreachable(format!("TLS protocols: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            NetError::Unreachable(format!("the relay certificate and key do not match: {e}"))
        })
}

/// Make a self-signed certificate valid for the given names (DNS names or IP
/// addresses). Returns `(certificate PEM, private key PEM)`. Give Nodes the
/// certificate as `relay_ca` so they trust the relay.
pub fn generate_cert(names: &[String]) -> Result<(String, String), NetError> {
    let cert = rcgen::generate_simple_self_signed(names.to_vec())
        .map_err(|e| NetError::Protocol(format!("cannot make a certificate: {e}")))?;
    Ok((cert.cert.pem(), cert.signing_key.serialize_pem()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_certificate_and_key_load_and_match() {
        let dir = tempfile::TempDir::new().unwrap();
        let (cert, key) = generate_cert(&["relay.example".into(), "10.20.0.20".into()]).unwrap();
        std::fs::write(dir.path().join("relay.pem"), cert).unwrap();
        std::fs::write(dir.path().join("relay.key"), key).unwrap();
        assert_eq!(load_certs(&dir.path().join("relay.pem")).unwrap().len(), 1);
        assert!(
            server_config(&dir.path().join("relay.pem"), &dir.path().join("relay.key")).is_ok()
        );
        // A key from another certificate is refused.
        let (_, other_key) = generate_cert(&["elsewhere".into()]).unwrap();
        std::fs::write(dir.path().join("other.key"), other_key).unwrap();
        assert!(
            server_config(&dir.path().join("relay.pem"), &dir.path().join("other.key")).is_err()
        );
        assert!(load_certs(&dir.path().join("missing.pem")).is_err());
    }
}
