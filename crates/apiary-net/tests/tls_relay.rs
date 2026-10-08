//! A relay with TLS and QUIC address discovery, trusted through a private
//! certificate.

use std::sync::Arc;
use std::time::Duration;

use apiary_core::SystemClock;
use apiary_net::relay::{RelayTlsFiles, generate_cert, load_certs};
use apiary_net::{
    ApiaryKey, Caps, IrohConfig, IrohTransport, Mesh, MeshConfig, NodeId, NodeKey, PathKind,
    PeerAddr, Protocol, RelayServer, RevocationStore, Token, TokenSpec, Transport, Trust,
};

struct Member {
    id: NodeId,
    mesh: Mesh,
}

async fn member(
    apiary: &ApiaryKey,
    relay: &str,
    ca: Vec<rustls_pki_types::CertificateDer<'static>>,
    quic_port: u16,
) -> Member {
    let key = NodeKey::generate();
    let mut cfg = IrohConfig::new(key.clone());
    cfg.relays = vec![relay.to_string()];
    cfg.relay_only = true;
    cfg.relay_ca = ca;
    cfg.relay_quic_port = Some(quic_port);
    let transport: Arc<dyn Transport> = Arc::new(IrohTransport::bind(cfg).await.unwrap());
    let spec = TokenSpec {
        apiary: "t".into(),
        colony: "c".into(),
        caps: Caps::ALL,
        lifetime_secs: 3600,
        node: None,
        bootstrap: vec![],
        relay: None,
    };
    let now = chrono::Utc::now().timestamp();
    let mesh = Mesh::new(
        transport,
        MeshConfig {
            trust: Trust {
                apiary: "t".into(),
                key: apiary.public(),
            },
            token: Token::parse(&apiary.issue(&spec, now)).unwrap(),
            site: None,
            clock: SystemClock::shared(),
        },
        Arc::new(RevocationStore::open(None, apiary.public())),
    );
    mesh.start();
    Member { id: key.id(), mesh }
}

async fn tls_relay(dir: &std::path::Path) -> (RelayServer, String, u16) {
    let (cert, key) = generate_cert(&["localhost".into(), "127.0.0.1".into()]).unwrap();
    std::fs::write(dir.join("relay.pem"), cert).unwrap();
    std::fs::write(dir.join("relay.key"), key).unwrap();
    let relay = RelayServer::spawn_tls(
        "127.0.0.1:0".parse().unwrap(),
        &RelayTlsFiles {
            https: "127.0.0.1:0".parse().unwrap(),
            quic: "127.0.0.1:0".parse().unwrap(),
            cert: dir.join("relay.pem"),
            key: dir.join("relay.key"),
        },
    )
    .await
    .unwrap();
    let url = format!("https://127.0.0.1:{}", relay.https_addr().unwrap().port());
    let quic = relay.quic_addr().expect("address discovery is on").port();
    (relay, url, quic)
}

#[tokio::test]
async fn nodes_trusting_the_relays_certificate_connect_through_it() {
    let dir = tempfile::TempDir::new().unwrap();
    let (relay, url, quic) = tls_relay(dir.path()).await;
    let ca = load_certs(&dir.path().join("relay.pem")).unwrap();

    let apiary = ApiaryKey::generate();
    let a = member(&apiary, &url, ca.clone(), quic).await;
    let b = member(&apiary, &url, ca, quic).await;
    let peer = PeerAddr {
        id: b.id,
        direct: vec![],
        relay: Some(url),
    };
    let conn = tokio::time::timeout(
        Duration::from_secs(20),
        a.mesh.connect(&peer, Protocol::Control),
    )
    .await
    .expect("connected through the TLS relay")
    .unwrap();
    assert_eq!(conn.peer, b.id);
    assert_eq!(conn.path().kind, PathKind::Relayed);
    relay.shutdown().await;
}

#[tokio::test]
async fn a_node_that_does_not_trust_the_relay_cannot_use_it() {
    let dir = tempfile::TempDir::new().unwrap();
    let (relay, url, quic) = tls_relay(dir.path()).await;
    let apiary = ApiaryKey::generate();
    // No CA given: only the built-in public roots, which do not know this one.
    let a = member(&apiary, &url, vec![], quic).await;
    let b = member(&apiary, &url, vec![], quic).await;
    let peer = PeerAddr {
        id: b.id,
        direct: vec![],
        relay: Some(url),
    };
    let outcome = tokio::time::timeout(
        Duration::from_secs(8),
        a.mesh.connect(&peer, Protocol::Control),
    )
    .await;
    assert!(
        !matches!(outcome, Ok(Ok(_))),
        "an untrusted relay certificate must not be accepted"
    );
    relay.shutdown().await;
}
