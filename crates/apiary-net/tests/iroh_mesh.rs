//! The mesh over real QUIC (iroh) on loopback: direct paths, a relay, and the
//! upgrade from one to the other.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use apiary_core::SystemClock;
use apiary_net::{
    Admitted, ApiaryKey, Caps, Handler, IrohConfig, IrohTransport, Mesh, MeshConfig, NetError,
    NodeId, NodeKey, PathKind, PeerAddr, Protocol, RelayServer, RevocationStore, Revocations,
    Token, TokenSpec, Transport, Trust,
};

const APIARY: &str = "factory";

struct Member {
    mesh: Mesh,
    id: NodeId,
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn token(apiary: &ApiaryKey) -> Token {
    let spec = TokenSpec {
        apiary: APIARY.into(),
        colony: "line1".into(),
        caps: Caps::ALL,
        lifetime_secs: 3600,
        node: None,
        bootstrap: vec![],
        relay: None,
    };
    Token::parse(&apiary.issue(&spec, now())).unwrap()
}

async fn member(apiary: &ApiaryKey, relays: Vec<String>, relay_only: bool) -> Member {
    let key = NodeKey::generate();
    let mut cfg = IrohConfig::new(key.clone());
    cfg.relays = relays;
    cfg.relay_only = relay_only;
    let transport: Arc<dyn Transport> = Arc::new(IrohTransport::bind(cfg).await.unwrap());
    let mesh = Mesh::new(
        transport,
        MeshConfig {
            trust: Trust {
                apiary: APIARY.into(),
                key: apiary.public(),
            },
            token: token(apiary),
            site: None,
            clock: SystemClock::shared(),
        },
        Arc::new(RevocationStore::open(None, apiary.public())),
    );
    mesh.start();
    mesh.register(Protocol::Control, Arc::new(Echo));
    Member { id: key.id(), mesh }
}

struct Echo;

#[async_trait]
impl Handler for Echo {
    async fn handle(&self, conn: Arc<Admitted>) {
        while let Ok(mut bi) = conn.accept_bi().await {
            let mut buf = Vec::new();
            if bi.recv.read_to_end(&mut buf).await.is_ok() {
                let _ = bi.send.write_all(&buf).await;
                let _ = bi.send.shutdown().await;
            }
        }
    }
}

async fn echo(conn: &Admitted, message: &[u8]) -> Vec<u8> {
    let mut bi = conn.open_bi().await.unwrap();
    bi.send.write_all(message).await.unwrap();
    bi.send.shutdown().await.unwrap();
    let mut out = Vec::new();
    bi.recv.read_to_end(&mut out).await.unwrap();
    out
}

/// The loopback address of `m`'s endpoint, as a direct hint.
fn direct_hint(m: &Member) -> PeerAddr {
    let mut addr = m.mesh.addr();
    addr.direct.retain(|a| a.ip().is_loopback());
    if addr.direct.is_empty() {
        // The endpoint reports interface addresses; fall back to any it has.
        addr = m.mesh.addr();
    }
    addr.relay = None;
    addr
}

#[tokio::test]
async fn two_nodes_connect_directly_by_key_and_talk() {
    let apiary = ApiaryKey::generate();
    let (a, b) = (
        member(&apiary, vec![], false).await,
        member(&apiary, vec![], false).await,
    );

    let hint = direct_hint(&b);
    assert!(!hint.direct.is_empty(), "b has addresses to dial");
    let conn = a.mesh.connect(&hint, Protocol::Control).await.unwrap();
    assert_eq!(conn.peer, b.id);
    assert_eq!(echo(&conn, b"over quic").await, b"over quic");
    assert_eq!(conn.path().kind, PathKind::Direct);
    assert!(conn.path().rtt.is_some());
}

#[tokio::test]
async fn a_peer_with_the_wrong_apiary_key_is_refused_over_quic() {
    let apiary = ApiaryKey::generate();
    let outsider_apiary = ApiaryKey::generate();
    let b = member(&apiary, vec![], false).await;
    let outsider = member(&outsider_apiary, vec![], false).await;

    let err = outsider
        .mesh
        .connect(&direct_hint(&b), Protocol::Control)
        .await
        .err()
        .expect("refused");
    assert!(matches!(err, NetError::Refused(_)), "{err}");
}

#[tokio::test]
async fn a_revoked_key_is_refused_over_quic() {
    let apiary = ApiaryKey::generate();
    let (a, victim) = (
        member(&apiary, vec![], false).await,
        member(&apiary, vec![], false).await,
    );
    let list = apiary.revoke(&Revocations::empty(), &[victim.id], &[], now());
    a.mesh.apply_revocations(list).unwrap();

    let err = victim
        .mesh
        .connect(&direct_hint(&a), Protocol::Control)
        .await
        .err()
        .expect("revoked");
    assert!(matches!(err, NetError::Refused(_)), "{err}");
}

#[tokio::test]
async fn nodes_that_cannot_reach_each_other_directly_talk_through_a_relay() {
    let relay = RelayServer::spawn("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let url = format!("http://{}", relay.addr().expect("the relay is serving"));

    // Neither has an IP socket: the relay is the only path there is.
    let apiary = ApiaryKey::generate();
    let a = member(&apiary, vec![url.clone()], true).await;
    let b = member(&apiary, vec![url.clone()], true).await;

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
    .expect("connected in time")
    .unwrap();
    assert_eq!(
        echo(&conn, b"through the relay").await,
        b"through the relay"
    );
    assert_eq!(conn.path().kind, PathKind::Relayed);

    relay.shutdown().await;
}

#[tokio::test]
async fn a_relayed_connection_upgrades_to_a_direct_path() {
    let relay = RelayServer::spawn("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let url = format!("http://{}", relay.addr().expect("the relay is serving"));

    let apiary = ApiaryKey::generate();
    let a = member(&apiary, vec![url.clone()], false).await;
    let b = member(&apiary, vec![url.clone()], false).await;

    // Dialled by id and relay only: it starts on the relay, then the nodes find
    // each other's addresses through it and move to a direct path.
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
    .expect("connected in time")
    .unwrap();
    assert_eq!(echo(&conn, b"hello").await, b"hello");

    let mut direct = conn.path().kind == PathKind::Direct;
    for _ in 0..200 {
        if direct {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        direct = conn.path().kind == PathKind::Direct;
    }
    assert!(direct, "the connection never left the relay");
    assert_eq!(echo(&conn, b"still works").await, b"still works");

    relay.shutdown().await;
}
