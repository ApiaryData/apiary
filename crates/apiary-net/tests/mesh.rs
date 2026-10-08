//! The mesh over the in-memory network: admission, refusal and revocation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use apiary_core::{Clock, ManualClock, SystemClock};
use apiary_net::{
    Admitted, ApiaryKey, Caps, Handler, MemNetwork, Mesh, MeshConfig, NetError, NodeId, NodeKey,
    PathInfo, PathKind, PeerAddr, Protocol, RevocationStore, Revocations, Token, TokenSpec,
    Transport, Trust,
};

const APIARY: &str = "factory";

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

struct Member {
    key: NodeKey,
    mesh: Mesh,
    store: Arc<RevocationStore>,
}

impl Member {
    fn id(&self) -> NodeId {
        self.key.id()
    }
}

struct Site {
    apiary: ApiaryKey,
    net: MemNetwork,
}

impl Site {
    fn new() -> Self {
        Self {
            apiary: ApiaryKey::generate(),
            net: MemNetwork::new(),
        }
    }

    fn spec(&self, colony: &str) -> TokenSpec {
        TokenSpec {
            apiary: APIARY.into(),
            colony: colony.into(),
            caps: Caps::ALL,
            lifetime_secs: 3600,
            node: None,
            bootstrap: vec![],
            relay: None,
        }
    }

    /// A Node with a token for `colony`, accepting connections.
    fn node(&self, colony: &str) -> Member {
        self.node_with(colony, SystemClock::shared(), now())
    }

    fn node_with(&self, colony: &str, clock: Arc<dyn Clock>, issued: i64) -> Member {
        let token = self.apiary.issue(&self.spec(colony), issued);
        self.node_with_token(&token, clock)
    }

    fn node_with_token(&self, token: &str, clock: Arc<dyn Clock>) -> Member {
        let key = NodeKey::generate();
        let transport: Arc<dyn Transport> = Arc::new(self.net.join(key.id()));
        let store = Arc::new(RevocationStore::open(None, self.apiary.public()));
        let mesh = Mesh::new(
            transport,
            MeshConfig {
                trust: Trust {
                    apiary: APIARY.into(),
                    key: self.apiary.public(),
                },
                token: Token::parse(token).unwrap(),
                site: Some("lab".into()),
                clock,
            },
            Arc::clone(&store),
        );
        mesh.start();
        Member { key, mesh, store }
    }
}

fn addr(m: &Member) -> PeerAddr {
    PeerAddr::id_only(m.id())
}

/// Echoes each stream's bytes back, once.
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

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..200 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test]
async fn two_members_admit_each_other_and_talk() {
    let site = Site::new();
    let (a, b) = (site.node("line1"), site.node("line1"));
    b.mesh.register(Protocol::Control, Arc::new(Echo));

    let conn = a.mesh.connect(&addr(&b), Protocol::Control).await.unwrap();
    assert_eq!(conn.peer, b.id());
    assert_eq!(conn.membership.colony, "line1");
    assert_eq!(conn.site.as_deref(), Some("lab"));

    let mut bi = conn.open_bi().await.unwrap();
    bi.send.write_all(b"hello bees").await.unwrap();
    bi.send.shutdown().await.unwrap();
    let mut echoed = Vec::new();
    bi.recv.read_to_end(&mut echoed).await.unwrap();
    assert_eq!(echoed, b"hello bees");

    // Both sides know each other.
    eventually("b to list a", || {
        b.mesh.peers().iter().any(|p| p.id == a.id())
    })
    .await;
    let seen = a.mesh.peers();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].id, b.id());
    assert_eq!(seen[0].protocols, vec![Protocol::Control]);

    // A second connect reuses the first.
    let again = a.mesh.connect(&addr(&b), Protocol::Control).await.unwrap();
    assert!(Arc::ptr_eq(&conn, &again));
}

#[tokio::test]
async fn a_token_from_the_wrong_key_is_refused_by_both_sides() {
    let site = Site::new();
    let b = site.node("line1");
    b.mesh.register(Protocol::Control, Arc::new(Echo));

    // An outsider with a token signed by some other key.
    let rogue = ApiaryKey::generate();
    let forged = rogue.issue(&site.spec("line1"), now());
    let outsider = site.node_with_token(&forged, SystemClock::shared());

    let err = outsider
        .mesh
        .connect(&addr(&b), Protocol::Control)
        .await
        .err()
        .expect("the outsider is refused");
    match err {
        NetError::Refused(reason) => assert!(reason.contains("signature"), "{reason}"),
        other => panic!("unexpected: {other}"),
    }
    assert!(
        b.mesh.peers().is_empty(),
        "the outsider never became a peer"
    );

    // And the other way: a member will not admit an outsider's server.
    outsider.mesh.register(Protocol::Control, Arc::new(Echo));
    let err = b
        .mesh
        .connect(&addr(&outsider), Protocol::Control)
        .await
        .err()
        .expect("the member refuses the outsider's token");
    assert!(matches!(err, NetError::Refused(_)));
}

#[tokio::test]
async fn an_expired_token_is_refused() {
    let site = Site::new();
    let b = site.node("line1");
    b.mesh.register(Protocol::Control, Arc::new(Echo));
    // Issued two hours ago for an hour.
    let stale = site.node_with("line1", SystemClock::shared(), now() - 7200);

    let err = stale
        .mesh
        .connect(&addr(&b), Protocol::Control)
        .await
        .err()
        .expect("an expired token is refused");
    match err {
        NetError::Refused(reason) => assert!(reason.contains("expired"), "{reason}"),
        other => panic!("unexpected: {other}"),
    }
}

#[tokio::test]
async fn a_node_booted_without_network_time_still_joins() {
    // A Pi whose clock says 1970. Its own token is fine, and so is the peer's:
    // it cannot tell whether either has expired, so it does not refuse them.
    let site = Site::new();
    let good = site.node("line1");
    good.mesh.register(Protocol::Control, Arc::new(Echo));

    let lost = chrono::DateTime::from_timestamp(5, 0).unwrap();
    let clock = Arc::new(ManualClock::new(lost));
    let pi = site.node_with("line1", clock, now());

    let conn = pi
        .mesh
        .connect(&addr(&good), Protocol::Control)
        .await
        .unwrap();
    assert!(conn.membership.clock_suspect, "accepted, and marked");
    assert_eq!(pi.mesh.peers().len(), 1);
}

#[tokio::test]
async fn a_token_bound_to_another_node_is_refused() {
    let site = Site::new();
    let b = site.node("line1");
    b.mesh.register(Protocol::Control, Arc::new(Echo));

    let someone_else = NodeKey::generate().id();
    let mut spec = site.spec("line1");
    spec.node = Some(someone_else);
    let stolen = site.apiary.issue(&spec, now());
    let thief = site.node_with_token(&stolen, SystemClock::shared());

    let err = thief
        .mesh
        .connect(&addr(&b), Protocol::Control)
        .await
        .err()
        .expect("a bound token is useless to another key");
    assert!(matches!(err, NetError::Refused(_)));
}

#[tokio::test]
async fn a_revoked_key_is_refused_and_its_live_connections_close() {
    let site = Site::new();
    let (a, b, victim) = (site.node("line1"), site.node("line1"), site.node("line1"));
    for m in [&a, &b] {
        m.mesh.register(Protocol::Control, Arc::new(Echo));
    }
    victim.mesh.register(Protocol::Control, Arc::new(Echo));

    // The victim is a member in good standing, and connected to b.
    let live = victim
        .mesh
        .connect(&addr(&b), Protocol::Control)
        .await
        .unwrap();
    assert!(!live.is_closed());
    eventually("b to see the victim", || !b.mesh.peers().is_empty()).await;

    // The Beekeeper revokes its key and hands the list to b only.
    let list = site
        .apiary
        .revoke(&Revocations::empty(), &[victim.id()], &[], now());
    assert!(b.mesh.apply_revocations(list).unwrap());

    eventually("the victim's connection to close", || live.is_closed()).await;
    assert!(b.mesh.peers().is_empty());

    // It cannot get back in at b ...
    let err = victim
        .mesh
        .connect(&addr(&b), Protocol::Control)
        .await
        .err()
        .expect("revoked");
    assert!(matches!(err, NetError::Refused(_)), "{err}");

    // ... and a, which was never told directly, learns the list from b the
    // moment they connect, and then refuses the victim too.
    a.mesh.connect(&addr(&b), Protocol::Control).await.unwrap();
    eventually("a to learn of the revocation", || {
        a.store.is_node_revoked(&victim.id())
    })
    .await;
    let err = victim
        .mesh
        .connect(&addr(&a), Protocol::Control)
        .await
        .err()
        .expect("revoked");
    assert!(matches!(err, NetError::Refused(_)), "{err}");
}

#[tokio::test]
async fn a_revocation_spreads_across_a_chain_of_peers() {
    // a -- b -- c, and the victim is connected to c only. The list is given to a.
    let site = Site::new();
    let (a, b, c, victim) = (
        site.node("line1"),
        site.node("line1"),
        site.node("line1"),
        site.node("line1"),
    );
    for m in [&a, &b, &c, &victim] {
        m.mesh.register(Protocol::Control, Arc::new(Echo));
    }
    a.mesh.connect(&addr(&b), Protocol::Control).await.unwrap();
    b.mesh.connect(&addr(&c), Protocol::Control).await.unwrap();
    let live = victim
        .mesh
        .connect(&addr(&c), Protocol::Control)
        .await
        .unwrap();

    let list = site
        .apiary
        .revoke(&Revocations::empty(), &[victim.id()], &[], now());
    a.mesh.apply_revocations(list).unwrap();

    eventually("c to cut the victim off", || live.is_closed()).await;
    assert!(b.store.is_node_revoked(&victim.id()));
    assert!(c.store.is_node_revoked(&victim.id()));
}

#[tokio::test]
async fn a_site_offline_during_a_revocation_learns_it_on_reconnecting() {
    let site = Site::new();
    let (hq, offline, victim) = (site.node("line1"), site.node("line1"), site.node("line1"));
    hq.mesh.register(Protocol::Control, Arc::new(Echo));
    offline.mesh.register(Protocol::Control, Arc::new(Echo));
    victim.mesh.register(Protocol::Control, Arc::new(Echo));

    // The site is cut off while the Beekeeper revokes a key.
    site.net.partition(offline.id(), hq.id());
    let list = site
        .apiary
        .revoke(&Revocations::empty(), &[victim.id()], &[], now());
    hq.mesh.apply_revocations(list).unwrap();
    assert!(!offline.store.is_node_revoked(&victim.id()));

    // Days later the link returns. The first connection carries the list.
    site.net.heal(offline.id(), hq.id());
    offline
        .mesh
        .connect(&addr(&hq), Protocol::Control)
        .await
        .unwrap();
    assert!(offline.store.is_node_revoked(&victim.id()));
    assert!(
        offline
            .mesh
            .connect(&addr(&victim), Protocol::Control)
            .await
            .is_err()
            || victim
                .mesh
                .connect(&addr(&offline), Protocol::Control)
                .await
                .is_err()
    );
}

#[tokio::test]
async fn a_forged_revocation_list_is_ignored() {
    let site = Site::new();
    let (a, b) = (site.node("line1"), site.node("line1"));
    b.mesh.register(Protocol::Control, Arc::new(Echo));
    a.mesh.connect(&addr(&b), Protocol::Control).await.unwrap();

    let forger = ApiaryKey::generate();
    let forged = forger.revoke(&Revocations::empty(), &[b.id()], &[], now());
    assert!(a.mesh.apply_revocations(forged).is_err());
    assert!(!a.store.is_node_revoked(&b.id()));
    assert_eq!(a.mesh.peers().len(), 1, "the real peer stays connected");
}

#[tokio::test]
async fn a_peer_in_another_colony_is_admitted_and_labelled() {
    let site = Site::new();
    let (a, far) = (site.node("line1"), site.node("line2"));
    far.mesh.register(Protocol::Control, Arc::new(Echo));
    let conn = a
        .mesh
        .connect(&addr(&far), Protocol::Control)
        .await
        .unwrap();
    assert_eq!(conn.membership.colony, "line2");
}

#[tokio::test]
async fn paths_are_reported_and_a_partition_makes_a_peer_unreachable() {
    let site = Site::new();
    let (a, b) = (site.node("line1"), site.node("line1"));
    b.mesh.register(Protocol::Control, Arc::new(Echo));

    site.net.set_path(
        a.id(),
        b.id(),
        PathInfo {
            kind: PathKind::Relayed,
            rtt: Some(Duration::from_millis(80)),
        },
    );
    let conn = a.mesh.connect(&addr(&b), Protocol::Control).await.unwrap();
    assert_eq!(conn.path().kind, PathKind::Relayed);
    assert_eq!(a.mesh.peers()[0].path.rtt, Some(Duration::from_millis(80)));

    site.net.partition(a.id(), b.id());
    eventually("the connection to close", || conn.is_closed()).await;
    let err = a
        .mesh
        .connect(&addr(&b), Protocol::Control)
        .await
        .err()
        .expect("partitioned");
    assert!(matches!(err, NetError::Unreachable(_)));
}

#[tokio::test]
async fn an_unregistered_protocol_gets_no_handler_but_a_refused_peer_never_reaches_one() {
    let site = Site::new();
    let b = site.node("line1");
    // b serves Control only. A Gossip dial is admitted (membership is checked
    // first) but nothing answers streams on it.
    b.mesh.register(Protocol::Control, Arc::new(Echo));
    let a = site.node("line1");
    let gossip = a.mesh.connect(&addr(&b), Protocol::Gossip).await.unwrap();
    assert_eq!(gossip.protocol(), Protocol::Gossip);
}

#[tokio::test]
async fn a_node_serves_streams_on_a_connection_it_dialled() {
    // Two nodes find each other at once, and the connection the host dialled is
    // the one the client ends up using to reach the host. The host must serve
    // requests that arrive on a connection it opened, not only ones it accepted.
    let site = Site::new();
    let (host, client) = (site.node("line1"), site.node("line1"));
    host.mesh.register(Protocol::Control, Arc::new(Echo));
    client.mesh.register(Protocol::Control, Arc::new(Echo));

    host.mesh
        .connect(&addr(&client), Protocol::Control)
        .await
        .unwrap();
    eventually("the client to hold the connection", || {
        client
            .mesh
            .peer_conn(host.id(), Protocol::Control)
            .is_some()
    })
    .await;

    // The client never dialled: it uses the connection it was given.
    let conn = client.mesh.peer_conn(host.id(), Protocol::Control).unwrap();
    let mut bi = conn.open_bi().await.unwrap();
    bi.send.write_all(b"served").await.unwrap();
    bi.send.shutdown().await.unwrap();
    let mut echoed = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), bi.recv.read_to_end(&mut echoed))
        .await
        .expect("the host answered a stream on a connection it dialled")
        .unwrap();
    assert_eq!(echoed, b"served");
}
