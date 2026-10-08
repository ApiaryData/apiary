//! Site measurement over the in-memory network.

use std::sync::Arc;
use std::time::Duration;

use apiary_core::SystemClock;
use apiary_net::{
    ApiaryKey, Caps, ControlRouter, MemNetwork, Mesh, MeshConfig, NodeId, NodeKey, PROBE_SERVICE,
    PathInfo, PathKind, PeerAddr, ProbeService, Protocol, RevocationStore, SiteMonitor, SiteRules,
    Token, TokenSpec, Transport, Trust, Verdict,
};

struct Member {
    id: NodeId,
    mesh: Mesh,
}

fn member(apiary: &ApiaryKey, net: &MemNetwork, site: Option<&str>) -> Member {
    let spec = TokenSpec {
        apiary: "factory".into(),
        colony: "line1".into(),
        caps: Caps::ALL,
        lifetime_secs: 3600,
        node: None,
        bootstrap: vec![],
        relay: None,
    };
    let now = chrono::Utc::now().timestamp();
    let key = NodeKey::generate();
    let transport: Arc<dyn Transport> = Arc::new(net.join(key.id()));
    let mesh = Mesh::new(
        transport,
        MeshConfig {
            trust: Trust {
                apiary: "factory".into(),
                key: apiary.public(),
            },
            token: Token::parse(&apiary.issue(&spec, now)).unwrap(),
            site: site.map(String::from),
            clock: SystemClock::shared(),
        },
        Arc::new(RevocationStore::open(None, apiary.public())),
    );
    mesh.start();
    let router = ControlRouter::new();
    router.add(PROBE_SERVICE, Arc::new(ProbeService));
    mesh.register(Protocol::Control, router);
    Member { id: key.id(), mesh }
}

#[tokio::test]
async fn peers_are_judged_by_label_and_by_measurement() {
    let apiary = ApiaryKey::generate();
    let net = MemNetwork::new();
    let me = member(&apiary, &net, Some("pi-site"));
    let neighbour = member(&apiary, &net, Some("pi-site"));
    let cloud = member(&apiary, &net, Some("cloud"));
    let mislabelled = member(&apiary, &net, Some("pi-site"));
    let unlabelled = member(&apiary, &net, None);

    net.set_path(
        me.id,
        cloud.id,
        PathInfo {
            kind: PathKind::Relayed,
            rtt: Some(Duration::from_millis(70)),
        },
    );
    net.set_path(
        me.id,
        mislabelled.id,
        PathInfo {
            kind: PathKind::Relayed,
            rtt: Some(Duration::from_millis(90)),
        },
    );
    net.set_path(
        me.id,
        unlabelled.id,
        PathInfo {
            kind: PathKind::Direct,
            rtt: Some(Duration::from_micros(400)),
        },
    );

    for peer in [&neighbour, &cloud, &mislabelled, &unlabelled] {
        me.mesh
            .connect(&PeerAddr::id_only(peer.id), Protocol::Control)
            .await
            .unwrap();
    }

    let monitor = SiteMonitor::new(
        me.mesh.clone(),
        Some("pi-site".into()),
        SiteRules::default(),
        256 * 1024,
    );
    monitor.measure_all().await;
    let view = monitor.view();
    let of = |id: NodeId| view.iter().find(|p| p.id == id).unwrap().clone();

    assert_eq!(of(neighbour.id).verdict, Verdict::SameSite);
    assert!(of(neighbour.id).same_site);
    assert_eq!(of(cloud.id).verdict, Verdict::OtherSite);
    assert!(!of(cloud.id).same_site);

    let flagged = of(mislabelled.id);
    assert!(
        matches!(flagged.verdict, Verdict::Contradiction(_)),
        "{:?}",
        flagged.verdict
    );
    assert!(flagged.same_site, "the declared label wins");

    let inferred = of(unlabelled.id);
    assert_eq!(
        inferred.verdict,
        Verdict::SameSite,
        "measurement fills in a missing label"
    );

    // Throughput was probed.
    assert!(
        of(neighbour.id)
            .measured
            .throughput
            .is_some_and(|t| t > 0.0)
    );
}

#[tokio::test]
async fn a_probe_measures_throughput_and_unknown_services_are_refused_politely() {
    let apiary = ApiaryKey::generate();
    let net = MemNetwork::new();
    let (a, b) = (member(&apiary, &net, None), member(&apiary, &net, None));
    let conn = a
        .mesh
        .connect(&PeerAddr::id_only(b.id), Protocol::Control)
        .await
        .unwrap();

    let rate = apiary_net::probe(&conn, 1024 * 1024).await.unwrap();
    assert!(rate > 1.0e5, "a megabyte over memory is quick: {rate}");

    // A service nobody serves gets an error reply, and the connection lives on.
    let mut bi = apiary_net::call(&conn, "nonsense", &()).await.unwrap();
    let reply: apiary_net::Reply<()> = apiary_net::wire::read_frame(&mut bi.recv).await.unwrap();
    assert!(matches!(reply, apiary_net::Reply::Err(ref m) if m.contains("nonsense")));
    assert!(apiary_net::probe(&conn, 1024).await.is_ok());
}
