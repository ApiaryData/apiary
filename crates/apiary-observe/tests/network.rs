//! Phase 3 gate, second half: the colony's real membership layer runs unchanged
//! over a simulated network, including its NAT, relay and outage paths, and a
//! run replays exactly from its seed.

use std::sync::Arc;
use std::time::Duration;

use apiary_net::{
    Admitted, Caps, Handler, Mesh, MeshConfig, NetError, NodeId, NodeKey, PathKind, PeerAddr,
    Protocol, RevocationStore, Token, TokenSpec, Transport, Trust,
};
use apiary_observe::{Link, Nat, Placement, Relay, Sim, SimNetwork, wall_origin};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const APIARY: &str = "plant";

struct Node {
    key: NodeKey,
    mesh: Mesh,
}

impl Node {
    fn id(&self) -> NodeId {
        self.key.id()
    }

    async fn connect(&self, to: &Node) -> Result<Arc<Admitted>, NetError> {
        self.mesh
            .connect(&PeerAddr::id_only(to.id()), Protocol::Control)
            .await
    }
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

/// A colony of real `Mesh`es on the simulated network.
struct Colony {
    nodes: Vec<(&'static str, Node)>,
}

impl Colony {
    fn new(sim: &Sim, net: &SimNetwork, members: &[(&'static str, Placement)]) -> Self {
        let apiary = sim.apiary_key();
        let issued = wall_origin().timestamp();
        let nodes = members
            .iter()
            .map(|(name, placement)| {
                let key = sim.node_key(name);
                let spec = TokenSpec {
                    apiary: APIARY.into(),
                    colony: "plant".into(),
                    caps: Caps::ALL,
                    lifetime_secs: 86_400,
                    node: None,
                    bootstrap: vec![],
                    relay: None,
                };
                let token = Token::parse(&apiary.issue(&spec, issued)).unwrap();
                let transport: Arc<dyn Transport> =
                    Arc::new(net.join(name, key.id(), placement.clone()));
                let mesh = Mesh::new(
                    transport,
                    MeshConfig {
                        trust: Trust {
                            apiary: APIARY.into(),
                            key: apiary.public(),
                        },
                        token,
                        site: Some(placement.site.clone()),
                        clock: sim.clock(),
                    },
                    Arc::new(RevocationStore::open(None, apiary.public())),
                );
                mesh.register(Protocol::Control, Arc::new(Echo));
                mesh.start();
                (*name, Node { key, mesh })
            })
            .collect();
        Self { nodes }
    }

    fn get(&self, name: &str) -> &Node {
        &self
            .nodes
            .iter()
            .find(|(n, _)| *n == name)
            .unwrap_or_else(|| panic!("no node {name}"))
            .1
    }
}

fn members(pod: Nat) -> Vec<(&'static str, Placement)> {
    vec![
        ("cloud", Placement::public("cloud")),
        ("pi-1", Placement::cone("pi")),
        ("pi-2", Placement::cone("pi")),
        (
            "pod",
            Placement {
                nat: pod,
                ..Placement::public("k8s")
            },
        ),
    ]
}

async fn echo(conn: &Admitted, message: &[u8]) -> Vec<u8> {
    let mut bi = conn.open_bi().await.unwrap();
    bi.send.write_all(message).await.unwrap();
    bi.send.shutdown().await.unwrap();
    let mut back = Vec::new();
    bi.recv.read_to_end(&mut back).await.unwrap();
    back
}

#[test]
fn paths_follow_the_nat_the_way_the_gate_measured() {
    // Cone NATs and a relay with address discovery: relayed at first, then punched.
    let run = Sim::run(1, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let colony = Colony::new(&sim, &net, &members(Nat::Cone));
        let (pi1, pi2, cloud, pod) = (
            colony.get("pi-1"),
            colony.get("pi-2"),
            colony.get("cloud"),
            colony.get("pod"),
        );

        let within_site = pi1.connect(pi2).await.unwrap();
        assert_eq!(within_site.path().kind, PathKind::Direct, "same site");
        let to_public = pi1.connect(cloud).await.unwrap();
        assert_eq!(to_public.path().kind, PathKind::Direct, "dialling out");

        let across = pod.connect(pi1).await.unwrap();
        assert_eq!(
            across.path().kind,
            PathKind::Relayed,
            "two NATs: relay first"
        );
        assert_eq!(echo(&across, b"over the relay").await, b"over the relay");
        sim.sleep(Duration::from_secs(5)).await;
        assert_eq!(across.path().kind, PathKind::Direct, "then punched through");
        assert_eq!(echo(&across, b"direct now").await, b"direct now");
    });
    assert!(
        run.trace
            .of_kind("net.path")
            .iter()
            .any(|e| e.detail.contains("now direct")),
        "the path change is traced:\n{}",
        run.trace.render()
    );

    // A symmetric NAT cannot be punched: it stays on the relay however long it waits.
    Sim::run(2, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let colony = Colony::new(&sim, &net, &members(Nat::Symmetric));
        let across = colony.get("pod").connect(colony.get("pi-1")).await.unwrap();
        sim.sleep(Duration::from_secs(60)).await;
        assert_eq!(across.path().kind, PathKind::Relayed);
        assert_eq!(echo(&across, b"still relayed").await, b"still relayed");
    });

    // A plain-HTTP relay forwards but cannot help punch, even through cone NATs.
    Sim::run(3, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Plain("cloud".into()));
        let colony = Colony::new(&sim, &net, &members(Nat::Cone));
        let across = colony.get("pod").connect(colony.get("pi-1")).await.unwrap();
        sim.sleep(Duration::from_secs(60)).await;
        assert_eq!(across.path().kind, PathKind::Relayed);
    });

    // No relay: two NATed sites cannot reach each other, and finding that out takes time.
    let run = Sim::run(4, |sim| async move {
        let net = sim.network();
        let colony = Colony::new(&sim, &net, &members(Nat::Cone));
        colony
            .get("pod")
            .connect(colony.get("pi-1"))
            .await
            .err()
            .expect("no relay, no route")
    });
    assert!(matches!(run.value, NetError::Unreachable(_)));
    assert!(run.elapsed >= Duration::from_secs(5), "{:?}", run.elapsed);
}

#[test]
fn a_relayed_path_costs_the_extra_legs() {
    let run = Sim::run(5, |sim| async move {
        let net = sim.network();
        net.set_wan(Link::with_latency(Duration::from_millis(20)));
        net.set_relay(Relay::Plain("cloud".into()));
        let colony = Colony::new(&sim, &net, &members(Nat::Cone));
        let (cloud, pod, pi1) = (colony.get("cloud"), colony.get("pod"), colony.get("pi-1"));
        let direct = pod.connect(cloud).await.unwrap();
        let relayed = pod.connect(pi1).await.unwrap();

        let time = |conn: Arc<Admitted>, sim: Sim| async move {
            let t0 = sim.clock().monotonic();
            echo(&conn, b"ping").await;
            sim.clock().monotonic() - t0
        };
        (time(direct, sim.clone()).await, time(relayed, sim).await)
    });
    let (direct, relayed) = run.value;
    assert!(
        relayed >= direct * 2 - Duration::from_millis(5),
        "two legs cost about twice one: direct {direct:?}, relayed {relayed:?}"
    );
}

#[test]
fn partitions_cut_connections_and_heal_lets_them_back() {
    Sim::run(6, |sim| async move {
        let net = sim.network();
        let colony = Colony::new(&sim, &net, &members(Nat::Cone));
        let (pi1, cloud) = (colony.get("pi-1"), colony.get("cloud"));
        let conn = pi1.connect(cloud).await.unwrap();
        assert_eq!(echo(&conn, b"before").await, b"before");

        net.partition("pi", "cloud");
        sim.sleep(Duration::from_millis(50)).await;
        assert!(conn.is_closed(), "a cut closes what crosses it");
        assert!(
            matches!(pi1.connect(cloud).await, Err(NetError::Unreachable(_))),
            "and refuses new connections"
        );

        net.heal("pi", "cloud");
        let again = pi1.connect(cloud).await.unwrap();
        assert_eq!(echo(&again, b"after").await, b"after");
    });
}

#[test]
fn a_relay_outage_breaks_relayed_paths_and_leaves_direct_ones() {
    Sim::run(7, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Plain("cloud".into()));
        let colony = Colony::new(&sim, &net, &members(Nat::Cone));
        let (pod, pi1, cloud) = (colony.get("pod"), colony.get("pi-1"), colony.get("cloud"));
        let relayed = pod.connect(pi1).await.unwrap();
        let direct = pod.connect(cloud).await.unwrap();

        net.set_relay_down(true);
        sim.sleep(Duration::from_millis(50)).await;
        assert!(relayed.is_closed());
        assert!(!direct.is_closed());
        assert_eq!(echo(&direct, b"still up").await, b"still up");
        assert!(
            pod.connect(pi1).await.is_err(),
            "no new relayed connections"
        );

        net.set_relay_down(false);
        let back = pod.connect(pi1).await.unwrap();
        assert_eq!(echo(&back, b"relay is back").await, b"relay is back");
    });
}

/// A messy day on the network, summarised.
async fn messy_day(sim: Sim) -> (bool, bool, usize) {
    let net = sim.network();
    net.set_wan(Link {
        latency: Duration::from_millis(15),
        jitter: Duration::from_millis(10),
        loss: 0.05,
        bandwidth: Some(2_000_000.0),
    });
    net.set_relay(Relay::Tls("cloud".into()));
    let colony = Colony::new(&sim, &net, &members(Nat::Cone));
    let (pod, pi1, cloud) = (colony.get("pod"), colony.get("pi-1"), colony.get("cloud"));

    let across = pod.connect(pi1).await.unwrap();
    let payload = vec![7u8; 200_000];
    let relayed_ok = echo(&across, &payload).await == payload;
    sim.sleep(Duration::from_secs(4)).await;
    let direct_now = across.path().kind == PathKind::Direct;

    net.partition("pi", "cloud");
    let refused = pi1.connect(cloud).await.is_err();
    sim.sleep(Duration::from_secs(10)).await;
    net.heal("pi", "cloud");
    let healed = pi1.connect(cloud).await.is_ok();
    (
        relayed_ok && direct_now,
        refused && healed,
        sim.trace().len(),
    )
}

#[test]
fn a_run_replays_exactly_from_its_seed() {
    let a = Sim::run(31, messy_day);
    let b = Sim::run(31, messy_day);
    assert!(a.value.0);
    assert!(a.value.1);
    let (ea, eb) = (a.trace.events(), b.trace.events());
    if let Some(i) = (0..ea.len().min(eb.len())).find(|i| ea[*i] != eb[*i]) {
        panic!(
            "the runs diverge at event {i}:\n  first:  {:?}\n  second: {:?}",
            ea[i], eb[i]
        );
    }
    assert_eq!(ea.len(), eb.len());
    assert_eq!(a.elapsed, b.elapsed);
    assert_eq!(a.trace.digest(), b.trace.digest());
    assert!(
        !a.trace.of_kind("net.loss").is_empty(),
        "the lossy link lost something:
{}",
        a.trace.render()
    );

    // Another seed draws other jitter and loss.
    assert_ne!(a.trace.digest(), Sim::run(32, messy_day).trace.digest());
}
