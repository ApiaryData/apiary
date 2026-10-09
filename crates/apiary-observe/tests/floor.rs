//! Phase 5a: the dance floor over the simulated network.
//!
//! Real Meshes, real gossip, virtual time: does every Node's copy of the floor
//! converge, does it heal after a partition, do a lost Node's entries fade with no
//! one deleting them, and does a run replay exactly from its seed.

use std::time::Duration;

use apiary_floor::{EntryBody, EntryKey, EntryKind, Floor, GossipConfig, GossipFloor, Presence};
use apiary_net::NodeId;
use apiary_observe::{Colony, Placement, Relay, Sim, SimNetwork};

const NAMES: [&str; 5] = ["cloud", "pi-1", "pi-2", "pod", "pi-3"];

fn placements() -> Vec<(&'static str, Placement)> {
    vec![
        ("cloud", Placement::public("cloud")),
        ("pi-1", Placement::cone("pi")),
        ("pi-2", Placement::cone("pi")),
        ("pod", Placement::public("k8s")),
        ("pi-3", Placement::cone("pi")),
    ]
}

fn presence(bees: u32) -> EntryBody {
    EntryBody::Presence(Presence {
        site: None,
        bees,
        idle_bees: bees,
        temperature: 0.0,
        memory_pressure: 0.0,
        memory_bytes: 0,
    })
}

struct Floors {
    colony: Colony,
    floors: Vec<GossipFloor>,
}

impl Floors {
    fn of(&self, name: &str) -> &GossipFloor {
        let i = NAMES.iter().position(|n| *n == name).unwrap();
        &self.floors[i]
    }

    fn id(&self, name: &str) -> NodeId {
        self.colony.get(name).id()
    }

    /// What `name` can see on its own copy: the Nodes with presence, sorted.
    fn seen_by(&self, name: &str) -> Vec<&'static str> {
        let mut seen: Vec<&'static str> = self
            .of(name)
            .entries(EntryKind::Presence)
            .iter()
            .map(|e| {
                *NAMES
                    .iter()
                    .find(|n| self.id(n) == e.key.node)
                    .expect("a known Node")
            })
            .collect();
        seen.sort();
        seen
    }
}

/// Five Nodes, each admitted to the others on the control protocol (as discovery
/// leaves them), each gossiping and posting its presence.
async fn start(sim: &Sim, net: &SimNetwork) -> Floors {
    net.set_relay(Relay::Tls("cloud".into()));
    let colony = Colony::new(sim, net, &placements());
    for a in colony.members() {
        for b in colony.members() {
            if a.id() < b.id() {
                a.connect(b).await.expect("admitted");
            }
        }
    }
    let floors: Vec<GossipFloor> = colony
        .members()
        .iter()
        .map(|m| {
            let mut cfg = GossipConfig::new("plant");
            cfg.seed = sim.seed();
            let floor = GossipFloor::start(m.mesh.clone(), sim.clock(), cfg);
            floor.publish(
                EntryKey::presence(m.id()),
                presence(4),
                Duration::from_secs(5),
            );
            floor
        })
        .collect();
    Floors { colony, floors }
}

/// Keep every Node's presence fresh, as a Node does, for `secs` seconds.
async fn live(sim: &Sim, f: &Floors, names: &[&str], secs: u64) {
    for _ in 0..secs * 2 {
        for name in names {
            f.of(name).publish(
                EntryKey::presence(f.id(name)),
                presence(4),
                Duration::from_secs(5),
            );
        }
        sim.sleep(Duration::from_millis(500)).await;
    }
}

#[test]
fn every_copy_of_the_floor_converges() {
    let run = Sim::run(1, |sim| async move {
        let net = sim.network();
        let f = start(&sim, &net).await;
        live(&sim, &f, &NAMES, 3).await;
        let mut expected = NAMES.to_vec();
        expected.sort();
        for name in NAMES {
            assert_eq!(f.seen_by(name), expected, "{name}'s floor");
        }
        // Idle floors go quiet: after convergence a round sends nothing new.
        let before = f.of("cloud").counts().entries_sent;
        sim.sleep(Duration::from_millis(900)).await;
        let after = f.of("cloud").counts();
        assert!(after.rounds > 0 && after.failed == 0, "{after:?}");
        assert!(after.entries_sent - before <= 5, "{before} -> {after:?}");
    });
    assert!(run.elapsed < Duration::from_secs(30));
}

#[test]
fn a_lost_nodes_entries_fade_with_no_one_deleting_them() {
    Sim::run(2, |sim| async move {
        let net = sim.network();
        let f = start(&sim, &net).await;
        live(&sim, &f, &NAMES, 3).await;
        assert!(f.seen_by("cloud").contains(&"pod"));

        // The pod dies: it stops posting and gossiping.
        f.of("pod").stop();
        net.isolate(f.id("pod"));
        let living = ["cloud", "pi-1", "pi-2", "pi-3"];
        live(&sim, &f, &living, 8).await;

        for name in living {
            assert!(
                !f.seen_by(name).contains(&"pod"),
                "{name} still sees the pod"
            );
            assert_eq!(f.seen_by(name).len(), 4, "{name}");
        }
    });
}

#[test]
fn the_floor_heals_after_a_partition() {
    Sim::run(3, |sim| async move {
        let net = sim.network();
        let f = start(&sim, &net).await;
        live(&sim, &f, &NAMES, 3).await;

        // The Pis lose the cloud and the pod, and post news of their own.
        let pis = ["pi-1", "pi-2", "pi-3"];
        let far = ["cloud", "pod"];
        // Partitions cut sites: the Pis are one site, the cloud and the pod others.
        net.partition("pi", "cloud");
        net.partition("pi", "k8s");
        for p in pis {
            f.of(p).publish(
                EntryKey::presence(f.id(p)),
                presence(9),
                Duration::from_secs(60),
            );
        }
        for q in far {
            f.of(q).publish(
                EntryKey::presence(f.id(q)),
                presence(2),
                Duration::from_secs(60),
            );
        }
        sim.sleep(Duration::from_secs(3)).await;
        let bees = |viewer: &str, who: &str| {
            let e = f
                .of(viewer)
                .get(&EntryKey::presence(f.id(who)))
                .unwrap_or_else(|| panic!("{viewer} has no entry for {who}"));
            match e.body {
                EntryBody::Presence(p) => p.bees,
                _ => unreachable!(),
            }
        };
        assert_eq!(
            bees("cloud", "pi-1"),
            4,
            "the cloud has not heard the Pis' news"
        );
        assert_eq!(bees("pi-1", "pod"), 4, "nor the Pis the pod's");
        assert_eq!(bees("pi-2", "pi-1"), 9, "within the Pis, news travels");

        net.heal("pi", "cloud");
        net.heal("pi", "k8s");
        // A cut closes connections; discovery would redial, so the test does.
        for p in pis {
            for q in far {
                f.colony
                    .get(p)
                    .connect(f.colony.get(q))
                    .await
                    .expect("redialled");
            }
        }
        sim.sleep(Duration::from_secs(4)).await;
        for viewer in NAMES {
            for pi in pis {
                assert_eq!(bees(viewer, pi), 9, "{viewer} on {pi}");
            }
            for q in far {
                assert_eq!(bees(viewer, q), 2, "{viewer} on {q}");
            }
        }
    });
}

#[test]
fn news_crosses_nodes_that_cannot_reach_each_other() {
    // pi-3 and the cloud never connect, yet pi-3's entry reaches the cloud by way
    // of the Nodes that can reach both.
    Sim::run(4, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let colony = Colony::new(&sim, &net, &placements());
        let link = |a: &str, b: &str| {
            let (a, b) = (colony.get(a), colony.get(b));
            async move { a.connect(b).await.expect("admitted") }
        };
        for (a, b) in [("cloud", "pi-1"), ("pi-1", "pi-3")] {
            link(a, b).await;
        }
        let floors: Vec<(&str, GossipFloor)> = colony
            .members()
            .iter()
            .map(|m| {
                let mut cfg = GossipConfig::new("plant");
                cfg.seed = 4;
                (m.name, GossipFloor::start(m.mesh.clone(), sim.clock(), cfg))
            })
            .collect();
        let of = |n: &str| &floors.iter().find(|(m, _)| *m == n).unwrap().1;
        of("pi-3").publish(
            EntryKey::presence(colony.get("pi-3").id()),
            presence(3),
            Duration::from_secs(30),
        );
        sim.sleep(Duration::from_secs(2)).await;
        assert!(
            of("cloud")
                .get(&EntryKey::presence(colony.get("pi-3").id()))
                .is_some(),
            "the cloud never heard of pi-3"
        );
    });
}

#[test]
fn a_withdrawn_entry_is_gone_everywhere_and_stays_gone() {
    Sim::run(5, |sim| async move {
        let net = sim.network();
        let f = start(&sim, &net).await;
        live(&sim, &f, &NAMES, 2).await;
        let key = EntryKey::presence(f.id("pi-1"));
        f.of("pi-1").withdraw(&key);
        sim.sleep(Duration::from_secs(2)).await;
        for name in NAMES {
            assert!(f.of(name).get(&key).is_none(), "{name}");
        }
    });
}

async fn scenario(sim: Sim) {
    let net = sim.network();
    let f = start(&sim, &net).await;
    live(&sim, &f, &NAMES, 2).await;
    net.partition("pi", "cloud");
    f.of("pi-1").publish(
        EntryKey::presence(f.id("pi-1")),
        presence(8),
        Duration::from_secs(30),
    );
    sim.sleep(Duration::from_secs(2)).await;
    net.heal("pi", "cloud");
    f.colony
        .get("pi-1")
        .connect(f.colony.get("cloud"))
        .await
        .expect("redialled");
    sim.sleep(Duration::from_secs(2)).await;
    for name in NAMES {
        let counts = f.of(name).counts();
        sim.trace().mark(
            name,
            None,
            "floor/counts",
            format!(
                "{} seen, {} pushes, {} sent, {} received",
                f.seen_by(name).len(),
                counts.pushes,
                counts.entries_sent,
                counts.entries_received
            ),
        );
    }
}

#[test]
fn a_run_replays_exactly_from_its_seed() {
    let a = Sim::run(11, scenario);
    let b = Sim::run(11, scenario);
    assert_eq!(a.trace.render(), b.trace.render());
    assert_eq!(a.trace.digest(), b.trace.digest());
}
