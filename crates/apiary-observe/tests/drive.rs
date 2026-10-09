//! The comb host's drive over the simulated network: the same service and client
//! the colony uses over QUIC, from behind a symmetric NAT through a relay, with
//! a lossy link and a relay outage, replaying exactly from its seed.
//!
//! This exercises the drive protocol (object put, get, list, create-if-absent)
//! through the object-store interface. It does not run a Delta table over the
//! drive inside the simulation: Delta's kernel reads the log through a blocking
//! bridge that holds the simulation's one thread while the host, in the same
//! simulation, would have to answer. Delta over the drive is covered over real
//! QUIC by `apiary-net`'s drive tests and the Phase 2 gate.

use std::sync::Arc;
use std::time::Duration;

use apiary_net::{ControlRouter, DRIVE_SERVICE, DriveService, DriveStore, Protocol};
use apiary_observe::{Colony, Link, Nat, Placement, Relay, Sim};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

fn p(s: &str) -> Path {
    Path::from(s)
}

/// What a scenario saw, for comparing runs.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    big_object_intact: bool,
    second_create_refused: bool,
    listing: Vec<String>,
    put_during_outage_failed: bool,
    put_after_outage_ok: bool,
}

async fn day_at_the_plant(sim: Sim) -> Seen {
    let net = sim.network();
    net.set_wan(Link {
        latency: Duration::from_millis(15),
        jitter: Duration::from_millis(5),
        loss: 0.02,
        bandwidth: Some(5_000_000.0),
    });
    net.set_relay(Relay::Plain("cloud".into()));
    let colony = Colony::new(
        &sim,
        &net,
        &[
            ("cloud", Placement::public("cloud")),
            ("host", Placement::cone("pi")),
            (
                "pod",
                Placement {
                    nat: Nat::Symmetric,
                    ..Placement::public("k8s")
                },
            ),
        ],
    );
    let (host, pod) = (colony.get("host"), colony.get("pod"));

    // The host serves its drive; the pod, behind a symmetric NAT, reaches it
    // through the relay.
    let router = ControlRouter::new();
    // The host's drive is in memory: a real disk's blocking reads finish in real
    // time, and the order they finish in would leak into the run.
    let drive: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    router.add(
        DRIVE_SERVICE,
        Arc::new(DriveService::new(Arc::clone(&drive))),
    );
    host.mesh.register(Protocol::Control, router);
    let store = DriveStore::new(pod.mesh.clone(), host.addr());

    // A large object crosses a lossy, capped, relayed link intact.
    let big: Vec<u8> = (0..1_500_000u32).map(|i| (i % 251) as u8).collect();
    store
        .put(&p("cells/big.parquet"), PutPayload::from(big.clone()))
        .await
        .unwrap();
    let back = store.get(&p("cells/big.parquet")).await.unwrap();
    let big_object_intact = back.bytes().await.unwrap() == big;

    // Create-if-absent holds across the network: the second create is refused.
    let create = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    store
        .put_opts(
            &p("log/0001.json"),
            PutPayload::from("first"),
            create.clone(),
        )
        .await
        .unwrap();
    let second_create_refused = matches!(
        store
            .put_opts(&p("log/0001.json"), PutPayload::from("second"), create)
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    );
    store
        .put(&p("log/0002.json"), PutPayload::from("x"))
        .await
        .unwrap();

    let listing: Vec<String> = store
        .list(Some(&p("log")))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap();

    // The relay goes down: the pod cannot reach the drive; the host's disk is untouched.
    net.set_relay_down(true);
    sim.sleep(Duration::from_millis(100)).await;
    let put_during_outage_failed = store
        .put(&p("log/0003.json"), PutPayload::from("lost"))
        .await
        .is_err();
    let landed_anyway = drive.head(&p("log/0003.json")).await.is_ok();
    assert!(!landed_anyway, "a refused write left nothing behind");

    net.set_relay_down(false);
    let put_after_outage_ok = store
        .put(&p("log/0003.json"), PutPayload::from("back"))
        .await
        .is_ok();

    Seen {
        big_object_intact,
        second_create_refused,
        listing,
        put_during_outage_failed,
        put_after_outage_ok,
    }
}

#[test]
fn the_drive_works_across_nat_and_a_relay_and_survives_an_outage() {
    let run = Sim::run(1, day_at_the_plant);
    let seen = run.value;
    assert!(
        seen.big_object_intact,
        "a big object survives loss and the relay"
    );
    assert!(
        seen.second_create_refused,
        "create-if-absent holds over the network"
    );
    assert_eq!(seen.listing, ["log/0001.json", "log/0002.json"]);
    assert!(seen.put_during_outage_failed, "no relay, no drive");
    assert!(seen.put_after_outage_ok, "and it returns with the relay");
    assert!(
        !run.trace.of_kind("net.loss").is_empty(),
        "the lossy link cost something"
    );
}

#[test]
fn a_day_at_the_plant_replays_exactly() {
    let run = |seed| Sim::run(seed, day_at_the_plant);
    let (a, b) = (run(5), run(5));
    let (ea, eb) = (a.trace.events(), b.trace.events());
    if let Some(i) = (0..ea.len().min(eb.len())).find(|i| ea[*i] != eb[*i]) {
        panic!(
            "the runs diverge at event {i} of {} and {}:\n  first:  {:?}\n  second: {:?}",
            ea.len(),
            eb.len(),
            ea[i],
            eb[i]
        );
    }
    assert_eq!(ea.len(), eb.len());
    assert_eq!(a.value, b.value);
    assert_eq!(a.elapsed, b.elapsed);
    assert_ne!(
        a.trace.digest(),
        run(6).trace.digest(),
        "another seed, another day"
    );
}
