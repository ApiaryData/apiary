//! Phase 5c: the semi-social colony, on a simulated network and clock.
//!
//! Real Bees choosing roles, a real gossiped floor, real Scouts, Foragers,
//! Followers and Receivers, and the real exchange in a simulated comb store; only
//! the cost of a Patch is made up, as virtual time.

use std::sync::Arc;
use std::time::Duration;

use apiary_core::SeededRng;
use apiary_forage::{ForageCounts, Mode};
use apiary_observe::{ColonySpec, ForageColony, Nat, NodeSpec, Placement, Relay, Shape, Sim};

fn nodes() -> Vec<NodeSpec> {
    vec![
        NodeSpec::new("cloud", Placement::public("cloud"), 4, 1.0),
        NodeSpec::new("pod", Placement::public("k8s"), 2, 1.0),
        NodeSpec::new(
            "pi-1",
            Placement {
                nat: Nat::Cone,
                ..Placement::public("pi")
            },
            4,
            0.25,
        ),
        NodeSpec::new(
            "pi-2",
            Placement {
                nat: Nat::Cone,
                ..Placement::public("pi")
            },
            4,
            0.25,
        ),
    ]
}

const ROLLUP: &str = "SELECT region, sum(x) FROM sensors GROUP BY region";

#[test]
fn a_query_is_foraged_by_the_whole_colony() {
    let run = Sim::run(1, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let colony = ForageColony::start(&sim, &net, &nodes(), ColonySpec::default()).await;
        colony.teach(ROLLUP, Shape::rollup(32, 800, 8, 400, 10_000));
        // Let the floor and the Bees settle.
        sim.sleep(Duration::from_secs(2)).await;

        let started = sim.clock().monotonic();
        let answer = colony.submit("pi-1", ROLLUP);
        let rows = tokio::time::timeout(Duration::from_secs(120), answer)
            .await
            .expect("the colony answers in time")
            .expect("the reply arrives")
            .expect("the query succeeds");
        let took = sim.clock().monotonic() - started;
        assert!(
            rows.batches.is_empty(),
            "the simulated result stage returns no rows"
        );

        let counts: Vec<_> = colony
            .nodes
            .iter()
            .map(|n| (n.name, n.forage.counts()))
            .collect();
        colony.shutdown().await;
        (took, counts)
    });
    let (took, counts) = run.value;
    let won = |name: &str| counts.iter().find(|(n, _)| *n == name).unwrap().1.won;
    let total: u64 = counts.iter().map(|(_, c)| c.won).sum();
    assert_eq!(
        total,
        32 + 8,
        "every Patch was committed exactly once: {counts:?}"
    );
    assert!(
        won("cloud") > 0 && won("pod") > 0,
        "the cloud helped: {counts:?}"
    );
    assert!(won("pi-2") > 0, "the other Pi helped: {counts:?}");
    assert_eq!(
        counts.iter().find(|(n, _)| *n == "pi-1").unwrap().1.planned,
        1
    );
    // The Pi alone, at a quarter of the speed with four Bees, would need a minute.
    assert!(took < Duration::from_secs(30), "took {took:?}: {counts:?}");
}

fn totals(colony: &ForageColony) -> ForageCounts {
    colony
        .nodes
        .iter()
        .map(|n| n.forage.counts())
        .fold(ForageCounts::default(), |a, c| ForageCounts {
            planned: a.planned + c.planned,
            attempts: a.attempts + c.attempts,
            won: a.won + c.won,
            lost: a.lost + c.lost,
            failed: a.failed + c.failed,
            yields: a.yields + c.yields,
            dances: a.dances + c.dances,
            redone: a.redone + c.redone,
            answered: a.answered + c.answered,
            whole: a.whole + c.whole,
            rows: a.rows + c.rows,
        })
}

async fn finish(
    answer: tokio::sync::oneshot::Receiver<apiary_core::Result<apiary_forage::Answer>>,
    within: Duration,
) {
    tokio::time::timeout(within, answer)
        .await
        .expect("the colony answers in time")
        .expect("the reply arrives")
        .expect("the query succeeds");
}

// ---------------------------------------------------------------------------
// Dances beat pull
// ---------------------------------------------------------------------------

const ROLLUPS: &str = "SELECT region, count(*) FROM sensors GROUP BY region";
const AD_HOC: &str = "SELECT * FROM sensors WHERE temp > 30 AND region = 'north'";
const INFERENCE: &str = "SELECT predict(model, features) FROM sensors";

/// Rows the colony produced in a fixed window of an overloaded, mixed workload:
/// rollups (cheap, many rows per Bee-second), ad hoc SQL and inference (dear, few).
fn mixed_workload(seed: u64, mode: Mode) -> (u64, std::collections::BTreeMap<u64, u64>) {
    Sim::run(seed, move |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let mut spec = ColonySpec::default();
        spec.forage.mode = mode;
        let colony = ForageColony::start(&sim, &net, &nodes(), spec).await;
        colony.teach(ROLLUPS, Shape::scan(12, 150, 30_000));
        colony.teach(AD_HOC, Shape::scan(12, 500, 20_000));
        colony.teach(INFERENCE, Shape::scan(12, 1_800, 2_000));
        sim.sleep(Duration::from_secs(2)).await;

        // More work arrives than the colony can do: a query every 200 ms for 40 seconds.
        let mut pick = sim.rng("arrivals", 0);
        let names = ["cloud", "pod", "pi-1", "pi-2"];
        for _ in 0..200 {
            let sql = [ROLLUPS, AD_HOC, INFERENCE][pick.next_below(3) as usize];
            let at = names[pick.next_below(4) as usize];
            drop(colony.submit(at, sql));
            sim.sleep(Duration::from_millis(200)).await;
        }
        let rows = totals(&colony).rows;
        let tally = colony.tally();
        colony.shutdown().await;
        (rows, tally)
    })
    .value
}

#[test]
fn dances_beat_pull_alone_on_a_mixed_workload() {
    let seeds: u64 = std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let (mut better, mut dances_total, mut pull_total) = (0, 0u64, 0u64);
    for seed in 0..seeds {
        let (dances, d_tally) = mixed_workload(seed, Mode::Dances);
        let (pull, p_tally) = mixed_workload(seed, Mode::Pull);
        eprintln!("seed {seed}: dances {dances} rows {d_tally:?}, pull {pull} rows {p_tally:?}");
        better += u32::from(dances > pull);
        dances_total += dances;
        pull_total += pull;
    }
    assert!(
        better >= seeds as u32 - 1,
        "dances beat pull on only {better} of {seeds} seeds"
    );
    assert!(
        dances_total as f64 > pull_total as f64 * 1.15,
        "dances {dances_total} rows against pull {pull_total}"
    );
}

// ---------------------------------------------------------------------------
// Duplicate work
// ---------------------------------------------------------------------------

/// Run `queries` copies of a scan of `patches` Patches of `patch_ms` each, spread
/// across the Nodes, to completion. Returns the colony's totals.
fn batch(
    seed: u64,
    spec: ColonySpec,
    patches: usize,
    patch_ms: u64,
    queries: usize,
) -> ForageCounts {
    Sim::run(seed, move |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let colony = ForageColony::start(&sim, &net, &nodes(), spec).await;
        colony.teach(ROLLUPS, Shape::scan(patches, patch_ms, 10_000));
        sim.sleep(Duration::from_secs(2)).await;
        let names = ["cloud", "pod", "pi-1", "pi-2"];
        let answers: Vec<_> = (0..queries)
            .map(|i| colony.submit(names[i % names.len()], ROLLUPS))
            .collect();
        for answer in answers {
            finish(answer, Duration::from_secs(600)).await;
        }
        let counts = totals(&colony);
        colony.shutdown().await;
        counts
    })
    .value
}

#[test]
fn duplicate_work_stays_under_its_bound_when_patches_outlast_gossip() {
    // Patches run for several gossip rounds (a round is 250 ms; these take 2 s on a
    // reference Node), which is how they are meant to be sized.
    let (mut lost, mut attempts) = (0, 0);
    for seed in 0..4 {
        let c = batch(seed, ColonySpec::default(), 24, 2_000, 6);
        assert_eq!(c.won, 6 * 24, "each Patch committed once: {c:?}");
        lost += c.lost;
        attempts += c.attempts;
    }
    let share = lost as f64 / attempts as f64;
    assert!(
        share < 0.03,
        "{lost} of {attempts} attempts were duplicates ({share:.3})"
    );
}

#[test]
fn a_follower_that_yields_to_the_lower_id_wastes_less_work() {
    // Patches shorter than a gossip round are the bad case: two Bees on different
    // Nodes claim one before either hears of the other. Waiting a round after
    // claiming, and yielding to the lower Node and Bee id, thins the duplicates.
    let run = |yield_above: f64| {
        let mut spec = ColonySpec::default();
        spec.forage.yield_above = yield_above;
        let (mut lost, mut attempts, mut yields) = (0, 0, 0);
        for seed in 0..3 {
            let c = batch(seed, spec.clone(), 48, 120, 6);
            assert_eq!(c.won, 6 * 48, "{c:?}");
            lost += c.lost;
            attempts += c.attempts;
            yields += c.yields;
        }
        (lost as f64 / attempts as f64, yields)
    };
    let (without, none) = run(2.0);
    let (with, yields) = run(-1.0);
    assert_eq!(none, 0);
    assert!(yields > 0);
    assert!(
        with < without,
        "yielding made it worse: {with:.3} against {without:.3}"
    );
}

// ---------------------------------------------------------------------------
// Failure and slowness
// ---------------------------------------------------------------------------

#[test]
fn a_straggler_is_run_again_and_whoever_finishes_first_is_committed() {
    let run = Sim::run(5, |sim| async move {
        let net = sim.network();
        let specs = vec![
            NodeSpec::new("cloud", Placement::public("cloud"), 4, 1.0),
            NodeSpec::new("slow", Placement::public("edge"), 2, 0.02),
        ];
        let colony = ForageColony::start(&sim, &net, &specs, ColonySpec::default()).await;
        colony.teach(ROLLUPS, Shape::scan(12, 200, 1_000));
        sim.sleep(Duration::from_secs(2)).await;
        let started = sim.clock().monotonic();
        // Planned on the cloud Node. The slow Node's Followers claim Patches at once and
        // are the stragglers: a 200 ms Patch takes them ten seconds.
        let answer = colony.submit("cloud", ROLLUPS);
        finish(answer, Duration::from_secs(120)).await;
        let took = sim.clock().monotonic() - started;
        let counts = totals(&colony);
        colony.shutdown().await;
        (took, counts)
    });
    let (took, counts) = run.value;
    assert_eq!(counts.won, 12, "{counts:?}");
    assert!(
        counts.redone > 0,
        "nobody ran a straggler again: {counts:?}"
    );
    assert!(
        took < Duration::from_secs(8),
        "the query waited for its stragglers: {took:?} {counts:?}"
    );
}

#[test]
fn a_node_lost_mid_query_does_not_lose_the_query() {
    let run = Sim::run(6, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let colony = ForageColony::start(&sim, &net, &nodes(), ColonySpec::default()).await;
        colony.teach(ROLLUPS, Shape::rollup(24, 1_000, 6, 500, 10_000));
        sim.sleep(Duration::from_secs(2)).await;
        let answer = colony.submit("cloud", ROLLUPS);
        sim.sleep(Duration::from_millis(1_500)).await;

        // The Pi that is mid-Patch loses power.
        let lost = colony.mesh.get("pi-2").id();
        colony.node("pi-2").bees.shutdown().await;
        colony.node("pi-2").floor.stop();
        net.isolate(lost);

        finish(answer, Duration::from_secs(120)).await;
        let counts = totals(&colony);
        colony.shutdown().await;
        counts
    });
    let counts = run.value;
    assert_eq!(counts.won, 24 + 6, "{counts:?}");
    assert_eq!(counts.answered, 1);
}

// ---------------------------------------------------------------------------
// Pinned stages, concurrency, replay
// ---------------------------------------------------------------------------

#[test]
fn a_stage_that_reads_rows_held_by_one_node_runs_only_there() {
    let run = Sim::run(7, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let colony = ForageColony::start(&sim, &net, &nodes(), ColonySpec::default()).await;
        colony.teach(ROLLUPS, Shape::rollup(8, 500, 8, 500, 1_000).pinned());
        sim.sleep(Duration::from_secs(2)).await;
        let answer = colony.submit("pi-1", ROLLUPS);
        finish(answer, Duration::from_secs(120)).await;
        let counts: Vec<_> = colony
            .nodes
            .iter()
            .map(|n| (n.name, n.forage.counts()))
            .collect();
        colony.shutdown().await;
        counts
    });
    let counts = run.value;
    let total: u64 = counts.iter().map(|(_, c)| c.won).sum();
    assert_eq!(total, 8 + 8, "{counts:?}");
    let pi1 = counts.iter().find(|(n, _)| *n == "pi-1").unwrap().1;
    assert!(
        pi1.won >= 8,
        "pi-1 ran every Patch of the pinned stage: {counts:?}"
    );
}

#[test]
fn a_small_query_is_run_whole_by_the_bee_that_planned_it() {
    let run = Sim::run(9, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let colony = ForageColony::start(&sim, &net, &nodes(), ColonySpec::default()).await;
        colony.teach(
            ROLLUPS,
            Shape::rollup(8, 100, 4, 50, 1_000).reading(1 << 20),
        );
        sim.sleep(Duration::from_secs(2)).await;
        let started = sim.clock().monotonic();
        finish(colony.submit("pi-1", ROLLUPS), Duration::from_secs(60)).await;
        let took = sim.clock().monotonic() - started;
        let counts: Vec<_> = colony
            .nodes
            .iter()
            .map(|n| (n.name, n.forage.counts()))
            .collect();
        colony.shutdown().await;
        (took, counts)
    });
    let (took, counts) = run.value;
    let pi1 = counts.iter().find(|(n, _)| *n == "pi-1").unwrap().1;
    assert_eq!(
        (pi1.planned, pi1.whole, pi1.answered),
        (1, 1, 1),
        "{counts:?}"
    );
    // Nobody was recruited: no dances, no Patches run by the colony.
    assert_eq!(
        counts.iter().map(|(_, c)| c.won + c.dances).sum::<u64>(),
        0,
        "{counts:?}"
    );
    // On a quarter-speed Node: (8 x 100 ms + 4 x 50 ms + 5 ms) / 0.25, about 4 s.
    assert!(took < Duration::from_secs(6), "{took:?}");
}

#[test]
fn queries_planned_on_every_node_all_finish() {
    let c = batch(8, ColonySpec::default(), 10, 600, 8);
    assert_eq!(c.won, 8 * 10);
    assert_eq!(c.answered, 8);
    assert_eq!(c.failed, 0, "{c:?}");
}

async fn scenario(sim: Sim) {
    let net = sim.network();
    net.set_relay(Relay::Tls("cloud".into()));
    let colony = ForageColony::start(&sim, &net, &nodes(), ColonySpec::default()).await;
    colony.teach(ROLLUPS, Shape::rollup(16, 700, 4, 300, 5_000));
    sim.sleep(Duration::from_secs(2)).await;
    let answers: Vec<_> = ["cloud", "pi-1", "pi-2"]
        .iter()
        .map(|n| colony.submit(n, ROLLUPS))
        .collect();
    for a in answers {
        finish(a, Duration::from_secs(300)).await;
    }
    for node in &colony.nodes {
        let c = node.forage.counts();
        sim.trace().mark(
            node.name,
            None,
            "forage/counts",
            format!(
                "{} attempts, {} won, {} lost, {} dances, {} rows",
                c.attempts, c.won, c.lost, c.dances, c.rows
            ),
        );
    }
    colony.shutdown().await;
}

#[test]
fn a_run_replays_exactly_from_its_seed() {
    let a = Sim::run(11, scenario);
    let b = Sim::run(11, scenario);
    assert_eq!(a.trace.render(), b.trace.render());
    assert_eq!(a.trace.digest(), b.trace.digest());
    assert_ne!(a.trace.digest(), Sim::run(12, scenario).trace.digest());
}

// ---------------------------------------------------------------------------
// The exchange protocol
// ---------------------------------------------------------------------------

#[test]
fn batches_travel_between_nodes_over_the_exchange_protocol() {
    let run = Sim::run(12, |sim| async move {
        let net = sim.network();
        net.set_relay(Relay::Tls("cloud".into()));
        let spec = ColonySpec {
            net_exchange: true,
            ..ColonySpec::default()
        };
        let colony = ForageColony::start(&sim, &net, &nodes(), spec).await;
        colony.teach(ROLLUPS, Shape::rollup(24, 600, 6, 300, 10_000));
        sim.sleep(Duration::from_secs(2)).await;
        let answers: Vec<_> = ["cloud", "pi-1", "pod"]
            .iter()
            .map(|n| colony.submit(n, ROLLUPS))
            .collect();
        for a in answers {
            finish(a, Duration::from_secs(300)).await;
        }
        let moved: Vec<_> = colony
            .nodes
            .iter()
            .map(|n| n.exchange.as_ref().unwrap().counts())
            .collect();
        colony.shutdown().await;
        moved
    });
    let moved = run.value;
    let sum = |f: fn(&apiary_forage::NetExchangeCounts) -> u64| moved.iter().map(f).sum::<u64>();
    assert!(
        sum(|c| c.peer) > 0,
        "nothing crossed between Nodes: {moved:?}"
    );
    assert!(sum(|c| c.local) > 0, "{moved:?}");
    // Every producer was up, so the store was never read for an output.
    assert_eq!(sum(|c| c.store), 0, "{moved:?}");
    assert_eq!(sum(|c| c.fell_back), 0, "{moved:?}");
}

#[test]
fn an_output_is_fetched_from_its_producer_and_read_from_the_store_once_it_is_gone() {
    use apiary_comb::custom_store::ObjectStoreBackend;
    use apiary_core::StorageBackend;
    use apiary_forage::NetExchange;
    use apiary_plan::exchange::{Completion, Exchange, PatchKey, StoreExchange};
    use bytes::Bytes;

    Sim::run(13, |sim| async move {
        let net = sim.network();
        let mesh = apiary_observe::Colony::new(
            &sim,
            &net,
            &[
                ("a", Placement::public("one")),
                ("b", Placement::public("two")),
            ],
        );
        let (a, b) = (mesh.get("a"), mesh.get("b"));
        let bucket = sim.store("bucket");
        let store: Arc<dyn StorageBackend> = Arc::new(ObjectStoreBackend::new(Arc::new(bucket)));
        let exchange = |m: &apiary_observe::Member| {
            NetExchange::new(
                m.id(),
                Arc::new(StoreExchange::new(Arc::clone(&store))),
                m.mesh.clone(),
                1 << 20,
            )
        };
        let (ex_a, ex_b) = (exchange(a), exchange(b));
        a.connect(b).await.expect("admitted");

        let key = PatchKey::new("q", 0, 0);
        let data = Bytes::from_static(b"nectar, in arrow");
        ex_a.put(&key, "a-1", 0, data.clone()).await.unwrap();
        ex_a.complete(
            &key,
            &Completion {
                attempt: "a-1".into(),
                node: "ignored".into(),
                rows: vec![1],
                bytes: vec![16],
            },
        )
        .await
        .unwrap();

        // B finds out where it was made and asks A for it.
        let record = ex_b.completion(&key).await.unwrap().unwrap();
        assert_eq!(ex_b.get(&key, &record, 0).await.unwrap(), data);
        let c = ex_b.counts();
        assert_eq!((c.peer, c.store, c.fell_back), (1, 0, 0), "{c:?}");

        // A is cut off. The output is still in the store.
        net.isolate(a.id());
        assert_eq!(ex_b.get(&key, &record, 0).await.unwrap(), data);
        let c = ex_b.counts();
        assert_eq!((c.peer, c.store, c.fell_back), (1, 1, 1), "{c:?}");
        // A itself still has it in memory.
        assert_eq!(ex_a.get(&key, &record, 0).await.unwrap(), data);
        assert_eq!(ex_a.counts().local, 1);
    });
}
