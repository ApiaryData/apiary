//! The Jones experiment, in miniature.
//!
//! Jones and colleagues found brood temperature less stable in genetically uniform
//! colonies: when every worker switches at the same temperature, they all stop
//! fanning together, the nest overheats, and they all resume together. Here a Node
//! runs the same arrivals twice, once with every Bee alike (σ = 0) and once with
//! the usual spread (σ > 0), and the variance of the Node's temperature *T* is
//! compared. The Bees, their roles, their thresholds, their cooling and the Node's
//! temperature are the real `apiary-colony`; only the work is synthetic.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apiary_colony::{BeeContext, Colony, ColonyConfig, Duties, Role, Stimuli, ThresholdParams};
use apiary_core::rng::SeededRng;
use apiary_observe::Sim;
use async_trait::async_trait;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

/// Patches waiting for a forager, each a span of work.
struct Arrivals {
    waiting: Mutex<VecDeque<Duration>>,
    done: Mutex<u64>,
}

#[async_trait]
impl Duties for Arrivals {
    fn stimuli(&self) -> Stimuli {
        Stimuli::none().with(Role::Forager, self.waiting.lock().unwrap().len() as f64)
    }

    fn claimed_waiting(&self) -> usize {
        // A forager starts the Patch it claims at once.
        0
    }

    async fn perform(&self, role: Role, bee: &BeeContext) -> bool {
        if role != Role::Forager {
            return false;
        }
        let Some(work) = self.waiting.lock().unwrap().pop_front() else {
            return false;
        };
        bee.clock().sleep(work).await;
        *self.done.lock().unwrap() += 1;
        true
    }
}

/// What one run of a Node under load looked like.
struct Observed {
    temperatures: Vec<f64>,
    patches_done: u64,
    arrived: u64,
    longest_queue: usize,
}

impl Observed {
    fn mean(&self) -> f64 {
        self.temperatures.iter().sum::<f64>() / self.temperatures.len() as f64
    }

    fn variance(&self) -> f64 {
        let mean = self.mean();
        self.temperatures
            .iter()
            .map(|t| (t - mean).powi(2))
            .sum::<f64>()
            / self.temperatures.len() as f64
    }
}

const BEES: usize = 8;
const SPAN: Duration = Duration::from_secs(300);

/// One Node, `BEES` Bees, an open arrival stream near its capacity, run for
/// `SPAN` of virtual time. The arrivals depend on the seed alone; `sigma` only
/// changes the Bees.
fn run_node(seed: u64, sigma: f64) -> Observed {
    Sim::run(seed, move |sim| async move {
        let duties = Arc::new(Arrivals {
            waiting: Mutex::default(),
            done: Mutex::default(),
        });
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let mut config = ColonyConfig::new("jones", BEES, 64 << 20, pool);
        config.params.thresholds = ThresholdParams {
            sigma,
            ..config.params.thresholds
        };
        let colony = Colony::start(config, &sim.env(), Arc::clone(&duties) as _, None);

        // Arrivals: exponential gaps, fixed-length Patches, offered load about 0.45
        // of what eight Bees can do: warm enough that Bees reach their cooling
        // thresholds, light enough that the Node keeps up.
        let mut arrivals = sim.rng("arrivals", 0);
        let work = Duration::from_millis(200);
        let mean_gap = work.as_secs_f64() / (BEES as f64 * 0.45);
        let mut arrived = 0u64;
        let mut temperatures = Vec::new();
        let mut longest_queue = 0;
        let mut next_arrival = Duration::ZERO;
        let tick = Duration::from_millis(20);
        let mut elapsed = Duration::ZERO;
        while elapsed < SPAN {
            while next_arrival <= elapsed {
                duties.waiting.lock().unwrap().push_back(work);
                arrived += 1;
                let gap = -mean_gap * (1.0 - arrivals.next_f64()).ln();
                next_arrival += Duration::from_secs_f64(gap);
            }
            colony.wake();
            sim.sleep(tick).await;
            elapsed += tick;
            // Skip the first seconds: the Bees calibrate and warm up.
            if elapsed > Duration::from_secs(10) {
                temperatures.push(colony.temperature());
            }
            longest_queue = longest_queue.max(duties.waiting.lock().unwrap().len());
        }
        colony.shutdown().await;
        let patches_done = *duties.done.lock().unwrap();
        Observed {
            temperatures,
            patches_done,
            arrived,
            longest_queue,
        }
    })
    .value
}

#[test]
fn diversity_steadies_a_nodes_temperature() {
    let seeds = 16;
    let (mut lower, mut higher, mut total_uniform, mut total_diverse) = (0, 0, 0.0, 0.0);
    for seed in 0..seeds {
        let uniform = run_node(seed, 0.0);
        let diverse = run_node(seed, 0.5);
        println!(
            "seed {seed:2}: σ=0   var T {:.5} (mean {:.3}, longest queue {:3}) | σ=0.5 var T {:.5} (mean {:.3}, longest queue {:3})",
            uniform.variance(),
            uniform.mean(),
            uniform.longest_queue,
            diverse.variance(),
            diverse.mean(),
            diverse.longest_queue,
        );
        // Every Patch that arrived is served either way: diversity costs queueing,
        // not work.
        for run in [&uniform, &diverse] {
            assert!(
                run.patches_done + 100 >= run.arrived,
                "{}/{}",
                run.patches_done,
                run.arrived
            );
        }
        match diverse.variance().partial_cmp(&uniform.variance()).unwrap() {
            std::cmp::Ordering::Less => lower += 1,
            std::cmp::Ordering::Greater => higher += 1,
            std::cmp::Ordering::Equal => {}
        }
        total_uniform += uniform.variance();
        total_diverse += diverse.variance();
    }
    // Some seeds tie exactly: a seed whose diverse Bees happen to leave six of
    // eight working caps the Node at the same temperature as σ = 0 does.
    assert_eq!(higher, 0, "σ > 0 is never less steady than σ = 0");
    assert!(
        lower * 4 >= seeds * 3,
        "and steadier on most seeds: {lower} of {seeds}"
    );
    assert!(
        total_diverse < total_uniform * 0.85,
        "and steadier overall: {total_diverse:.5} against {total_uniform:.5}"
    );
}

#[test]
fn a_colony_under_load_replays_exactly() {
    let a = run_node(3, 0.5);
    let b = run_node(3, 0.5);
    assert_eq!(
        a.temperatures, b.temperatures,
        "the same Bees, the same arrivals, the same T"
    );
    assert_eq!(a.patches_done, b.patches_done);
    assert_ne!(a.temperatures, run_node(4, 0.5).temperatures);
}
