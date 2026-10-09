//! A Bee: one execution slot, its role, its thresholds, its age.
//!
//! A Bee is moved into its own task and never shared. It reconsiders its role
//! only between Patches, answering the stimuli its Node shows it; it stops
//! claiming work when the Node runs hotter than its own cooling threshold, and
//! resumes below that threshold less a hysteresis; and it counts its age in
//! completed Patches, the first few of which are calibration.

use std::time::Duration;

use apiary_core::Env;
use apiary_core::rng::{SeededRng, StdSeededRng};

use crate::roles::{Role, Stimuli};
use crate::thresholds::{ThresholdParams, Thresholds, standard_normal};

/// Where a Bee stops claiming work as the Node heats.
#[derive(Clone, Copy, Debug)]
pub struct CoolingParams {
    /// The median cooling threshold θ_cool, as a Node temperature.
    pub center: f64,
    /// How far below θ_cool the Node must cool before the Bee resumes.
    pub hysteresis: f64,
    /// The lowest a cooling threshold may be.
    pub min: f64,
    /// The highest a cooling threshold may be.
    pub max: f64,
}

impl Default for CoolingParams {
    fn default() -> Self {
        Self {
            center: 0.75,
            hysteresis: 0.05,
            min: 0.4,
            max: 1.0,
        }
    }
}

/// Everything that shapes a Bee's behaviour.
#[derive(Clone, Copy, Debug)]
pub struct BeeParams {
    /// How thresholds are drawn and learn. Its σ is the colony's diversity, and
    /// spreads the cooling thresholds as well.
    pub thresholds: ThresholdParams,
    /// Where Bees stop claiming as the Node heats.
    pub cooling: CoolingParams,
    /// The least time between a Bee's role changes (damping).
    pub dwell: Duration,
    /// How strongly the share of foragers damps the forager stimulus (α).
    pub inhibition: f64,
    /// How many Patches a new Bee spends calibrating before it works.
    pub calibration_patches: u64,
}

impl Default for BeeParams {
    fn default() -> Self {
        Self {
            thresholds: ThresholdParams::default(),
            cooling: CoolingParams::default(),
            dwell: Duration::from_secs(2),
            inhibition: 1.0,
            calibration_patches: 1,
        }
    }
}

/// What a Bee measured about itself in its calibration Patches: the numbers
/// profitability is later judged against.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Calibration {
    /// How fast this Bee scans, in rows per second.
    pub scan_rows_per_sec: f64,
    /// How long a request to the comb store takes from here.
    pub store_latency: Duration,
}

/// What a Bee decided when it reconsidered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decision {
    /// The role it now holds (unchanged if nothing called it).
    pub role: Role,
    /// Whether some role's stimulus engaged it. If not, it stays idle.
    pub engaged: bool,
}

/// One Bee.
pub struct Bee {
    index: usize,
    id: String,
    role: Role,
    thresholds: Thresholds,
    theta_cool: f64,
    hysteresis: f64,
    paused: bool,
    age: u64,
    changed_at: Option<Duration>,
    calibration: Option<Calibration>,
    params: BeeParams,
    rng: StdSeededRng,
}

impl Bee {
    /// Bee number `index` on the Node called `node`. Its thresholds and every
    /// later random choice come from a stream seeded by the Node's seed, the Node
    /// id and the Bee's index, so the same Node replays the same Bees.
    pub fn new(node: &str, index: usize, env: &Env, params: BeeParams) -> Self {
        let mut rng = env.rng(node, index as u64);
        let thresholds = Thresholds::draw(&mut rng, &params.thresholds);
        let cooling = &params.cooling;
        let theta_cool = (cooling.center
            * (params.thresholds.sigma * standard_normal(&mut rng)).exp())
        .clamp(cooling.min, cooling.max);
        Self {
            index,
            id: format!("bee-{index}"),
            role: Role::Forager,
            thresholds,
            theta_cool,
            hysteresis: cooling.hysteresis,
            paused: false,
            age: 0,
            changed_at: None,
            calibration: None,
            params,
            rng,
        }
    }

    /// The Bee's number on its Node.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The Bee's id (`bee-<index>`).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The role the Bee holds (or last held).
    pub fn role(&self) -> Role {
        self.role
    }

    /// Completed Patches, calibration included.
    pub fn age(&self) -> u64 {
        self.age
    }

    /// Thresholds now.
    pub fn thresholds(&self) -> &Thresholds {
        &self.thresholds
    }

    /// The Node temperature above which this Bee stops claiming work.
    pub fn theta_cool(&self) -> f64 {
        self.theta_cool
    }

    /// Whether the Bee has stopped claiming because the Node is hot.
    pub fn is_cooling(&self) -> bool {
        self.paused
    }

    /// What the Bee measured about itself, once it has.
    pub fn calibration(&self) -> Option<Calibration> {
        self.calibration
    }

    /// Whether the Bee still has calibration Patches to do.
    pub fn needs_calibration(&self) -> bool {
        self.age < self.params.calibration_patches
    }

    /// Record a calibration Patch's measurements (if it made any) and count it.
    pub fn finish_calibration(&mut self, measured: Option<Calibration>) {
        if measured.is_some() {
            self.calibration = measured;
        }
        self.age += 1;
    }

    /// Count a finished Patch.
    pub fn finish_patch(&mut self) {
        self.age += 1;
    }

    /// Whether the Bee will claim work at Node temperature `temperature`.
    ///
    /// Above its cooling threshold it stops; it resumes only below the threshold
    /// less the hysteresis, so it does not flutter at the edge. Bees have different
    /// thresholds, so they drop out one at a time as a Node heats.
    pub fn may_claim(&mut self, temperature: f64) -> bool {
        if self.paused {
            if temperature < self.theta_cool - self.hysteresis {
                self.paused = false;
            }
        } else if temperature > self.theta_cool {
            self.paused = true;
        }
        !self.paused
    }

    /// Learn from `dt` seconds spent: the role performed (if any) gets easier.
    pub fn tick(&mut self, doing: Option<Role>, dt: Duration) {
        self.thresholds
            .tick(doing, dt.as_secs_f64(), &self.params.thresholds);
    }

    /// Reconsider the role. Called only between Patches.
    ///
    /// Within its dwell time a Bee may keep its role but not change it. Otherwise
    /// it looks at each role in an order of its own and engages the first whose
    /// probability `s² / (s² + θ²)` it draws; the forager stimulus is first damped
    /// by the share of foragers, `s / (1 + α F̂)`.
    pub fn reconsider(&mut self, stimuli: &Stimuli, now: Duration) -> Decision {
        let may_change = self
            .changed_at
            .is_none_or(|at| now.saturating_sub(at) >= self.params.dwell);
        let mut order: Vec<Role> = if may_change {
            Role::ALL.to_vec()
        } else {
            vec![self.role]
        };
        // A shuffle of its own, so Bees do not all look at the roles in one order.
        for i in (1..order.len()).rev() {
            let j = self.rng.next_below(i as u64 + 1) as usize;
            order.swap(i, j);
        }
        for role in order {
            let mut s = stimuli.get(role);
            if role == Role::Forager {
                s /= 1.0 + self.params.inhibition * stimuli.forager_share;
            }
            if s <= 0.0 {
                continue;
            }
            if self.rng.next_f64() < self.thresholds.engage_probability(role, s) {
                if role != self.role {
                    self.role = role;
                    self.changed_at = Some(now);
                }
                return Decision {
                    role,
                    engaged: true,
                };
            }
        }
        Decision {
            role: self.role,
            engaged: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apiary_core::SystemClock;

    fn env(seed: u64) -> Env {
        Env::new(SystemClock::shared(), seed)
    }

    #[test]
    fn a_node_replays_the_same_bees() {
        let params = BeeParams::default();
        let a = Bee::new("node-1", 2, &env(7), params);
        let b = Bee::new("node-1", 2, &env(7), params);
        assert_eq!(a.thresholds(), b.thresholds());
        assert_eq!(a.theta_cool(), b.theta_cool());
        let other = Bee::new("node-1", 3, &env(7), params);
        assert_ne!(a.thresholds(), other.thresholds());
        assert_ne!(
            a.thresholds(),
            Bee::new("node-2", 2, &env(7), params).thresholds()
        );
    }

    #[test]
    fn at_sigma_zero_every_bee_cools_at_the_same_temperature() {
        let mut params = BeeParams::default();
        params.thresholds.sigma = 0.0;
        let a = Bee::new("n", 0, &env(1), params);
        let b = Bee::new("n", 1, &env(1), params);
        assert_eq!(a.theta_cool(), b.theta_cool());
        assert_eq!(a.theta_cool(), params.cooling.center);
    }

    #[test]
    fn a_bee_stops_above_its_threshold_and_resumes_below_it_less_the_hysteresis() {
        let mut bee = Bee::new("n", 0, &env(1), BeeParams::default());
        let theta = bee.theta_cool();
        assert!(bee.may_claim(theta - 0.01));
        assert!(!bee.may_claim(theta + 0.01), "stops above");
        assert!(
            !bee.may_claim(theta - 0.01),
            "still stopped inside the hysteresis band"
        );
        assert!(bee.may_claim(theta - 0.06), "resumes below θ − h");
    }

    #[test]
    fn a_bee_does_not_change_role_within_its_dwell_time() {
        let mut params = BeeParams::default();
        params.thresholds.sigma = 0.0;
        params.thresholds.median = 0.01; // any stimulus engages
        params.dwell = Duration::from_secs(10);
        let mut bee = Bee::new("n", 0, &env(3), params);

        let ripe = Stimuli::none().with(Role::Ripener, 5.0);
        let first = bee.reconsider(&ripe, Duration::from_secs(1));
        assert_eq!((first.role, first.engaged), (Role::Ripener, true));

        // Something else calls, but the Bee is within its dwell: it keeps its role.
        let scout = Stimuli::none().with(Role::Scout, 5.0);
        let held = bee.reconsider(&scout, Duration::from_secs(5));
        assert_eq!(held.role, Role::Ripener);
        assert!(!held.engaged, "nothing calls the role it holds");

        let moved = bee.reconsider(&scout, Duration::from_secs(12));
        assert_eq!((moved.role, moved.engaged), (Role::Scout, true));
    }

    #[test]
    fn no_stimulus_engages_no_one() {
        let mut bee = Bee::new("n", 0, &env(3), BeeParams::default());
        let d = bee.reconsider(&Stimuli::none(), Duration::from_secs(100));
        assert!(!d.engaged);
    }

    #[test]
    fn foragers_damp_the_forager_stimulus() {
        let mut params = BeeParams::default();
        params.thresholds.sigma = 0.0;
        params.thresholds.median = 1.0;
        params.inhibition = 9.0;
        let count = |share: f64| {
            let mut engaged = 0;
            for seed in 0..2000 {
                let mut bee = Bee::new("n", 0, &env(seed), params);
                let stimuli = Stimuli::none()
                    .with(Role::Forager, 2.0)
                    .with_forager_share(share);
                engaged += usize::from(bee.reconsider(&stimuli, Duration::ZERO).engaged);
            }
            engaged
        };
        // s = 2, θ = 1 gives 0.8; with F̂ = 1 and α = 9, s' = 0.2 gives about 0.04.
        let (alone, crowded) = (count(0.0), count(1.0));
        assert!(alone > 1400 && alone < 1800, "{alone}");
        assert!(crowded < 200, "{crowded}");
    }

    #[test]
    fn calibration_patches_come_first_and_count_toward_age() {
        let mut bee = Bee::new("n", 0, &env(1), BeeParams::default());
        assert!(bee.needs_calibration());
        let measured = Calibration {
            scan_rows_per_sec: 1e6,
            store_latency: Duration::from_millis(3),
        };
        bee.finish_calibration(Some(measured));
        assert!(!bee.needs_calibration());
        assert_eq!(bee.age(), 1);
        assert_eq!(bee.calibration(), Some(measured));
        bee.finish_patch();
        assert_eq!(bee.age(), 2);
    }
}
