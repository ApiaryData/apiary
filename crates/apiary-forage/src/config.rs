//! How a colony forages.

use std::time::Duration;

/// How idle Bees choose among advertised work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Dances carry profitability: a Forager that finishes a Patch dances in
    /// proportion to how good it was for her, and a Follower picks among dances
    /// in proportion to their strength. Effort flows to the best work.
    Dances,
    /// Idle Bees pull: every advertisement counts the same, and a Follower takes a
    /// random group with Patches left. The baseline dances are measured against.
    Pull,
}

/// The knobs of foraging.
#[derive(Clone, Debug)]
pub struct ForageConfig {
    /// How idle Bees choose.
    pub mode: Mode,
    /// The most dances a Follower weighs: she follows one chosen in proportion to
    /// its strength from all the dances on her Node's floor copy, or from a uniform
    /// sample of this many when there are more.
    pub sample_k: usize,
    /// k in c₀ = ⌈k·π/π_baseline⌉.
    pub k_circuits: f64,
    /// The most circuits a dance may start with.
    pub max_circuits: f64,
    /// Circuits of a Scout's dance, which says only that there is work.
    pub flat_circuits: f64,
    /// δ: circuits a renewal takes off.
    pub decay_per_renewal: f64,
    /// τ: how long a circuit lasts.
    pub tau: Duration,
    /// The coefficient of variation of recent profitability above which Bees dance.
    pub cv_threshold: f64,
    /// Measurements of profitability a Node keeps.
    pub profit_window: usize,
    /// How long a claim lives before it must be renewed.
    pub claim_ttl: Duration,
    /// How often a Bee running a Patch renews her claim.
    pub claim_refresh: Duration,
    /// A claim running longer than this many times the group's median is a straggler.
    pub straggler_factor: f64,
    /// Time between a home Node's checks on its queries' progress.
    pub poll: Duration,
    /// A Node whose share of lost attempts is above this makes its Followers wait a
    /// gossip round after claiming, and yield to the lower Node and Bee id.
    pub yield_above: f64,
    /// How long gossip takes to spread a claim, which a yielding Follower waits.
    pub gossip_round: Duration,
    /// Seeds the choices Bees make.
    pub seed: u64,
}

impl Default for ForageConfig {
    fn default() -> Self {
        Self {
            mode: Mode::Dances,
            sample_k: 64,
            k_circuits: 5.0,
            max_circuits: 12.0,
            flat_circuits: 1.0,
            decay_per_renewal: 0.5,
            tau: Duration::from_secs(1),
            cv_threshold: 0.25,
            profit_window: 16,
            claim_ttl: Duration::from_secs(6),
            claim_refresh: Duration::from_secs(2),
            straggler_factor: 6.0,
            poll: Duration::from_millis(100),
            yield_above: 0.04,
            gossip_round: Duration::from_millis(250),
            seed: 0,
        }
    }
}
