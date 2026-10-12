//! Profitability and the dance that reports it.
//!
//! A Forager that finishes a Patch measures π, useful rows per Bee-second with
//! fetch time counted as cost. She divides it by her own baseline (the pace she
//! normally keeps) so a dance reports how good the work is *for the dancer*; a
//! cloud core's raw numbers would otherwise drown every Pi's.
//!
//! A dance starts at c₀ = ⌈k·π/π_baseline⌉ circuits, capped. Each Patch she
//! finishes from the group renews it one step lower, the linear decline of
//! Seeley's expiration of dissent. The entry's lifetime is set so it lapses when
//! the circuits run out, so a dead dancer's dance fades with no one deleting it:
//!
//! ```text
//! c_eff(t) = max(0, c₀ − δ·n − (t − t_last)/τ)
//! ```
//!
//! because the entry's remaining life, in units of τ, *is* c_eff.

use std::collections::VecDeque;
use std::time::Duration;

use crate::config::ForageConfig;

/// A Node's record of how profitable its Patches have been.
#[derive(Debug)]
pub struct Profit {
    /// The pace this Node normally keeps, in rows per Bee-second (an exponential
    /// average over everything it has run).
    baseline: Option<f64>,
    /// Recent profitability, each divided by the baseline in force when it was
    /// measured.
    window: VecDeque<f64>,
    capacity: usize,
}

impl Profit {
    /// An empty record that keeps the last `capacity` normalised measurements.
    pub fn new(capacity: usize) -> Self {
        Self {
            baseline: None,
            window: VecDeque::with_capacity(capacity),
            capacity: capacity.max(2),
        }
    }

    /// Record a Patch that produced `rows` in `elapsed`, fetch time included.
    /// Returns its profitability against the baseline: above 1 is better than this
    /// Node's usual.
    pub fn record(&mut self, rows: u64, elapsed: Duration) -> f64 {
        let seconds = elapsed.as_secs_f64().max(1e-6);
        let pi = rows as f64 / seconds;
        let normalised = match self.baseline {
            Some(b) if b > 0.0 => pi / b,
            _ => 1.0,
        };
        // The baseline moves slowly, so a run of good work stands out against it.
        self.baseline = Some(match self.baseline {
            Some(b) => 0.9 * b + 0.1 * pi,
            None => pi,
        });
        if self.window.len() == self.capacity {
            self.window.pop_front();
        }
        self.window.push_back(normalised);
        normalised
    }

    /// The coefficient of variation of recent profitability: how patchy the work
    /// has been. Zero until there are enough measurements to say.
    pub fn variation(&self) -> f64 {
        if self.window.len() < 4 {
            return 0.0;
        }
        let n = self.window.len() as f64;
        let mean = self.window.iter().sum::<f64>() / n;
        if mean <= 0.0 {
            return 0.0;
        }
        let variance = self.window.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        variance.sqrt() / mean
    }

    /// Whether the work has been patchy enough that dancing pays.
    pub fn worth_dancing(&self, cfg: &ForageConfig) -> bool {
        self.variation() > cfg.cv_threshold
    }
}

/// c₀: the circuits a dance starts with, for profitability `normalised`.
pub fn initial_circuits(normalised: f64, cfg: &ForageConfig) -> f64 {
    (cfg.k_circuits * normalised)
        .ceil()
        .clamp(1.0, cfg.max_circuits)
}

/// The circuits left after `renewals` finished Patches, before any decay with age.
pub fn circuits_after(c0: f64, renewals: u32, cfg: &ForageConfig) -> f64 {
    (c0 - cfg.decay_per_renewal * f64::from(renewals)).max(0.0)
}

/// The lifetime that makes an entry lapse when `circuits` have run.
pub fn lifetime_of(circuits: f64, cfg: &ForageConfig) -> Duration {
    cfg.tau.mul_f64(circuits.max(0.0))
}

/// The circuits a dance still has, from the lifetime left on its entry.
pub fn circuits_left(remaining_ms: u64, cfg: &ForageConfig) -> f64 {
    remaining_ms as f64 / cfg.tau.as_millis().max(1) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ForageConfig {
        ForageConfig::default()
    }

    #[test]
    fn profitability_is_measured_against_the_nodes_own_pace() {
        let mut fast = Profit::new(8);
        let mut slow = Profit::new(8);
        // A cloud core and a Pi each doing their usual work: both normal.
        for _ in 0..4 {
            fast.record(100_000, Duration::from_secs(1));
            slow.record(1_000, Duration::from_secs(1));
        }
        let fast_normal = fast.record(100_000, Duration::from_secs(1));
        let slow_normal = slow.record(1_000, Duration::from_secs(1));
        assert!((fast_normal - 1.0).abs() < 0.01 && (slow_normal - 1.0).abs() < 0.01);
        // Work twice as good as usual reads twice as good, on either.
        assert!(fast.record(200_000, Duration::from_secs(1)) > 1.8);
        assert!(slow.record(2_000, Duration::from_secs(1)) > 1.8);
    }

    #[test]
    fn uniform_work_is_not_worth_dancing_about() {
        let mut p = Profit::new(8);
        for _ in 0..8 {
            p.record(5_000, Duration::from_secs(1));
        }
        assert!(p.variation() < 0.01);
        assert!(!p.worth_dancing(&cfg()));

        let mut patchy = Profit::new(8);
        for rows in [1_000, 9_000, 2_000, 12_000, 1_500, 10_000, 900, 11_000] {
            patchy.record(rows, Duration::from_secs(1));
        }
        assert!(patchy.worth_dancing(&cfg()));
    }

    #[test]
    fn circuits_follow_profitability_are_capped_and_fall_with_each_renewal() {
        let cfg = cfg();
        assert_eq!(initial_circuits(0.1, &cfg), 1.0, "never less than one");
        assert!(initial_circuits(2.0, &cfg) > initial_circuits(1.0, &cfg));
        assert_eq!(initial_circuits(100.0, &cfg), cfg.max_circuits);
        let c0 = 6.0;
        let mut last = c0;
        for n in 1..=12 {
            let c = circuits_after(c0, n, &cfg);
            assert!(c <= last);
            last = c;
        }
        assert_eq!(circuits_after(c0, 100, &cfg), 0.0);
    }

    #[test]
    fn remaining_life_is_the_circuits_left() {
        let cfg = cfg();
        let life = lifetime_of(4.0, &cfg);
        assert_eq!(circuits_left(life.as_millis() as u64, &cfg), 4.0);
        // Halfway through its life a dance has half its circuits.
        assert_eq!(circuits_left(life.as_millis() as u64 / 2, &cfg), 2.0);
        assert_eq!(circuits_left(0, &cfg), 0.0);
    }
}
