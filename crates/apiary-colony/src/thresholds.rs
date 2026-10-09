//! Response thresholds: how readily a Bee takes up each role.
//!
//! A Bee engages role *j* with probability `s² / (s² + θ²)` where *s* is the
//! stimulus it sees and θ its own threshold for that role. Thresholds are drawn
//! per Bee from a log-normal spread, seeded by Node id and Bee index so the
//! simulator replays exactly. The spread σ is the colony's genetic diversity: at
//! σ = 0 every Bee on a Node switches at the same stimulus.
//!
//! Performing a role lowers its threshold and neglecting it raises it, within
//! bounds, so specialists emerge without configuration.

use apiary_core::rng::SeededRng;

use crate::roles::Role;

/// How thresholds are drawn and how they learn.
#[derive(Clone, Copy, Debug)]
pub struct ThresholdParams {
    /// The median threshold (the stimulus at which a typical Bee engages half the time).
    pub median: f64,
    /// The spread σ of the log-normal draw: zero makes every Bee the same.
    pub sigma: f64,
    /// The lowest a threshold may fall.
    pub min: f64,
    /// The highest a threshold may rise.
    pub max: f64,
    /// How fast a role's threshold falls while the Bee performs it (ξ, per second).
    pub learn: f64,
    /// How fast every other role's threshold rises (φ, per second).
    pub forget: f64,
}

impl Default for ThresholdParams {
    fn default() -> Self {
        Self {
            median: 0.5,
            sigma: 0.5,
            min: 0.05,
            max: 8.0,
            learn: 0.05,
            forget: 0.01,
        }
    }
}

/// One Bee's thresholds, a θ per role.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Thresholds {
    theta: [f64; 7],
}

/// A standard normal draw (Box–Muller), from the seeded generator.
pub fn standard_normal(rng: &mut impl SeededRng) -> f64 {
    // `1 - u` keeps the logarithm's argument in (0, 1].
    let u1 = 1.0 - rng.next_f64();
    let u2 = rng.next_f64();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

impl Thresholds {
    /// Draw a Bee's thresholds: `median · exp(σ z)` for an independent normal *z*
    /// per role, within bounds.
    pub fn draw(rng: &mut impl SeededRng, params: &ThresholdParams) -> Self {
        let mut theta = [0.0; 7];
        for slot in &mut theta {
            let z = standard_normal(rng);
            *slot = (params.median * (params.sigma * z).exp()).clamp(params.min, params.max);
        }
        Self { theta }
    }

    /// The same threshold for every role (no diversity).
    pub fn uniform(value: f64) -> Self {
        Self { theta: [value; 7] }
    }

    /// A role's threshold θ.
    pub fn get(&self, role: Role) -> f64 {
        self.theta[role.index()]
    }

    /// The probability of engaging `role` at stimulus `s`: `s² / (s² + θ²)`.
    pub fn engage_probability(&self, role: Role, s: f64) -> f64 {
        let s2 = s.max(0.0).powi(2);
        let t2 = self.get(role).powi(2);
        if s2 + t2 == 0.0 { 0.0 } else { s2 / (s2 + t2) }
    }

    /// Learn from `dt` seconds spent: the role performed (if any) gets easier, every
    /// other role gets harder, all within bounds.
    pub fn tick(&mut self, doing: Option<Role>, dt: f64, params: &ThresholdParams) {
        for role in Role::ALL {
            let slot = &mut self.theta[role.index()];
            *slot = if Some(role) == doing {
                *slot - params.learn * dt
            } else {
                *slot + params.forget * dt
            }
            .clamp(params.min, params.max);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apiary_core::rng::StdSeededRng;

    #[test]
    fn the_engage_probability_is_the_response_threshold_model() {
        let t = Thresholds::uniform(2.0);
        assert_eq!(t.engage_probability(Role::Forager, 0.0), 0.0);
        // At s = θ the Bee engages half the time.
        assert!((t.engage_probability(Role::Forager, 2.0) - 0.5).abs() < 1e-12);
        // s² / (s² + θ²) at s = 4, θ = 2 is 16 / 20.
        assert!((t.engage_probability(Role::Ripener, 4.0) - 0.8).abs() < 1e-12);
    }

    #[test]
    fn sigma_zero_makes_every_bee_alike_and_sigma_spreads_them() {
        let mut params = ThresholdParams {
            sigma: 0.0,
            ..Default::default()
        };
        let a = Thresholds::draw(&mut StdSeededRng::from_seed(1), &params);
        let b = Thresholds::draw(&mut StdSeededRng::from_seed(2), &params);
        assert_eq!(a, b, "no diversity, no difference");
        assert!(Role::ALL.iter().all(|r| a.get(*r) == params.median));

        params.sigma = 0.6;
        let c = Thresholds::draw(&mut StdSeededRng::from_seed(1), &params);
        let d = Thresholds::draw(&mut StdSeededRng::from_seed(2), &params);
        assert_ne!(c, d);
        // The same seed always draws the same Bee.
        assert_eq!(
            c,
            Thresholds::draw(&mut StdSeededRng::from_seed(1), &params)
        );
    }

    #[test]
    fn the_draw_is_log_normal_around_the_median() {
        let params = ThresholdParams {
            sigma: 0.5,
            median: 1.0,
            min: 0.0001,
            max: 10_000.0,
            ..Default::default()
        };
        let mut rng = StdSeededRng::from_seed(42);
        let mut logs: Vec<f64> = (0..4000)
            .map(|_| Thresholds::draw(&mut rng, &params).get(Role::Forager).ln())
            .collect();
        let mean = logs.iter().sum::<f64>() / logs.len() as f64;
        let var = logs.iter().map(|l| (l - mean).powi(2)).sum::<f64>() / logs.len() as f64;
        assert!(mean.abs() < 0.05, "median is 1, so ln has mean 0: {mean}");
        assert!(
            (var.sqrt() - 0.5).abs() < 0.05,
            "ln has sd σ: {}",
            var.sqrt()
        );
        logs.clear();
    }

    #[test]
    fn doing_a_role_lowers_its_threshold_and_neglect_raises_it_within_bounds() {
        let params = ThresholdParams::default();
        let mut t = Thresholds::uniform(1.0);
        t.tick(Some(Role::Ripener), 4.0, &params);
        assert!(t.get(Role::Ripener) < 1.0);
        assert!(t.get(Role::Forager) > 1.0);
        for _ in 0..10_000 {
            t.tick(Some(Role::Ripener), 10.0, &params);
        }
        assert_eq!(t.get(Role::Ripener), params.min, "bounded below");
        assert_eq!(t.get(Role::Forager), params.max, "bounded above");
    }
}
