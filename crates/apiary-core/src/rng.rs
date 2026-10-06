//! Seeded randomness: every random choice goes through [`SeededRng`].
//!
//! Thresholds, dance sampling and id generation draw from a generator seeded by
//! (colony seed, Node id, Bee index), so the simulator replays a run exactly.
//! [`StdSeededRng`] is xoshiro256** seeded through splitmix64; it is small,
//! fast and has no dependencies. It is not cryptographic and must never be
//! used for keys or tokens.

/// A deterministic source of random numbers.
pub trait SeededRng: Send {
    /// The next 64 random bits.
    fn next_u64(&mut self) -> u64;

    /// A uniform value in `[0, 1)`.
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A uniform value in `[0, bound)`. Returns 0 when `bound` is 0.
    fn next_below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        // Rejection sampling removes modulo bias.
        let zone = u64::MAX - (u64::MAX % bound);
        loop {
            let v = self.next_u64();
            if v < zone {
                return v % bound;
            }
        }
    }

    /// 16 random bytes.
    fn next_bytes16(&mut self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.next_u64().to_le_bytes());
        out[8..].copy_from_slice(&self.next_u64().to_le_bytes());
        out
    }
}

/// xoshiro256** seeded with splitmix64.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StdSeededRng {
    s: [u64; 4],
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// FNV-1a over a string, for turning ids into seed material.
pub fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

impl StdSeededRng {
    /// Create a generator from a single seed.
    pub fn from_seed(seed: u64) -> Self {
        let mut sm = seed;
        Self {
            s: [
                splitmix64(&mut sm),
                splitmix64(&mut sm),
                splitmix64(&mut sm),
                splitmix64(&mut sm),
            ],
        }
    }

    /// Create an independent stream from a colony seed and a path of parts,
    /// for example `[hash_str(node_id), bee_index]`. Different paths give
    /// uncorrelated streams; the same path always gives the same stream.
    pub fn derive(seed: u64, parts: &[u64]) -> Self {
        let mut sm = seed;
        let mut mixed = splitmix64(&mut sm);
        for part in parts {
            sm = mixed ^ part.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            mixed = splitmix64(&mut sm);
        }
        Self::from_seed(mixed)
    }

    /// A generator seeded from operating-system entropy. Not replayable.
    pub fn from_entropy() -> Self {
        let (hi, lo) = uuid::Uuid::new_v4().as_u64_pair();
        Self::from_seed(hi ^ lo.rotate_left(32))
    }
}

impl SeededRng for StdSeededRng {
    fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_replays_exactly() {
        let mut a = StdSeededRng::from_seed(42);
        let mut b = StdSeededRng::from_seed(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_paths_give_different_streams() {
        let mut a = StdSeededRng::derive(7, &[hash_str("node-a"), 0]);
        let mut b = StdSeededRng::derive(7, &[hash_str("node-a"), 1]);
        let mut c = StdSeededRng::derive(7, &[hash_str("node-b"), 0]);
        let (x, y, z) = (a.next_u64(), b.next_u64(), c.next_u64());
        assert!(x != y && x != z && y != z);
        assert_eq!(
            StdSeededRng::derive(7, &[hash_str("node-a"), 0]).next_u64(),
            x
        );
    }

    #[test]
    fn next_f64_is_in_unit_interval_and_roughly_uniform() {
        let mut rng = StdSeededRng::from_seed(1);
        let n = 20_000;
        let mut sum = 0.0;
        for _ in 0..n {
            let v = rng.next_f64();
            assert!((0.0..1.0).contains(&v));
            sum += v;
        }
        let mean = sum / n as f64;
        assert!((mean - 0.5).abs() < 0.02, "mean was {mean}");
    }

    #[test]
    fn next_below_respects_bound() {
        let mut rng = StdSeededRng::from_seed(3);
        for _ in 0..1000 {
            assert!(rng.next_below(7) < 7);
        }
        assert_eq!(rng.next_below(0), 0);
    }
}
