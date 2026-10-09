//! The environment a Node runs in: its clock and its colony seed.
//!
//! [`Env::system`] is production. The observation hive builds an `Env` from a
//! virtual clock and a fixed seed, so the same code runs unchanged under
//! simulation and replays exactly.

use std::sync::Arc;

use crate::clock::{Clock, SystemClock};
use crate::rng::{SeededRng, StdSeededRng, hash_str};

/// Clock and seed, cheap to clone and pass to every component.
#[derive(Clone)]
pub struct Env {
    clock: Arc<dyn Clock>,
    seed: u64,
    inline_cpu: bool,
}

impl Env {
    /// Production environment: the system clock and a random seed.
    pub fn system() -> Self {
        Self {
            clock: SystemClock::shared(),
            seed: StdSeededRng::from_entropy().next_u64(),
            inline_cpu: false,
        }
    }

    /// An environment with the given clock and seed.
    pub fn new(clock: Arc<dyn Clock>, seed: u64) -> Self {
        Self {
            clock,
            seed,
            inline_cpu: false,
        }
    }

    /// Run CPU work on the Node's own runtime instead of the blocking pool.
    ///
    /// A simulation needs this: its virtual clock moves only while every task
    /// waits, and a query parked on a blocking thread, waiting for a simulated
    /// store, would hold the clock still forever. Production leaves it off.
    pub fn with_inline_cpu(mut self) -> Self {
        self.inline_cpu = true;
        self
    }

    /// Whether CPU work runs on the Node's own runtime (see [`with_inline_cpu`](Self::with_inline_cpu)).
    pub fn inline_cpu(&self) -> bool {
        self.inline_cpu
    }

    /// The clock all time reads and sleeps go through.
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    /// The colony seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// An independent random stream for `owner` (a Node id, say) and an index
    /// (a Bee number). The same arguments always give the same stream.
    pub fn rng(&self, owner: &str, index: u64) -> StdSeededRng {
        StdSeededRng::derive(self.seed, &[hash_str(owner), index])
    }
}

impl std::fmt::Debug for Env {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Env")
            .field("seed", &self.seed)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_streams_are_stable_per_owner_and_index() {
        let env = Env::new(SystemClock::shared(), 99);
        let a = env.rng("node-1", 0).next_u64();
        assert_eq!(a, env.rng("node-1", 0).next_u64());
        assert_ne!(a, env.rng("node-1", 1).next_u64());
        assert_ne!(a, env.rng("node-2", 0).next_u64());
    }
}
