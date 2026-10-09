//! A Bee's share of the Node's memory pool.
//!
//! The Node runs one DataFusion memory pool, and each Bee runs its Patch under a
//! reservation capped at its share, with no oversubscription. A [`CappedPool`] is
//! that share: everything an operator reserves through it counts against the
//! Node's pool as well, but no more than the cap can be taken through it. An
//! operator that would exceed the cap is refused, and spills if it can.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::{Result, resources_datafusion_err};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

/// A pool that lends at most `cap` bytes of its parent.
pub struct CappedPool {
    name: String,
    parent: Arc<dyn MemoryPool>,
    cap: usize,
    used: AtomicUsize,
}

impl CappedPool {
    /// A share of `parent`, `cap` bytes at most, called `name` in errors.
    pub fn new(name: impl Into<String>, parent: Arc<dyn MemoryPool>, cap: usize) -> Self {
        Self {
            name: name.into(),
            parent,
            cap,
            used: AtomicUsize::new(0),
        }
    }

    /// The most this share may reserve.
    pub fn cap(&self) -> usize {
        self.cap
    }
}

impl fmt::Debug for CappedPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CappedPool")
            .field("name", &self.name)
            .field("cap", &self.cap)
            .field("used", &self.used.load(Ordering::Relaxed))
            .finish()
    }
}

impl fmt::Display for CappedPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (at most {} bytes)", self.name, self.cap)
    }
}

impl MemoryPool for CappedPool {
    fn name(&self) -> &str {
        &self.name
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.parent.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.parent.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
        self.parent.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.parent.shrink(reservation, shrink);
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        // Take the share first, so two operators cannot both pass the check.
        let before = self.used.fetch_add(additional, Ordering::Relaxed);
        if before + additional > self.cap {
            self.used.fetch_sub(additional, Ordering::Relaxed);
            return Err(resources_datafusion_err!(
                "Failed to allocate {additional} bytes for {}: this Bee's share is {} bytes and {before} are in use",
                reservation.consumer().name(),
                self.cap
            ));
        }
        if let Err(e) = self.parent.try_grow(reservation, additional) {
            self.used.fetch_sub(additional, Ordering::Relaxed);
            return Err(e);
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};

    #[test]
    fn a_bee_cannot_reserve_more_than_its_share_and_the_node_pool_sees_what_it_does() {
        let node: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1000));
        let share: Arc<dyn MemoryPool> = Arc::new(CappedPool::new("bee-0", Arc::clone(&node), 300));
        let r = MemoryConsumer::new("scan").register(&share);

        r.try_grow(200).unwrap();
        assert_eq!(share.reserved(), 200);
        assert_eq!(node.reserved(), 200, "the Node's pool counts it too");

        let refused = r.try_grow(150).unwrap_err().to_string();
        assert!(refused.contains("share is 300"), "{refused}");
        assert_eq!(r.size(), 200, "a refused grow changes nothing");

        r.try_grow(100).unwrap();
        r.shrink(250);
        assert_eq!(share.reserved(), 50);
        assert_eq!(node.reserved(), 50);
        drop(r);
        assert_eq!(node.reserved(), 0);
    }

    #[test]
    fn two_bees_cannot_together_pass_the_nodes_pool() {
        let node: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(500));
        let a: Arc<dyn MemoryPool> = Arc::new(CappedPool::new("a", Arc::clone(&node), 400));
        let b: Arc<dyn MemoryPool> = Arc::new(CappedPool::new("b", Arc::clone(&node), 400));
        let ra = MemoryConsumer::new("a").register(&a);
        let rb = MemoryConsumer::new("b").register(&b);
        ra.try_grow(300).unwrap();
        assert!(
            rb.try_grow(300).is_err(),
            "within b's share, but the Node is out"
        );
        assert_eq!(b.reserved(), 0, "and b's share was given back");
        rb.try_grow(200).unwrap();
    }

    #[test]
    fn a_spill_pool_underneath_still_counts_its_consumers() {
        let node: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1000));
        let share: Arc<dyn MemoryPool> = Arc::new(CappedPool::new("bee", Arc::clone(&node), 600));
        let spillable = MemoryConsumer::new("sort")
            .with_can_spill(true)
            .register(&share);
        spillable.try_grow(500).unwrap();
        assert_eq!(node.reserved(), 500);
        drop(spillable);
        assert_eq!(node.reserved(), 0);
    }
}
