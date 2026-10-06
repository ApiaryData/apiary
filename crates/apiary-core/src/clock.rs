//! Clock abstraction: every read of time and every sleep goes through [`Clock`].
//!
//! Production code uses [`SystemClock`]. The observation hive (a deterministic
//! simulator) supplies its own clock, so a run replays exactly from its seed.
//! [`ManualClock`] is the simplest such clock and is used in unit tests.
//!
//! A clock offers two views of time. The monotonic view counts from an
//! arbitrary origin and never goes backwards; the dance floor's remaining
//! lifetimes use it, because a Pi can boot without network time. The wall view
//! is calendar time, for Delta commit timestamps and anything shown to people.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::watch;

/// Milliseconds, as used for dance-floor lifetimes.
pub type Millis = u64;

/// A source of time and sleeping.
#[async_trait]
pub trait Clock: Send + Sync + 'static {
    /// Wall-clock time. May jump (NTP, boot without a real-time clock).
    fn now_utc(&self) -> DateTime<Utc>;

    /// Time since an arbitrary origin; never goes backwards.
    fn monotonic(&self) -> Duration;

    /// Complete after `duration` has passed on this clock.
    async fn sleep(&self, duration: Duration);
}

/// The real clock. Monotonic time is read from Tokio's clock, so
/// `tokio::time::pause()` also pauses it in tests.
#[derive(Debug)]
pub struct SystemClock {
    origin: tokio::time::Instant,
}

impl SystemClock {
    /// Create a system clock whose monotonic origin is now.
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }

    /// A shared system clock, ready to hand to components.
    pub fn shared() -> Arc<dyn Clock> {
        Arc::new(Self::new())
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Clock for SystemClock {
    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn monotonic(&self) -> Duration {
        self.origin.elapsed()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// A clock that moves only when told to. Sleepers wake when [`advance`](Self::advance)
/// carries time past their deadline.
#[derive(Debug)]
pub struct ManualClock {
    wall_origin: DateTime<Utc>,
    elapsed: Mutex<Duration>,
    tick: watch::Sender<Duration>,
}

impl ManualClock {
    /// Create a manual clock whose wall time starts at `wall_origin` and whose
    /// monotonic time starts at zero.
    pub fn new(wall_origin: DateTime<Utc>) -> Self {
        let (tick, _) = watch::channel(Duration::ZERO);
        Self {
            wall_origin,
            elapsed: Mutex::new(Duration::ZERO),
            tick,
        }
    }

    /// Move time forward and wake any sleeper whose deadline has passed.
    pub fn advance(&self, by: Duration) {
        let now = {
            let mut elapsed = self.elapsed.lock().expect("manual clock poisoned");
            *elapsed += by;
            *elapsed
        };
        self.tick.send_replace(now);
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now_utc(&self) -> DateTime<Utc> {
        let elapsed = *self.elapsed.lock().expect("manual clock poisoned");
        self.wall_origin
            + chrono::Duration::from_std(elapsed).expect("manual clock elapsed out of range")
    }

    fn monotonic(&self) -> Duration {
        *self.elapsed.lock().expect("manual clock poisoned")
    }

    async fn sleep(&self, duration: Duration) {
        let deadline = self.monotonic() + duration;
        let mut rx = self.tick.subscribe();
        while *rx.borrow_and_update() < deadline {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn system_clock_follows_paused_tokio_time() {
        let clock = SystemClock::new();
        let before = clock.monotonic();
        clock.sleep(Duration::from_secs(5)).await;
        assert!(clock.monotonic() - before >= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn manual_clock_sleeper_wakes_only_after_deadline() {
        let clock = Arc::new(ManualClock::new(Utc::now()));
        let sleeper = {
            let clock = Arc::clone(&clock);
            tokio::spawn(async move { clock.sleep(Duration::from_secs(10)).await })
        };

        tokio::task::yield_now().await;
        clock.advance(Duration::from_secs(4));
        tokio::task::yield_now().await;
        assert!(!sleeper.is_finished());

        clock.advance(Duration::from_secs(6));
        sleeper.await.unwrap();
        assert_eq!(clock.monotonic(), Duration::from_secs(10));
    }

    #[test]
    fn manual_clock_wall_time_tracks_monotonic() {
        let origin = Utc::now();
        let clock = ManualClock::new(origin);
        clock.advance(Duration::from_secs(90));
        assert_eq!(clock.now_utc(), origin + chrono::Duration::seconds(90));
    }
}
