//! A Frame's commit budget.
//!
//! Every Delta commit adds a log entry that every reader of the Frame must
//! replay, and a streaming Frame can be written to far more often than a table
//! should be committed to. The Node keeps each Frame's commit rate within a
//! budget (commits per minute): deposits and capping spend from it alike, and
//! when it is spent the crop simply holds more and the next commit is bigger.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apiary_core::Clock;

/// How long a commit counts against the budget.
const WINDOW: Duration = Duration::from_secs(60);

/// Commits per minute allowed to each Frame.
pub struct CommitBudget {
    per_minute: usize,
    clock: Arc<dyn Clock>,
    commits: Mutex<HashMap<String, VecDeque<Duration>>>,
}

impl CommitBudget {
    /// A budget of `per_minute` commits to each Frame (at least one).
    pub fn new(per_minute: u32, clock: Arc<dyn Clock>) -> Self {
        Self {
            per_minute: per_minute.max(1) as usize,
            clock,
            commits: Mutex::default(),
        }
    }

    fn trim(log: &mut VecDeque<Duration>, now: Duration) {
        while log
            .front()
            .is_some_and(|at| now.saturating_sub(*at) >= WINDOW)
        {
            log.pop_front();
        }
    }

    /// How many more commits `frame` may make now.
    pub fn remaining(&self, frame: &str) -> usize {
        let now = self.clock.monotonic();
        let mut commits = self.commits.lock().expect("budget lock");
        let log = commits.entry(frame.to_string()).or_default();
        Self::trim(log, now);
        self.per_minute.saturating_sub(log.len())
    }

    /// Spend `n` commits of `frame`'s budget, now.
    pub fn record(&self, frame: &str, n: usize) {
        let now = self.clock.monotonic();
        let mut commits = self.commits.lock().expect("budget lock");
        let log = commits.entry(frame.to_string()).or_default();
        Self::trim(log, now);
        for _ in 0..n {
            log.push_back(now);
        }
    }

    /// How long until `frame` may commit again, if it may not now.
    pub fn wait(&self, frame: &str) -> Duration {
        let now = self.clock.monotonic();
        let mut commits = self.commits.lock().expect("budget lock");
        let log = commits.entry(frame.to_string()).or_default();
        Self::trim(log, now);
        if log.len() < self.per_minute {
            return Duration::ZERO;
        }
        // The oldest commit leaves the window first.
        log.front()
            .map_or(Duration::ZERO, |at| (*at + WINDOW).saturating_sub(now))
    }

    /// The most commits any Frame may make in a minute.
    pub fn per_minute(&self) -> usize {
        self.per_minute
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apiary_core::{ManualClock, SystemClock};

    #[test]
    fn a_frame_may_commit_up_to_its_budget_each_minute() {
        let clock = Arc::new(ManualClock::new(chrono::Utc::now()));
        let budget = CommitBudget::new(3, clock.clone());
        assert_eq!(budget.remaining("a.b.c"), 3);
        budget.record("a.b.c", 2);
        assert_eq!(budget.remaining("a.b.c"), 1);
        assert_eq!(budget.remaining("a.b.other"), 3, "each Frame has its own");
        budget.record("a.b.c", 1);
        assert_eq!(budget.remaining("a.b.c"), 0);
        assert!(budget.wait("a.b.c") > Duration::ZERO);

        clock.advance(Duration::from_secs(61));
        assert_eq!(
            budget.remaining("a.b.c"),
            3,
            "a minute later it is spent no more"
        );
        assert_eq!(budget.wait("a.b.c"), Duration::ZERO);
    }

    #[test]
    fn a_budget_is_at_least_one() {
        let budget = CommitBudget::new(0, SystemClock::shared());
        assert_eq!(budget.per_minute(), 1);
    }
}
