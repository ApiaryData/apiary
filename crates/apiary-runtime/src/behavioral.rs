//! Behavioral model components: Task Abandonment.
//!
//! [`AbandonmentTracker`] tracks task failures and decides when to abandon. The
//! Node's temperature and the Bees' roles live in `apiary-colony`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apiary_core::types::TaskId;

/// Decision made by the abandonment tracker after a task failure.
#[derive(Debug, Clone, PartialEq)]
pub enum AbandonmentDecision {
    /// Retry the task (possibly on a different node).
    Retry,
    /// Abandon the task — query fails with diagnostic error.
    Abandon,
}

/// Tracker for task failures that decides when to abandon failing tasks.
///
/// Tasks that repeatedly fail are abandoned rather than retried indefinitely.
/// This prevents the colony from wasting effort on unrecoverable work.
#[derive(Debug)]
pub struct AbandonmentTracker {
    /// Map of task ID to failure count.
    trial_counts: Arc<Mutex<HashMap<TaskId, u32>>>,
    /// Maximum number of attempts before abandoning (default: 3).
    trial_limit: u32,
}

impl AbandonmentTracker {
    /// Create a new abandonment tracker with the given trial limit.
    pub fn new(trial_limit: u32) -> Self {
        Self {
            trial_counts: Arc::new(Mutex::new(HashMap::new())),
            trial_limit,
        }
    }

    /// Record a task failure and return the abandonment decision.
    ///
    /// # Arguments
    ///
    /// * `task_id` — The ID of the failed task
    ///
    /// # Returns
    ///
    /// * `AbandonmentDecision::Retry` — Try again (possibly on a different node)
    /// * `AbandonmentDecision::Abandon` — Give up with diagnostic error
    pub fn record_failure(&self, task_id: &TaskId) -> AbandonmentDecision {
        let mut counts = self.trial_counts.lock().unwrap();
        let count = counts.entry(task_id.clone()).or_insert(0);
        *count += 1;
        if *count >= self.trial_limit {
            AbandonmentDecision::Abandon
        } else {
            AbandonmentDecision::Retry
        }
    }

    /// Record a task success and clear its failure count.
    pub fn record_success(&self, task_id: &TaskId) {
        let mut counts = self.trial_counts.lock().unwrap();
        counts.remove(task_id);
    }

    /// Get the current failure count for a task (0 if not tracked).
    pub fn get_count(&self, task_id: &TaskId) -> u32 {
        let counts = self.trial_counts.lock().unwrap();
        counts.get(task_id).copied().unwrap_or(0)
    }

    /// Clear all tracked failures.
    pub fn clear(&self) {
        let mut counts = self.trial_counts.lock().unwrap();
        counts.clear();
    }
}

impl Default for AbandonmentTracker {
    fn default() -> Self {
        Self::new(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abandonment_tracker_retry_then_abandon() {
        let tracker = AbandonmentTracker::new(3);
        let task_id = TaskId::generate();

        // First failure: retry
        assert_eq!(tracker.record_failure(&task_id), AbandonmentDecision::Retry);
        assert_eq!(tracker.get_count(&task_id), 1);

        // Second failure: retry
        assert_eq!(tracker.record_failure(&task_id), AbandonmentDecision::Retry);
        assert_eq!(tracker.get_count(&task_id), 2);

        // Third failure: abandon
        assert_eq!(
            tracker.record_failure(&task_id),
            AbandonmentDecision::Abandon
        );
        assert_eq!(tracker.get_count(&task_id), 3);
    }

    #[test]
    fn test_abandonment_tracker_success_clears_count() {
        let tracker = AbandonmentTracker::new(3);
        let task_id = TaskId::generate();

        tracker.record_failure(&task_id);
        tracker.record_failure(&task_id);
        assert_eq!(tracker.get_count(&task_id), 2);

        tracker.record_success(&task_id);
        assert_eq!(tracker.get_count(&task_id), 0);
    }

    #[test]
    fn test_abandonment_tracker_independent_tasks() {
        let tracker = AbandonmentTracker::new(2);
        let task1 = TaskId::generate();
        let task2 = TaskId::generate();

        tracker.record_failure(&task1);
        assert_eq!(tracker.get_count(&task1), 1);
        assert_eq!(tracker.get_count(&task2), 0);

        tracker.record_failure(&task2);
        assert_eq!(tracker.get_count(&task1), 1);
        assert_eq!(tracker.get_count(&task2), 1);
    }

    #[test]
    fn test_abandonment_tracker_clear() {
        let tracker = AbandonmentTracker::new(3);
        let task1 = TaskId::generate();
        let task2 = TaskId::generate();

        tracker.record_failure(&task1);
        tracker.record_failure(&task2);
        tracker.clear();

        assert_eq!(tracker.get_count(&task1), 0);
        assert_eq!(tracker.get_count(&task2), 0);
    }
}
