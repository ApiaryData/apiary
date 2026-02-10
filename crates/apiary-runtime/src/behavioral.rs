//! Behavioral model components: Task Abandonment and Colony Temperature.
//!
//! These behaviors govern resource management and failure recovery in Apiary.
//! - [`AbandonmentTracker`]: Tracks task failures and decides when to abandon
//! - [`ColonyThermometer`]: Measures system health as a composite temperature metric

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apiary_core::types::TaskId;

use crate::bee::BeePool;

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

/// Temperature regulation state based on colony temperature reading.
#[derive(Debug, Clone, PartialEq)]
pub enum TemperatureRegulation {
    /// System underutilized (< 0.3) — run background maintenance.
    Cold,
    /// Normal operation (0.3-0.7).
    Ideal,
    /// Reduce non-essential work (0.7-0.85) — defer compaction.
    Warm,
    /// Throttle writes (0.85-0.95), pause maintenance.
    Hot,
    /// Reject new work (> 0.95), focus on completing in-progress tasks.
    Critical,
}

impl TemperatureRegulation {
    /// Convert regulation state to a human-readable string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Ideal => "ideal",
            Self::Warm => "warm",
            Self::Hot => "hot",
            Self::Critical => "critical",
        }
    }
}

/// Colony thermometer measures system health as a composite temperature metric.
///
/// Temperature is a value from 0.0 to 1.0 calculated from:
/// - CPU utilization (busy bees / total bees)
/// - Memory pressure (average memory utilization across bees)
/// - Queue pressure (queued tasks / capacity)
pub struct ColonyThermometer {
    /// Target temperature (default 0.5).
    setpoint: f64,
}

impl ColonyThermometer {
    /// Create a new thermometer with the given setpoint.
    pub fn new(setpoint: f64) -> Self {
        Self { setpoint }
    }

    /// Measure the current colony temperature from the bee pool state.
    ///
    /// Returns a value from 0.0 to 1.0 representing system load.
    pub async fn measure(&self, bee_pool: &BeePool) -> f64 {
        let status = bee_pool.status().await;
        let total_bees = status.len() as f64;

        if total_bees == 0.0 {
            return 0.0;
        }

        // CPU utilization: fraction of busy bees
        let busy_count = status.iter().filter(|s| s.state != "idle").count() as f64;
        let cpu_util = busy_count / total_bees;

        // Memory pressure: average memory utilization across all bees
        let memory_pressure = {
            let sum: f64 = status
                .iter()
                .map(|s| s.memory_used as f64 / s.memory_budget as f64)
                .sum();
            sum / total_bees
        };

        // Queue pressure: queued tasks / (total bees * 2)
        let queue_size = bee_pool.queue_size().await as f64;
        let queue_capacity = total_bees * 2.0;
        let queue_pressure = if queue_capacity > 0.0 {
            (queue_size / queue_capacity).min(1.0)
        } else {
            0.0
        };

        // Weighted composite: CPU and memory are most important
        let temperature = 0.4 * cpu_util + 0.4 * memory_pressure + 0.2 * queue_pressure;
        temperature.min(1.0)
    }

    /// Determine the appropriate regulation state for the given temperature.
    pub fn regulation(&self, temperature: f64) -> TemperatureRegulation {
        match temperature {
            t if t < 0.3 => TemperatureRegulation::Cold,
            t if t <= 0.7 => TemperatureRegulation::Ideal,
            t if t <= 0.85 => TemperatureRegulation::Warm,
            t if t <= 0.95 => TemperatureRegulation::Hot,
            _ => TemperatureRegulation::Critical,
        }
    }

    /// Get the setpoint.
    pub fn setpoint(&self) -> f64 {
        self.setpoint
    }
}

impl Default for ColonyThermometer {
    fn default() -> Self {
        Self::new(0.5)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apiary_core::config::NodeConfig;
    use std::time::Duration;

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

    #[test]
    fn test_temperature_regulation_classification() {
        let thermo = ColonyThermometer::default();

        assert_eq!(thermo.regulation(0.0), TemperatureRegulation::Cold);
        assert_eq!(thermo.regulation(0.2), TemperatureRegulation::Cold);
        assert_eq!(thermo.regulation(0.3), TemperatureRegulation::Ideal);
        assert_eq!(thermo.regulation(0.5), TemperatureRegulation::Ideal);
        assert_eq!(thermo.regulation(0.7), TemperatureRegulation::Ideal);
        assert_eq!(thermo.regulation(0.75), TemperatureRegulation::Warm);
        assert_eq!(thermo.regulation(0.85), TemperatureRegulation::Warm);
        assert_eq!(thermo.regulation(0.90), TemperatureRegulation::Hot);
        assert_eq!(thermo.regulation(0.95), TemperatureRegulation::Hot);
        assert_eq!(thermo.regulation(0.96), TemperatureRegulation::Critical);
        assert_eq!(thermo.regulation(1.0), TemperatureRegulation::Critical);
    }

    #[tokio::test]
    async fn test_colony_temperature_idle() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("local://test");
        config.cores = 4;
        config.memory_per_bee = 1024 * 1024;
        config.cache_dir = tmp.path().to_path_buf();

        let pool = BeePool::new(&config);
        let thermo = ColonyThermometer::default();

        let temp = thermo.measure(&pool).await;
        // All idle, no queue, no memory use → near zero
        assert!(temp < 0.1, "Expected low temperature, got {temp}");
        assert_eq!(thermo.regulation(temp), TemperatureRegulation::Cold);
    }

    #[tokio::test]
    async fn test_colony_temperature_all_busy() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("local://test");
        config.cores = 2;
        config.memory_per_bee = 1024 * 1024;
        config.cache_dir = tmp.path().to_path_buf();

        let pool = Arc::new(BeePool::new(&config));
        let thermo = ColonyThermometer::default();

        // Submit 2 long-running tasks to keep both bees busy
        let _h1 = pool
            .submit(|| {
                std::thread::sleep(Duration::from_millis(200));
                Ok(vec![])
            })
            .await;
        let _h2 = pool
            .submit(|| {
                std::thread::sleep(Duration::from_millis(200));
                Ok(vec![])
            })
            .await;

        // Give tasks time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        let temp = thermo.measure(&pool).await;
        // Both bees busy → CPU util = 1.0, so temp should be at least 0.4
        assert!(temp >= 0.3, "Expected elevated temperature, got {temp}");
    }

    #[test]
    fn test_temperature_regulation_as_str() {
        assert_eq!(TemperatureRegulation::Cold.as_str(), "cold");
        assert_eq!(TemperatureRegulation::Ideal.as_str(), "ideal");
        assert_eq!(TemperatureRegulation::Warm.as_str(), "warm");
        assert_eq!(TemperatureRegulation::Hot.as_str(), "hot");
        assert_eq!(TemperatureRegulation::Critical.as_str(), "critical");
    }
}
