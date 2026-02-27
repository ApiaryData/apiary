//! Structured timing instrumentation for query execution.
//!
//! Enabled via the `APIARY_TIMING=1` environment variable. When unset,
//! all timing operations are no-ops with zero overhead.
//!
//! After each query completes, a single parseable line is emitted to stderr:
//! ```text
//! [TIMING] query=Q1.1 total=5044ms parse=2ms plan=15ms ...
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Global flag cached at first access to avoid repeated env lookups.
static TIMING_ENABLED: AtomicBool = AtomicBool::new(false);
static TIMING_CHECKED: AtomicBool = AtomicBool::new(false);

/// Returns `true` when `APIARY_TIMING=1` is set.
#[inline]
pub fn timing_enabled() -> bool {
    if !TIMING_CHECKED.load(Ordering::Relaxed) {
        let enabled = std::env::var("APIARY_TIMING")
            .map(|v| v == "1")
            .unwrap_or(false);
        TIMING_ENABLED.store(enabled, Ordering::Relaxed);
        TIMING_CHECKED.store(true, Ordering::Relaxed);
    }
    TIMING_ENABLED.load(Ordering::Relaxed)
}

/// Collected timing phases for a single query.
pub struct QueryTimings {
    pub query_id: String,
    pub phases: Vec<(String, Duration)>,
    pub total: Duration,
    start: Instant,
}

impl QueryTimings {
    /// Create a new `QueryTimings` and start the total wall-clock timer.
    /// Returns `None` when timing is disabled (zero overhead path).
    #[inline]
    pub fn begin(query_id: impl Into<String>) -> Option<Self> {
        if !timing_enabled() {
            return None;
        }
        Some(Self {
            query_id: query_id.into(),
            phases: Vec::new(),
            total: Duration::ZERO,
            start: Instant::now(),
        })
    }

    /// Start timing a named phase. Returns the `Instant` to pass to [`end_phase`].
    #[inline]
    pub fn start_phase(&self) -> Instant {
        Instant::now()
    }

    /// Record the elapsed time for a phase started with [`start_phase`].
    #[inline]
    pub fn end_phase(&mut self, name: &str, since: Instant) {
        self.phases.push((name.to_string(), since.elapsed()));
    }

    /// Finish timing and emit the results to stderr.
    pub fn finish(mut self) {
        self.total = self.start.elapsed();

        let mut parts: Vec<String> = Vec::with_capacity(self.phases.len() + 2);
        parts.push(format!("query={}", self.query_id));
        parts.push(format!("total={}ms", self.total.as_millis()));
        for (name, dur) in &self.phases {
            parts.push(format!("{}={}ms", name, dur.as_millis()));
        }

        eprintln!("[TIMING] {}", parts.join(" "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timing_disabled_returns_none() {
        // Reset the cached check so we re-read the env var.
        TIMING_CHECKED.store(false, Ordering::Relaxed);
        std::env::remove_var("APIARY_TIMING");
        TIMING_CHECKED.store(false, Ordering::Relaxed);

        let t = QueryTimings::begin("test");
        assert!(t.is_none());
    }

    #[test]
    fn test_timing_enabled_returns_some() {
        // Reset the cached check so we re-read the env var.
        TIMING_CHECKED.store(false, Ordering::Relaxed);
        std::env::set_var("APIARY_TIMING", "1");
        TIMING_CHECKED.store(false, Ordering::Relaxed);

        let t = QueryTimings::begin("test_query");
        assert!(t.is_some());

        let mut t = t.unwrap();
        assert_eq!(t.query_id, "test_query");

        let phase_start = t.start_phase();
        std::thread::sleep(std::time::Duration::from_millis(1));
        t.end_phase("test_phase", phase_start);

        assert_eq!(t.phases.len(), 1);
        assert_eq!(t.phases[0].0, "test_phase");
        assert!(t.phases[0].1.as_nanos() > 0);

        t.finish();

        // Clean up
        std::env::remove_var("APIARY_TIMING");
        TIMING_CHECKED.store(false, Ordering::Relaxed);
    }

    #[test]
    fn test_query_timings_struct_fields() {
        TIMING_CHECKED.store(false, Ordering::Relaxed);
        std::env::set_var("APIARY_TIMING", "1");
        TIMING_CHECKED.store(false, Ordering::Relaxed);

        let mut t = QueryTimings::begin("Q1.1").unwrap();
        let s1 = t.start_phase();
        t.end_phase("parse", s1);
        let s2 = t.start_phase();
        t.end_phase("plan", s2);

        assert_eq!(t.phases.len(), 2);
        assert_eq!(t.phases[0].0, "parse");
        assert_eq!(t.phases[1].0, "plan");

        t.finish();

        std::env::remove_var("APIARY_TIMING");
        TIMING_CHECKED.store(false, Ordering::Relaxed);
    }
}
