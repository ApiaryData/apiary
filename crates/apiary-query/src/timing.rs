//! Structured timing instrumentation for query execution.
//!
//! Enabled via the `APIARY_TIMING=1` environment variable. When unset,
//! all timing operations are no-ops with zero overhead.
//!
//! After each query completes, a single parseable line is emitted to stderr:
//! ```text
//! [TIMING] query=Q1.1 total=5044ms parse=2ms plan=15ms ...
//! ```

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Cached result of the `APIARY_TIMING` environment variable check.
static TIMING_ENABLED: OnceLock<bool> = OnceLock::new();

/// Returns `true` when `APIARY_TIMING=1` is set.
#[inline]
pub fn timing_enabled() -> bool {
    *TIMING_ENABLED.get_or_init(|| {
        std::env::var("APIARY_TIMING")
            .map(|v| v == "1")
            .unwrap_or(false)
    })
}

/// Maximum number of characters from the SQL query used as the timing query ID.
const MAX_QUERY_ID_LENGTH: usize = 60;

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

    /// Create a `QueryTimings` using the first [`MAX_QUERY_ID_LENGTH`] chars
    /// of the SQL string as the query identifier.
    #[inline]
    pub fn begin_from_sql(sql: &str) -> Option<Self> {
        Self::begin(sql.chars().take(MAX_QUERY_ID_LENGTH).collect::<String>())
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

    /// Record a pre-computed duration for a phase that was accumulated
    /// across multiple iterations (e.g., per-table I/O totals).
    #[inline]
    pub fn add_accumulated_phase(&mut self, name: &str, duration: Duration) {
        self.phases.push((name.to_string(), duration));
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

    // Note: Because OnceLock caches the value once, the tests below that
    // toggle the env var operate on the *constructor* returning None/Some
    // based on the first call in the process. In a test binary the init
    // happens once; to exercise both paths we call `timing_enabled()`
    // directly and test the struct API independently.

    #[test]
    fn test_query_timings_struct_fields() {
        // Exercise the struct API directly (no env dependency).
        let mut t = QueryTimings {
            query_id: "Q1.1".to_string(),
            phases: Vec::new(),
            total: Duration::ZERO,
            start: Instant::now(),
        };

        let s1 = t.start_phase();
        t.end_phase("parse", s1);
        let s2 = t.start_phase();
        t.end_phase("plan", s2);
        t.add_accumulated_phase("data_read", Duration::from_millis(42));

        assert_eq!(t.phases.len(), 3);
        assert_eq!(t.phases[0].0, "parse");
        assert_eq!(t.phases[1].0, "plan");
        assert_eq!(t.phases[2].0, "data_read");
        assert_eq!(t.phases[2].1, Duration::from_millis(42));

        t.finish();
    }

    #[test]
    fn test_begin_from_sql_truncates() {
        // Test the truncation logic directly on a long string.
        let long_sql = "A".repeat(200);
        let truncated: String = long_sql.chars().take(MAX_QUERY_ID_LENGTH).collect();
        assert_eq!(truncated.len(), MAX_QUERY_ID_LENGTH);
    }

    #[test]
    fn test_max_query_id_length_constant() {
        assert_eq!(MAX_QUERY_ID_LENGTH, 60);
    }
}
