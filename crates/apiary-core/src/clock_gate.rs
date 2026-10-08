//! The commit gate: a Node refuses to commit until its clock can be trusted.
//!
//! Delta commit timestamps are wall time, and a Pi that booted without network
//! time may think it is 1970. Commits made then would put nonsense in the log
//! (and break time travel and vacuum for everyone), so a Node refuses them until
//! its clock is plausible. Ingest does not need wall time (the crop is ordered by
//! segment number), so a site whose clock is wrong keeps taking deposits into its
//! crops and ships them once the clock is right.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::Result;
use crate::error::ApiaryError;

/// The earliest time a working clock can read: 2025-01-01. Any Apiary Node runs
/// on a clock that has been past this since before the software existed.
pub const EARLIEST_PLAUSIBLE_SECS: i64 = 1_735_689_600;

/// Decides whether this Node may commit to a Delta log right now.
pub type CommitGate = Arc<dyn Fn() -> Result<()> + Send + Sync>;

/// Check a clock reading: it must be past [`EARLIEST_PLAUSIBLE_SECS`] and not
/// behind `not_before` (a time the Node knows has passed, such as when its own
/// join token was issued).
pub fn check_clock(now: DateTime<Utc>, not_before: Option<DateTime<Utc>>) -> Result<()> {
    if now.timestamp() < EARLIEST_PLAUSIBLE_SECS {
        return Err(ApiaryError::Clock {
            message: format!(
                "this node's clock reads {}, which cannot be right; commits wait until it is synchronised (crops keep taking deposits)",
                now.format("%Y-%m-%d %H:%M:%S UTC")
            ),
        });
    }
    if let Some(floor) = not_before
        && now < floor
    {
        return Err(ApiaryError::Clock {
            message: format!(
                "this node's clock reads {}, which is before {}, a time that has already passed; commits wait until it is synchronised",
                now.format("%Y-%m-%d %H:%M:%S UTC"),
                floor.format("%Y-%m-%d %H:%M:%S UTC")
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn a_clock_at_the_epoch_is_refused() {
        let epoch = Utc.timestamp_opt(5, 0).unwrap();
        let err = check_clock(epoch, None).unwrap_err();
        assert!(matches!(err, ApiaryError::Clock { .. }));
        assert!(err.to_string().contains("1970"), "{err}");
    }

    #[test]
    fn a_clock_behind_a_time_that_has_passed_is_refused() {
        let now = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
        let issued = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        assert!(check_clock(now, Some(issued)).is_err());
        assert!(check_clock(issued, Some(issued)).is_ok());
    }

    #[test]
    fn a_sane_clock_passes() {
        assert!(check_clock(Utc::now(), None).is_ok());
    }
}
