use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the number of seconds since the Unix epoch.
#[must_use]
pub(crate) fn secs_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}
