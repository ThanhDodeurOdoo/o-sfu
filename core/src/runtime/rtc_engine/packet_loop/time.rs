//! Deterministic packet-loop time.
//!
//! The async host translates real monotonic time into this packet-loop-local
//! clock before synchronous routing and scheduling helpers mutate state. Tests
//! can construct exact timestamps without reading the OS clock.

use std::{ops::Add, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PacketLoopTime {
    micros_since_origin: u64,
}

impl PacketLoopTime {
    pub const ZERO: Self = Self {
        micros_since_origin: 0,
    };

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub const fn from_millis(millis_since_origin: u64) -> Self {
        Self {
            micros_since_origin: millis_since_origin.saturating_mul(1_000),
        }
    }

    pub(super) fn from_duration_saturating(duration: Duration) -> Self {
        let micros_since_origin = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        Self {
            micros_since_origin,
        }
    }

    #[must_use]
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        Duration::from_micros(
            self.micros_since_origin
                .saturating_sub(earlier.micros_since_origin),
        )
    }
}

impl Add<Duration> for PacketLoopTime {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        let delta = Self::from_duration_saturating(duration);
        Self {
            micros_since_origin: self
                .micros_since_origin
                .saturating_add(delta.micros_since_origin),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::PacketLoopTime;

    #[test]
    fn packet_loop_time_adds_duration_without_os_clock() {
        let start = PacketLoopTime::from_millis(10);
        let later = start + Duration::from_millis(25);

        assert_eq!(
            later.saturating_duration_since(start),
            Duration::from_millis(25)
        );
    }

    #[test]
    fn packet_loop_time_saturates_reverse_duration() {
        let later = PacketLoopTime::from_millis(30);
        let earlier = PacketLoopTime::from_millis(20);

        assert_eq!(earlier.saturating_duration_since(later), Duration::ZERO);
    }
}
