/// Media bitrate stored as bits per second (not bytes per second).
///
/// This type is the core-domain value for transport caps, pressure snapshots,
/// source metadata and media-policy budgets. Convert to raw bps only at
/// environment, wire, telemetry and backend-library boundaries. Packet byte
/// counters, RTP payload lengths and buffer sizes should stay as byte counts so
/// the unit difference remains visible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bitrate(u64);

impl Bitrate {
    #[must_use]
    pub const fn from_bps(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn from_kbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000))
    }

    #[must_use]
    pub const fn from_mbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000_000))
    }

    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn as_bps(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    #[must_use]
    pub const fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    #[must_use]
    pub const fn divided_by(self, divisor: u64) -> Self {
        match self.0.checked_div(divisor) {
            Some(value) => Self(value),
            None => Self::zero(),
        }
    }
}
