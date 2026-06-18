use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RouterId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransportId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProducerId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConsumerId(pub u64);

/// unique identifier for a user's transport connection within the server process
///
/// this separates the ephemeral transport lifecycle from the persistent logical
/// user identity. a single user might create multiple connections over time due to
/// network drops or handovers. this identifier ensures media operations only apply
/// to the specific transport they were negotiated against
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
    #[must_use]
    pub fn allocate(next_connection_id: &mut u64) -> Self {
        let connection_id = Self(*next_connection_id);
        *next_connection_id = next_connection_id.saturating_add(1);
        connection_id
    }

    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for ConnectionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// runtime-local identifier for one rtc media worker
///
/// this is worker identity, not a worker count or vector capacity
/// convert to raw `usize` only when indexing worker storage or projecting
/// telemetry and diagnostics fields
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MediaWorkerId(usize);

impl MediaWorkerId {
    #[must_use]
    pub const fn from_raw(raw: usize) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.0
    }
}
