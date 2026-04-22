use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ConnectionId(u64);

impl ConnectionId {
    #[must_use]
    pub(crate) fn allocate(next_connection_id: &mut u64) -> Self {
        let connection_id = Self(*next_connection_id);
        *next_connection_id = next_connection_id.saturating_add(1);
        connection_id
    }

    #[must_use]
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub(crate) const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for ConnectionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ChannelRuntimeId(u64);

impl ChannelRuntimeId {
    #[must_use]
    pub(crate) fn allocate(next_channel_runtime_id: &mut u64) -> Self {
        let channel_runtime_id = Self(*next_channel_runtime_id);
        *next_channel_runtime_id = next_channel_runtime_id.saturating_add(1);
        channel_runtime_id
    }

    #[must_use]
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub(crate) const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for ChannelRuntimeId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
