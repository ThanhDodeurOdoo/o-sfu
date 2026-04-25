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
pub(crate) struct RoomInstanceId(u64);

impl RoomInstanceId {
    #[must_use]
    pub(crate) fn allocate(next_room_instance_id: &mut u64) -> Self {
        let room_instance_id = Self(*next_room_instance_id);
        *next_room_instance_id = next_room_instance_id.saturating_add(1);
        room_instance_id
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

impl Display for RoomInstanceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
