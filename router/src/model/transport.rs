use super::{SessionId, TransportId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportDirection {
    Send,
    Receive,
}

/// Router-owned transport record.
///
/// `id` is the stable transport identity, `session_id` ties the transport to
/// its owner, and `direction` is to validate producer and consumer attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transport {
    id: TransportId,
    session_id: SessionId,
    direction: TransportDirection,
}

impl Transport {
    /// Create a transport owned by `session_id` with a fixed router direction.
    #[must_use]
    pub fn new(id: TransportId, session_id: SessionId, direction: TransportDirection) -> Self {
        Self {
            id,
            session_id,
            direction,
        }
    }

    #[must_use]
    pub fn id(&self) -> TransportId {
        self.id
    }

    #[must_use]
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    #[must_use]
    pub fn direction(&self) -> TransportDirection {
        self.direction
    }
}
