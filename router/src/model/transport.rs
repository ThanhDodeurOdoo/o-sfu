use super::{SessionId, TransportId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportDirection {
    Send,
    Receive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transport {
    id: TransportId,
    session_id: SessionId,
    direction: TransportDirection,
}

impl Transport {
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
