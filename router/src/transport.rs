use crate::{SessionId, TransportId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transport {
    id: TransportId,
    session_id: SessionId,
}

impl Transport {
    #[must_use]
    pub fn new(id: TransportId, session_id: SessionId) -> Self {
        Self { id, session_id }
    }
}
