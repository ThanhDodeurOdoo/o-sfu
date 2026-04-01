use super::SessionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
}

impl Session {
    #[must_use]
    pub fn new(id: SessionId) -> Self {
        Self { id }
    }

    #[must_use]
    pub fn id(self) -> SessionId {
        self.id
    }
}
