use crate::RouterId;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Router {
    id: RouterId,
}

impl Router {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn id(self) -> RouterId {
        self.id
    }
}
