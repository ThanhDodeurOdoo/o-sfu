#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime::room) struct UserLayout {
    featured: Option<bool>,
}

impl UserLayout {
    #[must_use]
    pub(in crate::runtime::room) const fn featured(&self) -> Option<bool> {
        self.featured
    }

    pub(in crate::runtime::room) fn set_featured(&mut self, featured: Option<bool>) {
        self.featured = featured;
    }
}
