#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime::room) struct UserLayout {
    featured: Option<bool>,
}

impl UserLayout {
    #[must_use]
    pub const fn featured(&self) -> Option<bool> {
        self.featured
    }

    pub fn set_featured(&mut self, featured: Option<bool>) {
        self.featured = featured;
    }
}
