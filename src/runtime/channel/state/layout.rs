#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime::channel) struct SessionLayout {
    featured: Option<bool>,
}

impl SessionLayout {
    #[must_use]
    pub(in crate::runtime::channel) const fn featured(&self) -> Option<bool> {
        self.featured
    }

    pub(in crate::runtime::channel) fn set_featured(&mut self, featured: Option<bool>) {
        self.featured = featured;
    }
}
