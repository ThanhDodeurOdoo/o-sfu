use super::{CleanupInput, HomePlacementDecision, HomePlacementInput};

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room::topology) struct StrictPlacementPolicy;

impl StrictPlacementPolicy {
    pub(super) const fn choose_home_router(_input: HomePlacementInput) -> HomePlacementDecision {
        HomePlacementDecision::new(0)
    }

    pub(super) const fn active_router_count_to_keep_after_cleanup(_input: CleanupInput) -> usize {
        1
    }
}
