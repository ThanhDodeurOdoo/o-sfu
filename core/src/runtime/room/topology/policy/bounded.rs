use super::{CleanupInput, HomePlacementDecision, HomePlacementInput};

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room::topology) struct BoundedPlacementPolicy;

impl BoundedPlacementPolicy {
    pub(super) fn choose_home_router(input: HomePlacementInput) -> HomePlacementDecision {
        let router_count = input.reserved_router_count.max(1);
        let router_index = usize::try_from(input.connection_seed).unwrap_or(0) % router_count;
        HomePlacementDecision::new(router_index)
    }

    pub(super) const fn active_router_count_to_keep_after_cleanup(input: CleanupInput) -> usize {
        if input.occupied_router_count == 0 {
            1
        } else {
            input.occupied_router_count
        }
    }
}
