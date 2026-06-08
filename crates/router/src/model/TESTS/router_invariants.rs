use crate::{Router, model::test_support::router_satisfies_invariants};

pub(super) fn assert_router_is_consistent(router: &Router) {
    assert!(router_satisfies_invariants(router));
}
