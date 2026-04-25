use super::fixtures::*;
use crate::runtime::room::router_state::{RoomRouterState, RoomRouterStateError};

#[test]
fn router_state_reports_missing_user_mapping_when_transports_are_requested_without_join() {
    let mut router_state = RoomRouterState::new_for_test(RouterId(7));
    let user_id = UserId::Integer(10);

    assert_eq!(
        router_state.ensure_session_transports(&user_id),
        Err(RoomRouterStateError::MissingSessionMapping { user_id })
    );
}
