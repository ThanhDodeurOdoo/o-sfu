use o_sfu_model::UserId;

use super::super::topology::router_state::RouterAdapterState;
use crate::model::{RouterId, topology::router_state::RouterAdapterError};

#[test]
fn router_state_reports_missing_user_mapping_when_transports_are_requested_without_join() {
    let mut router_state = RouterAdapterState::new_for_test(RouterId(7));
    let user_id = UserId::Integer(10);

    assert_eq!(
        router_state.ensure_session_transports(&user_id),
        Err(RouterAdapterError::MissingSessionMapping { user_id })
    );
}
