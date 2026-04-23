use super::fixtures::*;
use crate::runtime::channel::router_state::{ChannelRouterState, ChannelRouterStateError};

#[test]
fn router_state_reports_missing_session_mapping_when_transports_are_requested_without_join() {
    let mut router_state = ChannelRouterState::new_for_test(RouterId(7));
    let session_id = SessionId::Integer(10);

    assert_eq!(
        router_state.ensure_session_transports(&session_id),
        Err(ChannelRouterStateError::MissingSessionMapping { session_id })
    );
}
