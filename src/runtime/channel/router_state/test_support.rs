use o_sfu_router::SessionPermissions as RouterSessionPermissions;

use super::ChannelRouterState;
use o_sfu_protocol::shared::SessionId;

impl ChannelRouterState {
    pub(in crate::runtime::channel) fn session_count(&self) -> u64 {
        u64::try_from(self.router.session_count()).unwrap_or(u64::MAX)
    }

    pub(in crate::runtime::channel) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<RouterSessionPermissions> {
        let router_session_id = self.router_session_ids_by_session_id.get(session_id)?;
        self.router
            .sessions()
            .find(|session| session.id() == *router_session_id)
            .map(o_sfu_router::Session::permissions)
    }
}
