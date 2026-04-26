use std::sync::Arc;

use o_sfu_protocol::shared::UserId;
use o_sfu_router::{RouterId, SessionPermissions as RouterSessionPermissions};

use super::RoomRouterState;
use crate::{
    MediaCodecFlags,
    runtime::{
        RoomInstanceId,
        metrics::RuntimeMetrics,
        recording::{MediaSource, MediaTap, RecordingService},
        room::rtp_capabilities::router_rtp_capabilities,
    },
};

impl RoomRouterState {
    pub(in crate::runtime::room) fn new_for_test(router_id: RouterId) -> Self {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        Self::new_with_recording_service(
            router_id,
            router_rtp_capabilities(MediaCodecFlags::default()),
            Arc::new(RecordingService::new(
                RoomInstanceId::from_raw(0),
                media_source,
                Arc::new(RuntimeMetrics::default()),
            )),
        )
    }

    pub(in crate::runtime::room) fn user_count(&self) -> u64 {
        u64::try_from(self.router.session_count()).unwrap_or(u64::MAX)
    }

    pub(in crate::runtime::room) fn session_permissions(
        &self,
        user_id: &UserId,
    ) -> Option<RouterSessionPermissions> {
        let router_user_id = self.router_user_ids_by_user_id.get(user_id)?;
        self.router
            .sessions()
            .find(|user| user.id() == *router_user_id)
            .map(o_sfu_router::Session::permissions)
    }

    pub(in crate::runtime::room) fn remove_session_mapping_for_test(&mut self, user_id: &UserId) {
        self.router_user_ids_by_user_id.remove(user_id);
    }

    pub(in crate::runtime::room) fn remove_transport_mapping_for_test(&mut self, user_id: &UserId) {
        self.transport_ids_by_user_id.remove(user_id);
    }
}
