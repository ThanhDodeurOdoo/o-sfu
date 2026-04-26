use std::sync::Arc;

use o_sfu_protocol::shared::UserId;
use o_sfu_router::RouterId;

use super::{RoomRouterObserverFactory, RoomTopology};
use crate::{
    MediaCodecFlags,
    runtime::{
        RoomInstanceId,
        metrics::RuntimeMetrics,
        recording::{MediaSource, MediaTap, RecordingService},
        room::rtp_capabilities::router_rtp_capabilities,
    },
};

impl RoomTopology {
    pub(in crate::runtime::room) fn new(primary_router_id: RouterId) -> Self {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        Self::new_with_recording_observer_factory(
            primary_router_id,
            router_rtp_capabilities(MediaCodecFlags::default()),
            &RoomRouterObserverFactory::new(Arc::new(RecordingService::new(
                RoomInstanceId::from_raw(0),
                media_source,
                Arc::new(RuntimeMetrics::default()),
            ))),
        )
    }

    pub(in crate::runtime::room) fn user_count(&self) -> u64 {
        self.routers
            .values()
            .map(super::super::router_state::RoomRouterState::user_count)
            .sum()
    }

    pub(in crate::runtime::room) fn home_router_id_for_user(
        &self,
        user_id: &UserId,
    ) -> Option<RouterId> {
        self.session_home_router.get(user_id).copied()
    }

    pub(in crate::runtime::room) fn remove_router_for_test(&mut self, router_id: RouterId) {
        self.routers.remove(&router_id);
    }

    pub(in crate::runtime::room) fn remove_session_mapping_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.session_home_router.get(user_id).copied() else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_session_mapping_for_test(user_id);
    }

    pub(in crate::runtime::room) fn remove_transport_mapping_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.session_home_router.get(user_id).copied() else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_transport_mapping_for_test(user_id);
    }
}
