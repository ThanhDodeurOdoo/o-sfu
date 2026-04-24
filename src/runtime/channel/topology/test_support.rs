use std::sync::Arc;

use o_sfu_protocol::shared::SessionId;
use o_sfu_router::{RouterId, SessionPermissions as RouterSessionPermissions};

use super::{ChannelRouterObserverFactory, ChannelTopology};
use crate::{
    config::MediaCodecFlags,
    runtime::{
        ChannelInstanceId,
        channel::rtp_capabilities::router_rtp_capabilities,
        metrics::RuntimeMetrics,
        recording::{MediaSource, MediaTap, RecordingService},
    },
};

impl ChannelTopology {
    pub(in crate::runtime::channel) fn new(primary_router_id: RouterId) -> Self {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        Self::new_with_recording_observer_factory(
            primary_router_id,
            router_rtp_capabilities(MediaCodecFlags::default()),
            &ChannelRouterObserverFactory::new(Arc::new(RecordingService::new(
                ChannelInstanceId::from_raw(0),
                media_source,
                Arc::new(RuntimeMetrics::default()),
            ))),
        )
    }

    pub(in crate::runtime::channel) fn session_count(&self) -> u64 {
        self.routers
            .values()
            .map(super::super::router_state::ChannelRouterState::session_count)
            .sum()
    }

    pub(in crate::runtime::channel) fn home_router_id_for_session(
        &self,
        session_id: &SessionId,
    ) -> Option<RouterId> {
        self.session_home_router.get(session_id).copied()
    }

    pub(in crate::runtime::channel) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<RouterSessionPermissions> {
        let router_id = self.session_home_router.get(session_id).copied()?;
        self.routers
            .get(&router_id)?
            .session_permissions(session_id)
    }

    pub(in crate::runtime::channel) fn remove_router_for_test(&mut self, router_id: RouterId) {
        self.routers.remove(&router_id);
    }

    pub(in crate::runtime::channel) fn remove_session_mapping_for_test(
        &mut self,
        session_id: &SessionId,
    ) {
        let Some(router_id) = self.session_home_router.get(session_id).copied() else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_session_mapping_for_test(session_id);
    }

    pub(in crate::runtime::channel) fn remove_transport_mapping_for_test(
        &mut self,
        session_id: &SessionId,
    ) {
        let Some(router_id) = self.session_home_router.get(session_id).copied() else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_transport_mapping_for_test(session_id);
    }
}
