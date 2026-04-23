use std::sync::Arc;

use o_sfu_router::{RouterId, SessionPermissions as RouterSessionPermissions};

use super::ChannelRouterState;
use crate::config::MediaCodecFlags;
use crate::runtime::ChannelInstanceId;
use crate::runtime::channel::rtp_capabilities::router_rtp_capabilities;
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::{MediaSource, MediaTap, RecordingService};
use o_sfu_protocol::shared::SessionId;

impl ChannelRouterState {
    pub(in crate::runtime::channel) fn new_for_test(router_id: RouterId) -> Self {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        Self::new_with_recording_service(
            router_id,
            router_rtp_capabilities(MediaCodecFlags::default()),
            Arc::new(RecordingService::new(
                ChannelInstanceId::from_raw(0),
                media_source,
                Arc::new(RuntimeMetrics::default()),
            )),
        )
    }

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

    pub(in crate::runtime::channel) fn remove_session_mapping_for_test(
        &mut self,
        session_id: &SessionId,
    ) {
        self.router_session_ids_by_session_id.remove(session_id);
    }

    pub(in crate::runtime::channel) fn remove_transport_mapping_for_test(
        &mut self,
        session_id: &SessionId,
    ) {
        self.transport_ids_by_session_id.remove(session_id);
    }
}
