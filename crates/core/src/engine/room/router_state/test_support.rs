use std::sync::Arc;

use o_sfu_router::RouterId;

use super::RoomRouterState;
use crate::{
    MediaCodecFlags,
    engine::{
        RoomInstanceId, UserId, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry, recording::RecordingService,
        room::rtp_capabilities::router_rtp_capabilities,
    },
};

impl RoomRouterState {
    pub fn new_for_test(router_id: RouterId) -> Self {
        let packet_sink_registry = Arc::new(RoomPacketSinkRegistry::default());
        let event_sink = Arc::new(RecordingService::new(
            RoomInstanceId::from_raw(0),
            packet_sink_registry,
            Arc::new(RuntimeMetrics::default()),
        ));
        Self::new(
            router_id,
            router_rtp_capabilities(MediaCodecFlags::default()),
            event_sink,
        )
    }

    pub fn remove_session_mapping_for_test(&mut self, user_id: &UserId) {
        self.sessions_by_user.remove(user_id);
    }

    pub fn remove_transport_mapping_for_test(&mut self, user_id: &UserId) {
        self.transports_by_user.remove(user_id);
    }

    pub fn mapped_session_count_for_test(&self) -> usize {
        self.sessions_by_user.len()
    }
}
