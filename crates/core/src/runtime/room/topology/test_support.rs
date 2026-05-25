use o_sfu_router::RouterId;
#[cfg(test)]
use {
    super::RoomRouterStateFactory,
    crate::{
        MediaCodecFlags,
        runtime::{
            RoomInstanceId,
            metrics::RuntimeMetrics,
            packet_sink_registry::RoomPacketSinkRegistry,
            recording::RecordingService,
            room::{
                LocalRoomRouterPlacements, LocalRouterRuntimeContext,
                rtp_capabilities::router_rtp_capabilities,
            },
        },
    },
    std::sync::Arc,
};

use super::RoomTopology;
use crate::runtime::UserId;

impl RoomTopology {
    #[cfg(test)]
    pub fn new(primary_router_id: RouterId) -> Self {
        Self::new_with_placements(primary_router_id, 1)
    }

    #[cfg(test)]
    pub fn new_with_placements(primary_router_id: RouterId, local_router_count: usize) -> Self {
        let packet_sink_registry = Arc::new(RoomPacketSinkRegistry::default());
        let event_sink = Arc::new(RecordingService::new(
            RoomInstanceId::from_raw(0),
            packet_sink_registry,
            Arc::new(RuntimeMetrics::default()),
        ));
        let primary = LocalRouterRuntimeContext {
            router: primary_router_id,
            media_worker: 0,
        };
        let spillover = (1..local_router_count.max(1))
            .map(|offset| LocalRouterRuntimeContext {
                router: RouterId(
                    primary_router_id
                        .0
                        .saturating_add(u64::try_from(offset).unwrap_or(u64::MAX)),
                ),
                media_worker: offset,
            })
            .collect::<Vec<_>>();
        let local_routers = LocalRoomRouterPlacements::new(primary, spillover);
        Self::new_with_router_state_factory(
            &local_routers,
            router_rtp_capabilities(MediaCodecFlags::default()),
            &RoomRouterStateFactory::new(event_sink),
        )
    }

    #[cfg(test)]
    pub fn new_with_bounded_spillover(
        primary_router_id: RouterId,
        local_router_count: usize,
    ) -> Self {
        Self::new_with_placements(primary_router_id, local_router_count)
    }

    #[cfg(test)]
    pub fn user_count(&self) -> u64 {
        u64::try_from(self.session_home_router.len()).unwrap_or(u64::MAX)
    }

    pub fn router_count(&self) -> usize {
        self.routers.len()
    }

    #[cfg(test)]
    pub fn mapped_session_count_for_router(&self, router_id: RouterId) -> Option<usize> {
        self.routers
            .get(&router_id)
            .map(super::RoomRouterState::mapped_session_count_for_test)
    }

    pub fn home_router_id_for_user(&self, user_id: &UserId) -> Option<RouterId> {
        self.session_home_router.get(user_id).copied()
    }

    #[cfg(test)]
    pub fn remove_router_for_test(&mut self, router_id: RouterId) {
        self.routers.remove(&router_id);
    }

    #[cfg(test)]
    pub fn remove_session_mapping_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.session_home_router.get(user_id).copied() else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_session_mapping_for_test(user_id);
    }

    #[cfg(test)]
    pub fn remove_transport_mapping_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.session_home_router.get(user_id).copied() else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_transport_mapping_for_test(user_id);
    }
}
