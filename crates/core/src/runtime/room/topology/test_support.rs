#[cfg(test)]
use std::sync::Arc;

use o_sfu_router::RouterId;

#[cfg(test)]
use super::RoomRouterStateFactory;
use super::RoomTopology;
use crate::runtime::UserId;
#[cfg(test)]
use crate::{
    MediaCodecFlags, RoomWorkerPolicy,
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
};

impl RoomTopology {
    #[cfg(test)]
    pub fn new(primary_router_id: RouterId) -> Self {
        Self::new_with_policy(
            primary_router_id,
            RoomWorkerPolicy::strict_single_router(),
            1,
        )
    }

    #[cfg(test)]
    pub fn new_with_policy(
        primary_router_id: RouterId,
        room_worker_policy: RoomWorkerPolicy,
        local_router_count: usize,
    ) -> Self {
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
        Self::new_with_router_state_factory(
            LocalRoomRouterPlacements::new(primary, spillover),
            room_worker_policy,
            router_rtp_capabilities(MediaCodecFlags::default()),
            &RoomRouterStateFactory::new(event_sink),
        )
    }

    #[cfg(test)]
    pub fn new_with_bounded_spillover(
        primary_router_id: RouterId,
        local_router_count: usize,
    ) -> Self {
        Self::new_with_policy(
            primary_router_id,
            RoomWorkerPolicy::bounded_local_spillover(local_router_count),
            local_router_count,
        )
    }

    #[cfg(test)]
    pub fn new_with_load_spillover(
        primary_router_id: RouterId,
        local_router_count: usize,
        policy: crate::LocalSpilloverPolicy,
    ) -> Self {
        Self::new_with_policy(
            primary_router_id,
            RoomWorkerPolicy::load_triggered_local_spillover(local_router_count, policy),
            local_router_count,
        )
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
    pub fn active_load_router_count_for_test(&self) -> usize {
        self.placement_policy.active_router_count_for_test()
    }

    #[cfg(test)]
    pub fn last_load_pressure_reason_for_test(&self) -> Option<super::LoadPressureReason> {
        self.placement_policy.last_load_pressure_reason_for_test()
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
